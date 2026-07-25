from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from crypto_address_identity.address_enrichment import (
    AddressEnrichmentArtifactError,
    BtcV2SAddressEnrichmentService,
    BtcV2SAddressQueueBuilder,
)
from crypto_address_identity.chains.bitcoin import normalize_bitcoin_address
from crypto_address_identity.cli import main
from crypto_address_identity.core.config import Settings
from crypto_address_identity.evidence import EvidenceService, VerifierRegistry
from crypto_address_identity.providers.zero_x_router import ZeroXRouterClient
from crypto_address_identity.storage.raw_payloads import RawPayloadStore
from crypto_address_identity.storage.sqlite import IdentityDatabase


BTC_ADDRESSES = (
    "1BoatSLRHtKNngkdXEeobR76b53LETtpyT",
    "bc1qcr8te4kr609gcawutmrza0j4xv80jy8z306fyu",
    "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy",
    "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
    "1BitcoinEaterAddressDontSendf59kuE",
)


def _settings(env_mapping: dict[str, str]) -> Settings:
    env_mapping["CAI_0XROUTER_TOKEN"] = "test-token"
    env_mapping["CAI_0XROUTER_MAX_TRANSPORT_RETRIES"] = "0"
    env_mapping["CAI_CHAINDATA_COVERAGE_REQUESTS_PER_MINUTE"] = "25"
    env_mapping["CAI_CHAINDATA_COVERAGE_RESPONSE_BYTES_BUDGET"] = "1048576"
    return Settings.model_validate(env_mapping)


def _semantic_hash(value: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _write_manifest(
    root: Path,
    *,
    campaign_id: str,
    parquet_path: Path,
    row_count: int,
) -> Path:
    manifest: dict[str, object] = {
        "campaign_id": campaign_id,
        "candidate_rows": row_count,
        "files": [
            {
                "path": str(parquet_path.relative_to(root)),
                "row_count": row_count,
                "size": parquet_path.stat().st_size,
                "sha256": hashlib.sha256(parquet_path.read_bytes()).hexdigest(),
            }
        ],
    }
    manifest["manifest_sha256"] = _semantic_hash(manifest)
    path = root / "manifest.json"
    path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return path


def _write_inputs(root: Path) -> tuple[Path, Path]:
    candidate_root = root / "candidate"
    candidate_path = (
        candidate_root
        / "candidates"
        / "tier=p0"
        / "bucket=00"
        / "part-00000.parquet"
    )
    candidate_path.parent.mkdir(parents=True)
    row_hashes = [
        hashlib.sha256(address.encode()).hexdigest()
        for address in BTC_ADDRESSES
    ]
    candidate_table = pa.table(
        {
            "normalized_address": list(BTC_ADDRESSES),
            "candidate_tier": ["p0", "p1", "p0", "p1", "p0"],
            "v2_chain_score": [80, 70, 60, 30, 50],
            "current_utxo_sats": [
                500_000_000_000,
                400_000_000_000,
                300_000_000_000,
                200_000_000_000,
                100_000_000_000,
            ],
            "lifetime_received_sats": [
                900_000_000_000,
                800_000_000_000,
                700_000_000_000,
                600_000_000_000,
                500_000_000_000,
            ],
            "candidate_row_sha256": row_hashes,
        }
    )
    pq.write_table(candidate_table, candidate_path)
    _write_manifest(
        candidate_root,
        campaign_id="fixture-v2s",
        parquet_path=candidate_path,
        row_count=len(BTC_ADDRESSES),
    )

    coverage_root = root / "coverage" / "snapshot"
    coverage_root.mkdir(parents=True)
    coverage_path = coverage_root / "btc_v2s_coverage_state.parquet"
    coverage_table = pa.table(
        {
            "normalized_address": list(BTC_ADDRESSES),
            "candidate_tier": ["p0", "p1", "p0", "p1", "p0"],
            "candidate_row_sha256": row_hashes,
            "coverage_state": [
                "needs_direct_enrichment",
                "needs_direct_enrichment",
                "needs_direct_enrichment",
                "needs_direct_enrichment",
                "entity_membership_covered",
            ],
            "provider_entity_ids": [[], [], [], [], ["known-entity"]],
            "direct_enriched": [False] * 5,
            "entity_membership_covered": [False, False, False, False, True],
            "local_evidence_covered": [False] * 5,
            "active_conflict": [True, False, False, False, False],
            "explicit_direct_requirement": [True, True, False, False, False],
            "coverage_reason_codes": [
                ["active_identity_conflict"],
                ["explicit_address_level_requirement"],
                ["no_explicit_entity_membership"],
                ["no_explicit_entity_membership"],
                ["provider_entity_prediction"],
            ],
        }
    )
    pq.write_table(coverage_table, coverage_path)
    coverage_manifest: dict[str, object] = {
        "schema_version": "btc_v2s_coverage_state_v1",
        "snapshot_id": "fixture-coverage",
        "source_campaign_id": "fixture-v2s",
        "source_manifest_file_sha256": hashlib.sha256(
            (candidate_root / "manifest.json").read_bytes()
        ).hexdigest(),
        "source_candidate_rows": len(BTC_ADDRESSES),
        "state_counts": {
            "direct_enriched": 0,
            "entity_membership_covered": 1,
            "local_evidence_covered": 0,
            "needs_direct_enrichment": 4,
        },
        "files": [
            {
                "path": coverage_path.name,
                "row_count": len(BTC_ADDRESSES),
                "size": coverage_path.stat().st_size,
                "sha256": hashlib.sha256(coverage_path.read_bytes()).hexdigest(),
            }
        ],
    }
    coverage_manifest["manifest_sha256"] = _semantic_hash(coverage_manifest)
    (coverage_root / "manifest.json").write_text(
        json.dumps(coverage_manifest, sort_keys=True),
        encoding="utf-8",
    )
    return candidate_root, coverage_root


def test_queue_builder_is_checksum_pinned_and_orders_urgent_then_p0_then_p1(
    runtime_root: Path,
) -> None:
    candidate_root, coverage_root = _write_inputs(runtime_root)

    result = BtcV2SAddressQueueBuilder().build(
        candidate_campaign_root=candidate_root,
        coverage_snapshot_root=coverage_root,
        output_root=runtime_root / "queues",
        built_at=datetime(2026, 7, 25, tzinfo=UTC),
    )

    assert result.queue_rows == 4
    assert result.cohort_counts == {"urgent": 2, "p0": 1, "p1": 1}
    rows = pq.read_table(result.parquet_path).to_pylist()
    assert [row["cohort"] for row in rows] == [
        "urgent",
        "urgent",
        "p0",
        "p1",
    ]
    assert [row["queue_rank"] for row in rows] == [1, 2, 3, 4]
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    declared = manifest.pop("manifest_sha256")
    assert declared == _semantic_hash(manifest)
    assert result.parquet_sha256 == hashlib.sha256(
        result.parquet_path.read_bytes()
    ).hexdigest()


def test_queue_builder_fails_closed_on_coverage_checksum_drift(
    runtime_root: Path,
) -> None:
    candidate_root, coverage_root = _write_inputs(runtime_root)
    parquet_path = coverage_root / "btc_v2s_coverage_state.parquet"
    parquet_path.write_bytes(parquet_path.read_bytes() + b"drift")

    with pytest.raises(
        AddressEnrichmentArtifactError, match="wrong size"
    ):
        BtcV2SAddressQueueBuilder().build(
            candidate_campaign_root=candidate_root,
            coverage_snapshot_root=coverage_root,
            output_root=runtime_root / "queues",
        )


def test_address_campaign_dry_run_writes_nothing_and_execute_is_once_per_address(
    env_mapping: dict[str, str],
    runtime_root: Path,
) -> None:
    candidate_root, coverage_root = _write_inputs(runtime_root)
    queue = BtcV2SAddressQueueBuilder().build(
        candidate_campaign_root=candidate_root,
        coverage_snapshot_root=coverage_root,
        output_root=runtime_root / "queues",
        built_at=datetime(2026, 7, 25, tzinfo=UTC),
    )
    settings = _settings(env_mapping)
    database = IdentityDatabase(settings.database_path)
    database.migrate()
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        address = request.url.path.split("/")[-2]
        entity_id = "entity-a" if len(calls) == 1 else "entity-b"
        return httpx.Response(
            200,
            json={
                "bitcoin": {
                    "address": address,
                    "chain": "bitcoin",
                    "arkhamEntity": {
                        "id": entity_id,
                        "name": entity_id.title(),
                    },
                }
            },
        )

    provider = ZeroXRouterClient(
        settings, transport=httpx.MockTransport(handler)
    )
    service = BtcV2SAddressEnrichmentService(
        database=database,
        settings=settings,
        provider=provider,
        raw_payloads=RawPayloadStore(database, settings.raw_payload_root),
        evidence=EvidenceService(database, VerifierRegistry()),
        sleeper=lambda seconds: None,
    )
    try:
        dry_run = service.run(
            queue_root=queue.manifest_path.parent,
            campaign_id="fixture-urgent",
            cohort="urgent",
            request_limit=10,
            campaign_point_limit=20,
            dry_run=True,
            now=datetime(2026, 7, 25, tzinfo=UTC),
        )
        first = service.run(
            queue_root=queue.manifest_path.parent,
            campaign_id="fixture-urgent",
            cohort="urgent",
            request_limit=10,
            campaign_point_limit=20,
            dry_run=False,
            now=datetime(2026, 7, 25, 0, 2, tzinfo=UTC),
        )
        second = service.run(
            queue_root=queue.manifest_path.parent,
            campaign_id="fixture-urgent",
            cohort="urgent",
            request_limit=10,
            campaign_point_limit=20,
            dry_run=False,
            now=datetime(2026, 7, 25, 0, 4, tzinfo=UTC),
        )
    finally:
        provider.close()

    assert dry_run.planned_addresses == 2
    assert dry_run.requests == 0
    assert first.requests == 2
    assert first.parsed_addresses == 2
    assert first.new_entities == 2
    assert first.entity_seeds_inserted == 2
    assert second.planned_addresses == 0
    assert second.requests == 0
    assert len(calls) == 2
    assert all("/address_enriched/" in path for path in calls)
    with database.read_connection() as connection:
        assert connection.execute(
            """
            SELECT COUNT(*) FROM coverage_address_campaign_attempt
            WHERE campaign_id = 'fixture-urgent'
            """
        ).fetchone()[0] == 2
        assert connection.execute(
            """
            SELECT COUNT(*) FROM source_observation
            WHERE query_profile =
                  'btc_v2s_address_enrichment:fixture-urgent'
            """
        ).fetchone()[0] == 2
        assert connection.execute(
            """
            SELECT COUNT(DISTINCT provider_entity_id)
            FROM coverage_entity_seed
            """
        ).fetchone()[0] == 2

    with pytest.raises(ValueError, match="different immutable inputs"):
        service.run(
            queue_root=queue.manifest_path.parent,
            campaign_id="fixture-urgent",
            cohort="urgent",
            request_limit=10,
            campaign_point_limit=21,
            dry_run=True,
        )


def test_address_campaign_rate_limit_uses_request_start_spacing(
    env_mapping: dict[str, str],
    runtime_root: Path,
) -> None:
    candidate_root, coverage_root = _write_inputs(runtime_root)
    queue = BtcV2SAddressQueueBuilder().build(
        candidate_campaign_root=candidate_root,
        coverage_snapshot_root=coverage_root,
        output_root=runtime_root / "queues",
        built_at=datetime(2026, 7, 25, tzinfo=UTC),
    )
    settings = _settings(env_mapping)
    database = IdentityDatabase(settings.database_path)
    database.migrate()
    monotonic_now = 0.0
    request_starts: list[float] = []
    sleep_calls: list[float] = []

    def monotonic() -> float:
        return monotonic_now

    def sleeper(seconds: float) -> None:
        nonlocal monotonic_now
        sleep_calls.append(seconds)
        monotonic_now += seconds

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal monotonic_now
        request_starts.append(monotonic_now)
        monotonic_now += 0.8
        address = request.url.path.split("/")[-2]
        return httpx.Response(
            200,
            json={
                "bitcoin": {
                    "address": address,
                    "chain": "bitcoin",
                }
            },
        )

    provider = ZeroXRouterClient(
        settings, transport=httpx.MockTransport(handler)
    )
    try:
        result = BtcV2SAddressEnrichmentService(
            database=database,
            settings=settings,
            provider=provider,
            raw_payloads=RawPayloadStore(
                database, settings.raw_payload_root
            ),
            evidence=EvidenceService(database, VerifierRegistry()),
            sleeper=sleeper,
            monotonic=monotonic,
        ).run(
            queue_root=queue.manifest_path.parent,
            campaign_id="fixture-start-spacing",
            cohort="urgent",
            request_limit=10,
            campaign_point_limit=20,
            dry_run=False,
            now=datetime(2026, 7, 25, tzinfo=UTC),
        )
    finally:
        provider.close()

    assert result.requests == 2
    assert request_starts == pytest.approx([0.0, 2.4])
    assert sleep_calls == pytest.approx([1.6])


def test_campaign_point_limit_bounds_planned_addresses(
    env_mapping: dict[str, str],
    runtime_root: Path,
) -> None:
    candidate_root, coverage_root = _write_inputs(runtime_root)
    queue = BtcV2SAddressQueueBuilder().build(
        candidate_campaign_root=candidate_root,
        coverage_snapshot_root=coverage_root,
        output_root=runtime_root / "queues",
        built_at=datetime(2026, 7, 25, tzinfo=UTC),
    )
    settings = _settings(env_mapping)
    database = IdentityDatabase(settings.database_path)
    database.migrate()
    provider = ZeroXRouterClient(
        settings,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={})
        ),
    )
    try:
        result = BtcV2SAddressEnrichmentService(
            database=database,
            settings=settings,
            provider=provider,
            raw_payloads=RawPayloadStore(
                database, settings.raw_payload_root
            ),
            evidence=EvidenceService(database, VerifierRegistry()),
        ).run(
            queue_root=queue.manifest_path.parent,
            campaign_id="fixture-one-point",
            cohort="urgent",
            request_limit=10,
            campaign_point_limit=1,
            dry_run=True,
        )
    finally:
        provider.close()

    assert result.eligible_addresses == 2
    assert result.planned_addresses == 1


def test_new_membership_skips_non_explicit_queue_row_but_not_urgent_row(
    env_mapping: dict[str, str],
    runtime_root: Path,
) -> None:
    candidate_root, coverage_root = _write_inputs(runtime_root)
    queue = BtcV2SAddressQueueBuilder().build(
        candidate_campaign_root=candidate_root,
        coverage_snapshot_root=coverage_root,
        output_root=runtime_root / "queues",
        built_at=datetime(2026, 7, 25, tzinfo=UTC),
    )
    settings = _settings(env_mapping)
    database = IdentityDatabase(settings.database_path)
    database.migrate()
    for address in (BTC_ADDRESSES[0], BTC_ADDRESSES[2]):
        subject = normalize_bitcoin_address(address)
        with database.write_transaction() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO address_subject (
                    address_id, chain_key, normalized_address,
                    display_address, address_type, first_seen_at
                ) VALUES (?, 'bitcoin', ?, ?, ?, '2026-07-25T00:00:00Z')
                """,
                (
                    subject.address_id,
                    subject.normalized_address,
                    subject.display_address,
                    subject.address_type,
                ),
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO source_observation (
                    observation_id, source_id, source_version, source_kind,
                    endpoint_template, query_profile, requested_at,
                    completed_at, outcome, chain_key
                ) VALUES (
                    'fixture-observation', 'fixture', 'fixture', 'import',
                    'fixture', 'fixture', '2026-07-25T00:00:00Z',
                    '2026-07-25T00:00:00Z', 'success', 'bitcoin'
                )
                """
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO coverage_entity_prediction (
                    prediction_id, prediction_fingerprint, observation_id,
                    provider_entity_id, address_id, prediction_rank,
                    observed_at
                ) VALUES (?, ?, 'fixture-observation', 'new-entity', ?, 1,
                          '2026-07-25T00:00:00Z')
                """,
                (
                    f"prediction-{subject.address_id}",
                    f"fingerprint-{subject.address_id}",
                    subject.address_id,
                ),
            )

    provider = ZeroXRouterClient(
        settings,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={})
        ),
    )
    try:
        service = BtcV2SAddressEnrichmentService(
            database=database,
            settings=settings,
            provider=provider,
            raw_payloads=RawPayloadStore(
                database, settings.raw_payload_root
            ),
            evidence=EvidenceService(database, VerifierRegistry()),
        )
        urgent = service.run(
            queue_root=queue.manifest_path.parent,
            campaign_id="fixture-urgent-membership",
            cohort="urgent",
            request_limit=10,
            campaign_point_limit=20,
            dry_run=True,
        )
        p0 = service.run(
            queue_root=queue.manifest_path.parent,
            campaign_id="fixture-p0-membership",
            cohort="p0",
            request_limit=10,
            campaign_point_limit=20,
            dry_run=True,
        )
    finally:
        provider.close()

    assert urgent.planned_addresses == 2
    assert p0.planned_addresses == 0
    assert p0.skipped_newly_covered == 1


def test_address_enrichment_cli_dry_run_is_structured_and_secret_free(
    env_mapping: dict[str, str],
    runtime_root: Path,
    monkeypatch,
    capsys,
) -> None:
    candidate_root, coverage_root = _write_inputs(runtime_root)
    queue = BtcV2SAddressQueueBuilder().build(
        candidate_campaign_root=candidate_root,
        coverage_snapshot_root=coverage_root,
        output_root=runtime_root / "queues",
        built_at=datetime(2026, 7, 25, tzinfo=UTC),
    )
    for key, value in env_mapping.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("CAI_0XROUTER_TOKEN", raising=False)
    assert main(["init-db"]) == 0
    capsys.readouterr()

    assert (
        main(
            [
                "coverage-sync",
                "address-enrichment",
                "--queue-root",
                str(queue.manifest_path.parent),
                "--campaign-id",
                "fixture-cli-urgent",
                "--cohort",
                "urgent",
                "--request-limit",
                "10",
                "--campaign-point-limit",
                "20",
                "--dry-run",
            ]
        )
        == 0
    )
    output = json.loads(capsys.readouterr().out)

    assert output["status"] == "dry_run"
    assert output["planned_addresses"] == 2
    assert output["requests"] == 0
    assert "token" not in json.dumps(output).lower()
