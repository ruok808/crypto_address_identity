from __future__ import annotations

import gzip
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from crypto_address_identity.candidates import CandidateInput, CandidateService
from crypto_address_identity.chains.bitcoin import normalize_bitcoin_address
from crypto_address_identity.core.config import Settings
from crypto_address_identity.entity_fanout import (
    BtcEntityFanoutService,
    BtcV2SCoverageSnapshotBuilder,
    CanaryEntitySeedReader,
)
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
    return Settings.model_validate(env_mapping)


def _write_canary(root: Path, *, count: int = 3) -> Path:
    canary = root / "canary"
    raw_root = canary / "raw"
    raw_root.mkdir(parents=True)
    ledger_rows: list[dict[str, object]] = []
    sample_rows: list[dict[str, object]] = []
    entities = ("binance", "coinbase", "binance")
    for sequence, (address, entity_id) in enumerate(
        zip(BTC_ADDRESSES[:count], entities[:count], strict=True), start=1
    ):
        body = json.dumps(
            {
                "bitcoin": {
                    "address": address,
                    "chain": "bitcoin",
                    "arkhamEntity": {"id": entity_id, "name": entity_id.title()},
                }
            },
            sort_keys=True,
        ).encode()
        payload_sha256 = hashlib.sha256(body).hexdigest()
        relative_path = (
            Path("sha256")
            / payload_sha256[:2]
            / f"{payload_sha256}.json.gz"
        )
        destination = raw_root / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(gzip.compress(body, mtime=0))
        ledger_rows.append(
            {
                "sequence": sequence,
                "address_id": f"bitcoin:{address}",
                "payload_sha256": payload_sha256,
                "raw_relative_path": str(relative_path),
                "outcome": "success",
                "parse_outcome": "parsed_success",
            }
        )
        sample_rows.append(
            {
                "sequence": sequence,
                "address_id": f"bitcoin:{address}",
                "normalized_address": address,
                "candidate_tier": "p0",
            }
        )
    (canary / "request_ledger.jsonl").write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in ledger_rows)
    )
    (canary / "sample.ndjson").write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in sample_rows)
    )
    return canary


def _write_candidate_artifact(root: Path) -> Path:
    campaign = root / "campaign"
    destination = campaign / "candidates" / "tier=p0" / "bucket=00"
    destination.mkdir(parents=True)
    table = pa.table(
        {
            "normalized_address": list(BTC_ADDRESSES),
            "candidate_tier": ["p0", "p0", "p1", "edge", "edge"],
            "candidate_row_sha256": [
                hashlib.sha256(address.encode()).hexdigest()
                for address in BTC_ADDRESSES
            ],
        }
    )
    parquet_path = destination / "part-00000.parquet"
    pq.write_table(table, parquet_path)
    receipt_path = campaign / "execution_receipt.json"
    receipt_path.write_text(
        json.dumps({"status": "completed"}, sort_keys=True),
        encoding="utf-8",
    )
    manifest = {
        "campaign_id": "fixture-v2s",
        "candidate_rows": len(BTC_ADDRESSES),
        "files": [
            {
                "path": "candidates/tier=p0/bucket=00/part-00000.parquet",
                "row_count": len(BTC_ADDRESSES),
                "size": parquet_path.stat().st_size,
                "sha256": hashlib.sha256(parquet_path.read_bytes()).hexdigest(),
            },
            {
                "path": "execution_receipt.json",
                "row_count": None,
                "size": receipt_path.stat().st_size,
                "sha256": hashlib.sha256(
                    receipt_path.read_bytes()
                ).hexdigest(),
            },
        ],
    }
    manifest["manifest_sha256"] = hashlib.sha256(
        json.dumps(
            manifest, sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()
    (campaign / "manifest.json").write_text(json.dumps(manifest, sort_keys=True))
    return campaign


def test_canary_seed_reader_validates_hashes_and_exactly_deduplicates(
    runtime_root: Path,
) -> None:
    canary = _write_canary(runtime_root)

    result = CanaryEntitySeedReader(canary).read()

    assert result.entity_ids == ("binance", "coinbase")
    assert result.entity_labeled_addresses == 3
    assert result.duplicate_entity_mentions == 1
    assert result.verified_payloads == 3


def test_fanout_requests_each_due_entity_once_and_only_calls_predictions(
    env_mapping: dict[str, str],
) -> None:
    settings = _settings(env_mapping)
    database = IdentityDatabase(settings.database_path)
    database.migrate()
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        entity_id = request.url.path.rsplit("/", 1)[-1]
        address = BTC_ADDRESSES[0] if entity_id == "binance" else BTC_ADDRESSES[1]
        return httpx.Response(200, json=[{"address": address}])

    provider = ZeroXRouterClient(
        settings, transport=httpx.MockTransport(handler)
    )
    try:
        service = BtcEntityFanoutService(
            database=database,
            settings=settings,
            provider=provider,
            raw_payloads=RawPayloadStore(database, settings.raw_payload_root),
        )
        first = service.run(
            entity_ids=("coinbase", "binance", "coinbase"),
            dry_run=False,
            request_limit=10,
            now=datetime(2026, 7, 25, tzinfo=UTC),
        )
        second = service.run(
            entity_ids=("binance", "coinbase"),
            dry_run=False,
            request_limit=10,
            now=datetime(2026, 7, 25, 0, 2, tzinfo=UTC),
        )
    finally:
        provider.close()

    assert first.planned_entities == 2
    assert first.requests == 2
    assert first.successful_entities == 2
    assert first.unique_prediction_addresses == 2
    assert second.planned_entities == 0
    assert second.requests == 0
    assert calls == [
        "/chaindata/intelligence/entity_predictions/coinbase",
        "/chaindata/intelligence/entity_predictions/binance",
    ]
    assert all("/address_enriched/" not in path for path in calls)
    assert all("/entity/" not in path for path in calls)


def test_fanout_dry_run_merges_local_entities_without_provider_calls(
    env_mapping: dict[str, str],
) -> None:
    settings = _settings(env_mapping)
    database = IdentityDatabase(settings.database_path)
    database.migrate()
    with database.write_transaction() as connection:
        connection.execute(
            """
            INSERT INTO coverage_entity_seed (
                seed_id, seed_fingerprint, provider_entity_id, priority,
                source_reference, requested_at, created_at
            ) VALUES (
                'entity-seed', 'entity-fingerprint', 'local-entity', 80,
                'fixture:local', '2026-07-25T00:00:00Z',
                '2026-07-25T00:00:00Z'
            )
            """
        )

    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json=[])

    provider = ZeroXRouterClient(
        settings, transport=httpx.MockTransport(handler)
    )
    try:
        result = BtcEntityFanoutService(
            database=database,
            settings=settings,
            provider=provider,
            raw_payloads=RawPayloadStore(database, settings.raw_payload_root),
        ).run(
            entity_ids=("canary-entity",),
            include_local_entities=True,
            dry_run=True,
            request_limit=10,
            now=datetime(2026, 7, 25, tzinfo=UTC),
        )
    finally:
        provider.close()

    assert result.input_entities == 1
    assert result.local_entities == 1
    assert result.merged_unique_entities == 2
    assert result.planned_entities == 2
    assert calls == 0


def test_fanout_supports_large_entity_batch_while_preserving_rate_ceiling(
    env_mapping: dict[str, str],
) -> None:
    settings = _settings(env_mapping)
    database = IdentityDatabase(settings.database_path)
    database.migrate()
    provider = ZeroXRouterClient(
        settings,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=[])
        ),
    )
    try:
        result = BtcEntityFanoutService(
            database=database,
            settings=settings,
            provider=provider,
            raw_payloads=RawPayloadStore(
                database, settings.raw_payload_root
            ),
        ).run(
            entity_ids=tuple(f"entity-{index}" for index in range(40)),
            dry_run=True,
            request_limit=200,
        )
    finally:
        provider.close()

    assert settings.coverage_requests_per_minute == 25
    assert result.planned_entities == 40
    assert result.requests == 0


def test_fanout_stops_after_three_consecutive_provider_failures(
    env_mapping: dict[str, str],
) -> None:
    settings = _settings(env_mapping)
    database = IdentityDatabase(settings.database_path)
    database.migrate()
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(502, json={"error": "fixture"})

    provider = ZeroXRouterClient(
        settings, transport=httpx.MockTransport(handler)
    )
    try:
        result = BtcEntityFanoutService(
            database=database,
            settings=settings,
            provider=provider,
            raw_payloads=RawPayloadStore(
                database, settings.raw_payload_root
            ),
            sleeper=lambda seconds: None,
        ).run(
            entity_ids=tuple(f"entity-{index}" for index in range(10)),
            dry_run=False,
            request_limit=10,
            now=datetime(2026, 7, 25, tzinfo=UTC),
        )
    finally:
        provider.close()

    assert result.status == "blocked"
    assert result.requests == 3
    assert result.failed_entities == 3
    assert calls == 3


def test_coverage_snapshot_uses_deterministic_state_precedence(
    env_mapping: dict[str, str], runtime_root: Path
) -> None:
    settings = _settings(env_mapping)
    database = IdentityDatabase(settings.database_path)
    database.migrate()
    canary = _write_canary(runtime_root, count=1)
    campaign = _write_candidate_artifact(runtime_root)
    output_root = runtime_root / "coverage"
    addresses = BTC_ADDRESSES

    with database.write_transaction() as connection:
        connection.execute(
            """
            INSERT INTO ingestion_run (
                ingestion_run_id, mode, status, started_at, completed_at,
                request_limit, response_bytes_budget
            ) VALUES (
                'run', 'execute', 'completed', '2026-07-25T00:00:00Z',
                '2026-07-25T00:00:00Z', 25, 1048576
            )
            """
        )
        connection.execute(
            """
            INSERT INTO source_observation (
                observation_id, source_id, source_version, source_kind,
                endpoint_template, query_profile, requested_at, completed_at,
                http_status, outcome, response_bytes, payload_sha256,
                chain_key, address_id, ingestion_run_id
            ) VALUES (
                'prediction-observation', 'fixture', 'fixture-v1', 'import',
                'fixture', 'fixture', '2026-07-25T00:00:00Z',
                '2026-07-25T00:00:00Z', 200, 'success', 0, NULL,
                'bitcoin', NULL, 'run'
            )
            """
        )
        for address in addresses[1:]:
            subject = normalize_bitcoin_address(address)
            connection.execute(
                """
                INSERT OR IGNORE INTO address_subject (
                    address_id, chain_key, normalized_address, display_address,
                    address_type, first_seen_at
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
            INSERT INTO coverage_entity_prediction (
                prediction_id, prediction_fingerprint, observation_id,
                provider_entity_id, address_id, prediction_rank, observed_at
            ) VALUES (
                'prediction', 'prediction-fingerprint', 'prediction-observation',
                'coinbase', ?, 1, '2026-07-25T00:00:00Z'
            )
            """,
            (normalize_bitcoin_address(addresses[1]).address_id,),
        )
        connection.execute(
            """
            INSERT INTO coverage_entity_prediction (
                prediction_id, prediction_fingerprint, observation_id,
                provider_entity_id, address_id, prediction_rank, observed_at
            ) VALUES (
                'conflict-prediction', 'conflict-prediction-fingerprint',
                'prediction-observation', 'coinbase', ?, 2,
                '2026-07-25T00:00:00Z'
            )
            """,
            (normalize_bitcoin_address(addresses[3]).address_id,),
        )
        connection.execute(
            """
            INSERT INTO identity_evidence (
                evidence_id, evidence_fingerprint, address_id, observation_id,
                assertion_type, candidate_entity_id, candidate_entity_name,
                candidate_label, candidate_wallet_role, provider_entity_id,
                provider_tag_id, source_authority, evidence_tier,
                verification_method, verification_result, source_url,
                artifact_sha256, license_ref, independence_group, asserted_at,
                observed_at, effective_from, effective_to, expires_at,
                evidence_status, imported_by
            ) VALUES (
                'evidence', 'evidence-fingerprint', ?, NULL, 'entity_control',
                'local-fund', 'Local Fund', NULL, NULL, NULL, NULL,
                'official', 'A', 'fixture', 'passed', 'https://example.test',
                NULL, 'fixture-license', 'fixture', NULL,
                '2026-07-25T00:00:00Z', NULL, NULL, NULL, 'valid', 'pytest'
            )
            """,
            (normalize_bitcoin_address(addresses[2]).address_id,),
        )
        connection.execute(
            """
            INSERT INTO conflict_set (
                conflict_set_id, address_id, assertion_type, created_at,
                resolved_at, status
            ) VALUES (
                'conflict', ?, 'entity_control', '2026-07-25T00:00:00Z',
                NULL, 'active'
            )
            """,
            (normalize_bitcoin_address(addresses[3]).address_id,),
        )
    CandidateService(database).import_candidates(
        [
            CandidateInput(
                chain_key="bitcoin",
                address=addresses[4],
                reason="manual_review",
                priority=90,
                source_reference="fixture:address-level-requirement",
                requested_at=datetime(2026, 7, 25, tzinfo=UTC),
            )
        ],
        created_at=datetime(2026, 7, 25, tzinfo=UTC),
    )

    result = BtcV2SCoverageSnapshotBuilder(database).build(
        campaign_root=campaign,
        canary_root=canary,
        output_root=output_root,
        built_at=datetime(2026, 7, 25, tzinfo=UTC),
    )

    table = pq.read_table(result.parquet_path)
    rows = {
        row["normalized_address"]: row
        for row in table.to_pylist()
    }
    assert rows[addresses[0]]["coverage_state"] == "direct_enriched"
    assert rows[addresses[1]]["coverage_state"] == "entity_membership_covered"
    assert rows[addresses[2]]["coverage_state"] == "local_evidence_covered"
    assert rows[addresses[3]]["coverage_state"] == "needs_direct_enrichment"
    assert rows[addresses[4]]["coverage_state"] == "needs_direct_enrichment"
    assert result.state_counts == {
        "direct_enriched": 1,
        "entity_membership_covered": 1,
        "local_evidence_covered": 1,
        "needs_direct_enrichment": 2,
    }
    assert result.candidate_rows == 5
    assert result.intersected_prediction_addresses == 2
    assert result.manifest_path.is_file()
