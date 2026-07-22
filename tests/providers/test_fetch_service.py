from __future__ import annotations

import json
from datetime import UTC, datetime

import httpx

from crypto_address_identity.candidates import CandidateInput, CandidateService
from crypto_address_identity.core.config import Settings
from crypto_address_identity.evidence import EvidenceService, VerifierRegistry
from crypto_address_identity.fetch import FetchService
from crypto_address_identity.providers.zero_x_router import ZeroXRouterClient
from crypto_address_identity.storage.raw_payloads import RawPayloadStore
from crypto_address_identity.storage.sqlite import IdentityDatabase


BTC_ADDRESS = "1BoatSLRHtKNngkdXEeobR76b53LETtpyT"


def _candidate(priority: int = 80) -> CandidateInput:
    return CandidateInput.model_validate(
        {
            "chain_key": "bitcoin",
            "address": BTC_ADDRESS,
            "reason": "whale_counterparty",
            "priority": priority,
            "source_reference": "fixture-event",
            "requested_at": "2026-07-22T00:00:00Z",
        }
    )


def _payload() -> dict:
    return {
        "bitcoin": {
            "address": BTC_ADDRESS,
            "chain": "bitcoin",
            "isUserAddress": True,
            "arkhamEntity": {"id": "entity-1", "name": "Example Exchange"},
        }
    }


def _service(runtime_root, env_mapping, handler):
    env_mapping["CAI_0XROUTER_TOKEN"] = "fixture-token"
    settings = Settings.model_validate(env_mapping)
    database = IdentityDatabase(settings.database_path)
    database.migrate()
    CandidateService(database).import_candidates([_candidate()])
    client = ZeroXRouterClient(settings, transport=httpx.MockTransport(handler))
    return database, FetchService(
        database=database,
        settings=settings,
        provider=client,
        raw_payloads=RawPayloadStore(database, settings.raw_payload_root),
        evidence=EvidenceService(database, VerifierRegistry()),
    )


def test_dry_run_selects_work_without_network_or_state_mutation(runtime_root, env_mapping) -> None:
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        return httpx.Response(200, json=_payload())

    database, service = _service(runtime_root, env_mapping, handler)
    before = {}
    with database.read_connection() as connection:
        for table in ("ingestion_run", "source_observation", "identity_evidence", "raw_payload_object"):
            before[table] = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    result = service.run(dry_run=True, limit=1, now=datetime(2026, 7, 22, tzinfo=UTC))

    assert result.status == "dry_run"
    assert result.selected_count == 1
    assert result.written_paths == ()
    assert calls == 0
    with database.read_connection() as connection:
        for table, count in before.items():
            assert connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == count


def test_execute_persists_raw_observation_and_tier_c_evidence(runtime_root, env_mapping) -> None:
    payload = json.dumps(_payload()).encode()
    database, service = _service(
        runtime_root,
        env_mapping,
        lambda request: httpx.Response(200, content=payload),
    )

    result = service.run(dry_run=False, limit=1, now=datetime(2026, 7, 22, tzinfo=UTC))

    assert result.status == "completed"
    assert result.request_count == 1
    assert result.response_bytes == len(payload)
    assert result.evidence_count == 1
    assert len(result.written_paths) == 1
    with database.read_connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM source_observation").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM raw_payload_object").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM identity_evidence").fetchone()[0] == 1


def test_rate_limit_is_audited_without_negative_evidence(runtime_root, env_mapping) -> None:
    database, service = _service(
        runtime_root, env_mapping, lambda request: httpx.Response(429, content=b"rate limited")
    )

    result = service.run(dry_run=False, limit=1, now=datetime(2026, 7, 22, tzinfo=UTC))

    assert result.status == "partial"
    assert result.outcome_counts["rate_limited"] == 1
    assert result.evidence_count == 0
    with database.read_connection() as connection:
        assert connection.execute("SELECT outcome FROM source_observation").fetchone()[0] == "rate_limited"
        assert connection.execute("SELECT COUNT(*) FROM identity_evidence").fetchone()[0] == 0


def test_malformed_provider_payload_is_audited_and_not_promoted(runtime_root, env_mapping) -> None:
    database, service = _service(
        runtime_root,
        env_mapping,
        lambda request: httpx.Response(200, json={"bitcoin": {"address": BTC_ADDRESS, "chain": "ethereum"}}),
    )

    result = service.run(dry_run=False, limit=1, now=datetime(2026, 7, 22, tzinfo=UTC))

    assert result.status == "partial"
    assert result.outcome_counts["malformed_payload"] == 1
    with database.read_connection() as connection:
        assert connection.execute("SELECT outcome FROM source_observation").fetchone()[0] == "malformed_payload"
        assert connection.execute("SELECT COUNT(*) FROM identity_evidence").fetchone()[0] == 0


def test_fresh_discovery_skips_repeat_provider_work(runtime_root, env_mapping) -> None:
    database, service = _service(
        runtime_root,
        env_mapping,
        lambda request: httpx.Response(200, content=json.dumps(_payload()).encode()),
    )
    service.run(dry_run=False, limit=1, now=datetime(2026, 7, 22, tzinfo=UTC))

    result = service.run(dry_run=True, limit=1, now=datetime(2026, 7, 22, 1, tzinfo=UTC))

    assert result.selected_count == 0
    assert result.skipped_fresh_count == 1
    assert result.request_count == 0


def test_high_priority_candidate_uses_detail_after_existing_discovery(runtime_root, env_mapping) -> None:
    database, service = _service(
        runtime_root,
        env_mapping,
        lambda request: httpx.Response(200, content=json.dumps(_payload()).encode()),
    )
    service.run(dry_run=False, limit=1, now=datetime(2026, 7, 22, tzinfo=UTC))
    CandidateService(database).import_candidates([_candidate(priority=95)])

    result = service.run(dry_run=True, limit=2, now=datetime(2026, 7, 22, 1, tzinfo=UTC))

    assert result.profile_counts == {"detail": 1}
    assert result.selected_count == 1


def test_repeated_candidate_provenance_does_not_duplicate_provider_request(runtime_root, env_mapping) -> None:
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        return httpx.Response(200, content=json.dumps(_payload()).encode())

    database, service = _service(runtime_root, env_mapping, handler)
    CandidateService(database).import_candidates([_candidate(priority=95)])

    result = service.run(dry_run=False, limit=2, now=datetime(2026, 7, 22, tzinfo=UTC))

    assert calls == 1
    assert result.selected_count == 1
    assert result.duplicate_candidate_count == 1
