from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import httpx

from crypto_address_identity.candidates import CandidateInput, CandidateService
from crypto_address_identity.core.config import Settings
from crypto_address_identity.coverage import (
    CoverageEntitySeedInput,
    CoverageEntitySeedService,
    CoverageSyncService,
    parse_discovery_entities,
    parse_prediction_addresses,
)
from crypto_address_identity.evidence import EvidenceService, VerifierRegistry
from crypto_address_identity.providers.zero_x_router import ZeroXRouterClient
from crypto_address_identity.storage.raw_payloads import RawPayloadStore
from crypto_address_identity.storage.sqlite import IdentityDatabase


BTC_ADDRESS = "1BoatSLRHtKNngkdXEeobR76b53LETtpyT"
SECOND_BTC_ADDRESS = "3QJmV3qfvL9SuYo34YihAf3sRCW3qSinyC"
THIRD_BTC_ADDRESS = "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy"


def _settings(env_mapping: dict[str, str]) -> Settings:
    env_mapping["CAI_0XROUTER_TOKEN"] = "fixture-token"
    env_mapping["CAI_CHAINDATA_COVERAGE_MAX_ENTITIES_PER_RUN"] = "5"
    env_mapping["CAI_CHAINDATA_COVERAGE_MAX_ADDRESSES_PER_RUN"] = "5"
    return Settings.model_validate(env_mapping)


def _service(env_mapping: dict[str, str], handler):
    settings = _settings(env_mapping)
    database = IdentityDatabase(settings.database_path)
    database.migrate()
    client = ZeroXRouterClient(settings, transport=httpx.MockTransport(handler))
    return database, CoverageSyncService(
        database=database,
        settings=settings,
        provider=client,
        raw_payloads=RawPayloadStore(database, settings.raw_payload_root),
        evidence=EvidenceService(database, VerifierRegistry()),
    )


def _candidate() -> CandidateInput:
    return CandidateInput.model_validate(
        {
            "chain_key": "bitcoin",
            "address": BTC_ADDRESS,
            "reason": "whale_counterparty",
            "priority": 95,
            "source_reference": "fixture:whale",
            "requested_at": "2026-07-24T00:00:00Z",
        }
    )


def test_coverage_parsers_dedupe_entities_and_only_keep_valid_bitcoin_predictions() -> None:
    entities = parse_discovery_entities(
        json.dumps(
            {
                "entities": [
                    {"id": "binance", "name": "Binance", "type": "exchange"},
                    {"id": "binance", "name": "Binance", "type": "exchange"},
                    {"id": "coinbase", "name": "Coinbase", "type": "exchange"},
                ]
            }
        ).encode()
    )
    predictions = parse_prediction_addresses(
        json.dumps(
            {"addresses": [BTC_ADDRESS, {"address": SECOND_BTC_ADDRESS}, "not-a-bitcoin-address", BTC_ADDRESS]}
        ).encode()
    )

    assert [entity.provider_entity_id for entity in entities] == ["binance", "coinbase"]
    assert predictions == (BTC_ADDRESS, SECOND_BTC_ADDRESS)


def test_entity_seeds_are_append_only_and_deduplicated(env_mapping: dict[str, str]) -> None:
    settings = _settings(env_mapping)
    database = IdentityDatabase(settings.database_path)
    database.migrate()
    service = CoverageEntitySeedService(database)
    seed = CoverageEntitySeedInput.model_validate(
        {
            "provider_entity_id": "binance",
            "priority": 90,
            "source_reference": "fixture:seed",
            "requested_at": "2026-07-24T00:00:00Z",
        }
    )

    first = service.import_seeds([seed], created_at=datetime(2026, 7, 24, tzinfo=UTC))
    second = service.import_seeds([seed], created_at=datetime(2026, 7, 24, 1, tzinfo=UTC))

    assert first.inserted_count == 1
    assert second.duplicate_count == 1
    with database.read_connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM coverage_entity_seed").fetchone()[0] == 1


def test_coverage_dry_run_never_calls_provider_or_mutates_state(env_mapping: dict[str, str]) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={})

    database, service = _service(env_mapping, handler)
    CoverageEntitySeedService(database).import_seeds(
        [
            CoverageEntitySeedInput.model_validate(
                {
                    "provider_entity_id": "binance",
                    "source_reference": "fixture:seed",
                    "requested_at": "2026-07-24T00:00:00Z",
                }
            )
        ]
    )
    CandidateService(database).import_candidates([_candidate()])
    before = {}
    with database.read_connection() as connection:
        for table in ("ingestion_run", "source_observation", "coverage_entity_observation", "coverage_entity_prediction"):
            before[table] = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    result = service.run(dry_run=True, now=datetime(2026, 7, 24, tzinfo=UTC))

    assert result.status == "dry_run"
    assert result.entity_discovery_requests == 2
    assert result.entity_count == 1
    assert result.address_enrichment_requests == 1
    assert calls == 0
    with database.read_connection() as connection:
        for table, expected in before.items():
            assert connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == expected


def test_coverage_execute_fans_out_entities_without_repeating_fresh_detail_or_address(
    env_mapping: dict[str, str]
) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path.endswith("/entity_balance_changes"):
            return httpx.Response(200, json={"entities": [{"id": "binance", "name": "Binance", "type": "exchange"}]})
        if request.url.path.endswith("/entity/binance"):
            return httpx.Response(200, json={"entity": {"id": "binance", "name": "Binance", "type": "exchange"}})
        if request.url.path.endswith("/entity_predictions/binance"):
            return httpx.Response(200, json={"addresses": [SECOND_BTC_ADDRESS, {"address": THIRD_BTC_ADDRESS}]})
        if request.url.path.endswith(f"/address_enriched/{BTC_ADDRESS}/all"):
            return httpx.Response(
                200,
                json={
                    "bitcoin": {
                        "address": BTC_ADDRESS,
                        "chain": "bitcoin",
                        "arkhamEntity": {"id": "binance", "name": "Binance"},
                    }
                },
            )
        if request.url.path.endswith(f"/address_enriched/{SECOND_BTC_ADDRESS}/all"):
            return httpx.Response(
                200,
                json={"bitcoin": {"address": SECOND_BTC_ADDRESS, "chain": "bitcoin"}},
            )
        if request.url.path.endswith(f"/address_enriched/{THIRD_BTC_ADDRESS}/all"):
            return httpx.Response(
                200,
                json={"bitcoin": {"address": THIRD_BTC_ADDRESS, "chain": "bitcoin"}},
            )
        return httpx.Response(404, json={})

    database, service = _service(env_mapping, handler)
    CoverageEntitySeedService(database).import_seeds(
        [
            CoverageEntitySeedInput.model_validate(
                {
                    "provider_entity_id": "binance",
                    "source_reference": "fixture:seed",
                    "requested_at": "2026-07-24T00:00:00Z",
                }
            )
        ]
    )
    CandidateService(database).import_candidates([_candidate()])
    now = datetime(2026, 7, 24, tzinfo=UTC)

    first = service.run(dry_run=False, now=now)
    second = service.run(dry_run=False, now=now + timedelta(hours=1))

    assert first.status == "completed"
    assert first.entity_discovery_requests == 2
    assert first.entity_detail_requests == 1
    assert first.prediction_requests == 1
    assert first.prediction_address_count == 2
    assert first.address_enrichment_requests == 3
    assert first.address_evidence_count == 1
    assert first.estimated_points >= 1
    assert second.entity_detail_requests == 0
    assert second.prediction_requests == 0
    assert second.address_enrichment_requests == 0
    with database.read_connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM coverage_entity_prediction").fetchone()[0] == 2
        assert connection.execute("SELECT COUNT(*) FROM identity_evidence").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM raw_payload_object").fetchone()[0] == 6
    assert calls.count("/chaindata/intelligence/entity_predictions/binance") == 1


def test_malformed_address_enrichment_does_not_enter_the_ttl_cache(env_mapping: dict[str, str]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/entity_balance_changes"):
            return httpx.Response(200, json={"entities": [{"id": "binance"}]})
        if request.url.path.endswith("/entity/binance"):
            return httpx.Response(200, json={"entity": {"id": "binance"}})
        if request.url.path.endswith("/entity_predictions/binance"):
            return httpx.Response(200, json={"addresses": [SECOND_BTC_ADDRESS]})
        if request.url.path.endswith(f"/address_enriched/{BTC_ADDRESS}/all"):
            return httpx.Response(200, json={"bitcoin": {"address": BTC_ADDRESS, "chain": "ethereum"}})
        return httpx.Response(404, json={})

    database, service = _service(env_mapping, handler)
    CandidateService(database).import_candidates([_candidate()])
    now = datetime(2026, 7, 24, tzinfo=UTC)
    first = service.run(dry_run=False, now=now)
    second = service.run(dry_run=False, now=now + timedelta(hours=1))

    assert first.status == "partial"
    assert first.outcome_counts["malformed_payload"] == 1
    # The malformed candidate is retried, and the entity-prediction address is
    # separately attempted because neither has a successful parse cache entry.
    assert second.address_enrichment_requests == 2
    with database.read_connection() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM coverage_address_parse_result WHERE parse_outcome = 'malformed_payload'"
        ).fetchone()[0] == 2
