from __future__ import annotations

from pathlib import Path

from crypto_address_identity.consumers.quant_crypto_btc import IdentityEnricher, replay_events
from crypto_address_identity.evidence import EvidenceInput, EvidenceService, VerifierRegistry
from crypto_address_identity.exports import ResolverExporter
from crypto_address_identity.resolver import ResolverService
from crypto_address_identity.storage.sqlite import IdentityDatabase


BTC_ADDRESS = "1BoatSLRHtKNngkdXEeobR76b53LETtpyT"


def _snapshot(runtime_root: Path) -> Path:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    record = EvidenceInput.model_validate(
        {
            "chain_key": "bitcoin",
            "address": BTC_ADDRESS,
            "assertion_type": "entity_control",
            "candidate_entity_id": "arkham:one",
            "candidate_entity_name": "Example",
            "source_authority": "commercial_provider",
            "evidence_tier": "C",
            "verification_method": "api-observation",
            "source_url": "https://example.test/source",
            "artifact_sha256": "a" * 64,
            "license_ref": "fixture-license",
            "independence_group": "fixture-provider",
            "observed_at": "2026-07-22T00:00:00Z",
            "evidence_status": "valid",
            "imported_by": "fixture",
        }
    )
    EvidenceService(database, VerifierRegistry()).import_records([record])
    ResolverService(database).rebuild(as_of="2026-07-22T01:00:00Z")
    return ResolverExporter(database, runtime_root / "exports").export(
        chain_key="bitcoin", as_of="2026-07-22T01:00:00Z"
    ).directory


def test_enricher_returns_identity_caveat_without_business_action(runtime_root: Path) -> None:
    enricher = IdentityEnricher.from_snapshot_directory(_snapshot(runtime_root))

    lookup = enricher.lookup(BTC_ADDRESS)

    assert lookup.identity_lookup_status == "found"
    assert lookup.identity_state == "resolved"
    assert lookup.identity_operational_tier == "discovery_only"
    assert lookup.identity_entity_display is None


def test_invalid_snapshot_becomes_a_caveat_not_a_crash(runtime_root: Path) -> None:
    directory = _snapshot(runtime_root)
    (directory / "manifest.json").write_text("{}", encoding="utf-8")

    enricher = IdentityEnricher.from_snapshot_directory(directory)

    assert enricher.lookup(BTC_ADDRESS).identity_lookup_status == "snapshot_invalid"


def test_replay_preserves_existing_btc_business_decisions(runtime_root: Path) -> None:
    enricher = IdentityEnricher.from_snapshot_directory(_snapshot(runtime_root))
    event = {
        "event_id": "bitcoin:fixture:watched",
        "watched_address": BTC_ADDRESS,
        "amount_btc": "500.00000000",
        "direction": "watched_outflow",
        "threshold_result": "alert",
        "quality_decision": "allow",
        "alert_decision": "send",
        "ownership_semantics": "immediate_alert",
    }

    result = replay_events([event], enricher)

    assert result.changed_business_fields == 0
    enriched = result.events[0]
    for key in (
        "event_id",
        "amount_btc",
        "direction",
        "threshold_result",
        "quality_decision",
        "alert_decision",
        "ownership_semantics",
    ):
        assert enriched[key] == event[key]
    assert enriched["identity_lookup_status"] == "found"
    assert enriched["identity_state"] == "resolved"
