from __future__ import annotations

from pathlib import Path

from crypto_address_identity.consumers.btc_whale_bilateral import replay_bilateral_whale_events
from crypto_address_identity.consumers.quant_crypto_btc import IdentityEnricher
from crypto_address_identity.evidence import EvidenceInput, EvidenceService, VerifierRegistry
from crypto_address_identity.exports import ResolverExporter
from crypto_address_identity.resolver import ResolverService
from crypto_address_identity.storage.sqlite import IdentityDatabase


OUTPUT_ADDRESS = "1BoatSLRHtKNngkdXEeobR76b53LETtpyT"
INPUT_ADDRESS = "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy"
CONFLICT_ADDRESS = "3QJmV3qfvL9SuYo34YihAf3sRCW3qSinyC"


def _record(address: str, *, entity_id: str, entity_name: str) -> EvidenceInput:
    return EvidenceInput.model_validate(
        {
            "chain_key": "bitcoin",
            "address": address,
            "assertion_type": "entity_control",
            "candidate_entity_id": entity_id,
            "candidate_entity_name": entity_name,
            "source_authority": "commercial_provider",
            "evidence_tier": "C",
            "verification_method": "api-observation",
            "source_url": f"https://example.test/{entity_id}",
            "artifact_sha256": "a" * 64,
            "license_ref": "fixture-license",
            "independence_group": f"fixture-{entity_id}",
            "observed_at": "2026-07-23T00:00:00Z",
            "evidence_status": "valid",
            "imported_by": "fixture",
        }
    )


def _snapshot(
    runtime_root: Path,
    *,
    conflicting_input: bool = False,
    additional_conflicting_input: bool = False,
) -> Path:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    records = [
        _record(OUTPUT_ADDRESS, entity_id="arkham:example-output", entity_name="Example Entity"),
        _record(INPUT_ADDRESS, entity_id="arkham:example-input", entity_name="Example Entity"),
    ]
    if conflicting_input:
        records.append(
            _record(INPUT_ADDRESS, entity_id="arkham:other-input", entity_name="Other Entity")
        )
    if additional_conflicting_input:
        records.extend(
            [
                _record(
                    CONFLICT_ADDRESS,
                    entity_id="arkham:conflict-a",
                    entity_name="Conflict A",
                ),
                _record(
                    CONFLICT_ADDRESS,
                    entity_id="arkham:conflict-b",
                    entity_name="Conflict B",
                ),
            ]
        )
    EvidenceService(database, VerifierRegistry()).import_records(records)
    ResolverService(database).rebuild(as_of="2026-07-23T01:00:00Z")
    return ResolverExporter(database, runtime_root / "exports").export(
        chain_key="bitcoin", as_of="2026-07-23T01:00:00Z"
    ).directory


def _event(**overrides: object) -> dict[str, object]:
    event: dict[str, object] = {
        "output_address": OUTPUT_ADDRESS,
        "input_addresses": [INPUT_ADDRESS],
        "missing_input_address_count": 0,
        "semantic_decision": "internal_candidate",
        "quality_tags": ["self_churn_possible", "possible_change"],
        "internal_transfer_score": 85,
        "ownership_transfer_score": 20,
        "status": "sent",
    }
    event.update(overrides)
    return event


def test_bilateral_replay_distinguishes_provider_default_from_independent_evidence(runtime_root: Path) -> None:
    impact = replay_bilateral_whale_events(
        [_event()], IdentityEnricher.from_snapshot_directory(_snapshot(runtime_root))
    )

    assert impact.events == 1
    assert impact.internal_candidate_events == 1
    assert impact.output_identity_found_events == 1
    assert impact.input_identity_found_addresses == 1
    assert impact.same_entity_bilateral_events == 1
    assert impact.same_entity_provider_default_events == 1
    assert impact.same_entity_independent_evidence_events == 0
    assert impact.source_strong_condition_events == 1
    assert impact.provider_default_suppression_candidates == 1
    assert impact.independent_evidence_suppression_candidates == 0
    assert impact.provider_default_candidate_status_counts == {"sent": 1}
    assert impact.live_action_changes == 0


def test_bilateral_replay_keeps_conflicts_out_of_suppression_candidates(runtime_root: Path) -> None:
    impact = replay_bilateral_whale_events(
        [_event()],
        IdentityEnricher.from_snapshot_directory(_snapshot(runtime_root, conflicting_input=True)),
    )

    assert impact.conflict_first_side_events == 1
    assert impact.same_entity_bilateral_events == 0
    assert impact.provider_default_suppression_candidates == 0
    assert impact.independent_evidence_suppression_candidates == 0


def test_bilateral_replay_excludes_a_same_entity_match_when_another_input_conflicts(
    runtime_root: Path,
) -> None:
    impact = replay_bilateral_whale_events(
        [_event(input_addresses=[INPUT_ADDRESS, CONFLICT_ADDRESS])],
        IdentityEnricher.from_snapshot_directory(
            _snapshot(runtime_root, additional_conflicting_input=True)
        ),
    )

    assert impact.same_entity_bilateral_events == 1
    assert impact.same_entity_provider_default_events == 1
    assert impact.conflict_first_side_events == 1
    assert impact.provider_default_suppression_candidates == 0


def test_bilateral_replay_reports_malformed_and_missing_input_context(runtime_root: Path) -> None:
    impact = replay_bilateral_whale_events(
        [
            _event(input_addresses=[], missing_input_address_count=1),
            {"output_address": OUTPUT_ADDRESS, "semantic_decision": "internal_candidate"},
        ],
        IdentityEnricher.from_snapshot_directory(_snapshot(runtime_root)),
    )

    assert impact.events == 2
    assert impact.events_with_missing_input_address == 1
    assert impact.malformed_events == 1
    assert impact.provider_default_suppression_candidates == 0


def test_bilateral_replay_excludes_partial_input_context_from_suppression_candidates(
    runtime_root: Path,
) -> None:
    impact = replay_bilateral_whale_events(
        [_event(missing_input_address_count=1)],
        IdentityEnricher.from_snapshot_directory(_snapshot(runtime_root)),
    )

    assert impact.same_entity_bilateral_events == 1
    assert impact.events_with_missing_input_address == 1
    assert impact.provider_default_suppression_candidates == 0
