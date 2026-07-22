from __future__ import annotations

import hashlib

from crypto_address_identity.evidence import EvidenceInput, EvidenceService, VerifierRegistry
from crypto_address_identity.resolver import ResolverService, canonical_asserted_value
from crypto_address_identity.storage.sqlite import IdentityDatabase


BTC_ADDRESS = "1BoatSLRHtKNngkdXEeobR76b53LETtpyT"


def _record(
    *,
    entity_id: str,
    tier: str = "C",
    source_authority: str = "commercial_provider",
    source_suffix: str = "one",
    expires_at: str | None = None,
) -> EvidenceInput:
    return EvidenceInput.model_validate(
        {
            "chain_key": "bitcoin",
            "address": BTC_ADDRESS,
            "assertion_type": "entity_control",
            "candidate_entity_id": entity_id,
            "candidate_entity_name": entity_id.replace(":", " "),
            "source_authority": source_authority,
            "evidence_tier": tier,
            "verification_method": "published-list",
            "source_url": f"https://example.test/{source_suffix}",
            "artifact_sha256": hashlib.sha256(source_suffix.encode()).hexdigest(),
            "license_ref": "fixture-license",
            "independence_group": f"fixture-{source_suffix}",
            "observed_at": "2026-07-22T00:00:00Z",
            "effective_from": "2026-07-01T00:00:00Z",
            "expires_at": expires_at,
            "evidence_status": "valid",
            "imported_by": "fixture",
        }
    )


def test_tier_c_only_resolves_to_discovery_only(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    EvidenceService(database, VerifierRegistry()).import_records([_record(entity_id="arkham:example")])

    result = ResolverService(database).rebuild(as_of="2026-07-22T01:00:00Z")
    resolution = ResolverService(database).show("bitcoin", BTC_ADDRESS)

    assert result.resolution_count == 1
    assert resolution.state == "resolved"
    assert resolution.operational_tier == "discovery_only"
    assert resolution.accepted_entity is None


def test_conflicting_claims_resolve_to_ambiguous_not_a_winner(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    evidence = EvidenceService(database, VerifierRegistry())
    evidence.import_records(
        [
            _record(entity_id="arkham:one", source_suffix="one"),
            _record(entity_id="arkham:two", source_suffix="two"),
        ]
    )

    ResolverService(database).rebuild(as_of="2026-07-22T01:00:00Z")
    resolution = ResolverService(database).show("bitcoin", BTC_ADDRESS)

    assert resolution.state == "ambiguous"
    assert resolution.operational_tier == "lookup_only"
    assert resolution.conflict_set_id is not None
    assert resolution.accepted_entity is None


def test_tier_b_requires_explicit_review_before_lookup_usable(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    record = _record(
        entity_id="official:one", tier="B", source_authority="official", source_suffix="official"
    )
    EvidenceService(database, VerifierRegistry()).import_records([record])
    resolver = ResolverService(database)

    resolver.rebuild(as_of="2026-07-22T01:00:00Z")
    before_review = resolver.show("bitcoin", BTC_ADDRESS)
    resolver.record_review(
        chain_key="bitcoin",
        address=BTC_ADDRESS,
        assertion_type="entity_control",
        asserted_value=canonical_asserted_value(record),
        reviewer_ref="fixture-review",
        decision="accept",
        reviewed_at="2026-07-22T02:00:00Z",
    )
    resolver.rebuild(as_of="2026-07-22T03:00:00Z")
    after_review = resolver.show("bitcoin", BTC_ADDRESS)

    assert before_review.operational_tier == "discovery_only"
    assert after_review.state == "resolved"
    assert after_review.operational_tier == "lookup_usable"
    assert after_review.accepted_entity == "official:one"


def test_expired_evidence_resolves_to_stale(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    EvidenceService(database, VerifierRegistry()).import_records(
        [_record(entity_id="arkham:old", expires_at="2026-07-21T00:00:00Z")]
    )

    ResolverService(database).rebuild(as_of="2026-07-22T01:00:00Z")
    resolution = ResolverService(database).show("bitcoin", BTC_ADDRESS)

    assert resolution.state == "stale"
    assert resolution.operational_tier == "none"


def test_same_as_of_rebuild_is_reproducible(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    EvidenceService(database, VerifierRegistry()).import_records([_record(entity_id="arkham:one")])
    resolver = ResolverService(database)

    first = resolver.rebuild(as_of="2026-07-22T01:00:00Z")
    second = resolver.rebuild(as_of="2026-07-22T01:00:00Z")

    assert first.resolution_ids == second.resolution_ids
