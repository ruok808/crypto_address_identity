from __future__ import annotations

import hashlib

import pytest

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


def test_uncontested_tier_c_entity_resolves_as_provider_default(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    EvidenceService(database, VerifierRegistry()).import_records([_record(entity_id="arkham:example")])

    result = ResolverService(database).rebuild(as_of="2026-07-22T01:00:00Z")
    resolution = ResolverService(database).show("bitcoin", BTC_ADDRESS)

    assert result.resolution_count == 1
    assert resolution.state == "resolved"
    assert resolution.operational_tier == "lookup_usable"
    assert resolution.resolution_policy == "provider_default"
    assert resolution.accepted_entity == "arkham:example"
    assert resolution.accepted_entity_display == "arkham example"


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
    assert resolution.resolution_policy == "conflict_first"
    assert resolution.conflict_set_id is not None
    assert resolution.accepted_entity is None


def test_local_override_selects_a_supported_claim_without_erasing_the_conflict(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    evidence = EvidenceService(database, VerifierRegistry())
    provider = _record(entity_id="arkham:gemini", source_suffix="provider")
    provider = provider.model_copy(update={"candidate_entity_name": "Gemini"})
    local_correction = _record(
        entity_id="local:okx",
        source_authority="local_inference",
        source_suffix="local-correction",
    )
    local_correction = local_correction.model_copy(update={"candidate_entity_name": "OKX"})
    evidence.import_records([provider, local_correction])
    resolver = ResolverService(database)

    resolver.rebuild(as_of="2026-07-22T01:00:00Z")
    assert resolver.show("bitcoin", BTC_ADDRESS).state == "ambiguous"
    override_id = resolver.record_local_override(
        chain_key="bitcoin",
        address=BTC_ADDRESS,
        assertion_type="entity_control",
        asserted_value=canonical_asserted_value(local_correction),
        decision="select",
        reviewer_ref="fixture-local-review",
        reason_ref="https://example.test/local-correction",
        reviewed_at="2026-07-22T02:00:00Z",
    )

    resolver.rebuild(as_of="2026-07-22T03:00:00Z")
    resolution = resolver.show("bitcoin", BTC_ADDRESS)

    assert override_id
    assert resolution.state == "resolved"
    assert resolution.operational_tier == "lookup_usable"
    assert resolution.resolution_policy == "local_override"
    assert resolution.accepted_entity == "local:okx"
    assert resolution.accepted_entity_display == "OKX"
    assert resolution.conflict_set_id is not None


def test_local_override_rejection_removes_only_the_rejected_provider_claim(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    provider = _record(entity_id="arkham:gemini")
    EvidenceService(database, VerifierRegistry()).import_records([provider])
    resolver = ResolverService(database)
    resolver.record_local_override(
        chain_key="bitcoin",
        address=BTC_ADDRESS,
        assertion_type="entity_control",
        asserted_value=canonical_asserted_value(provider),
        decision="reject",
        reviewer_ref="fixture-local-review",
        reason_ref="https://example.test/rejection",
        reviewed_at="2026-07-22T01:00:00Z",
    )

    resolver.rebuild(as_of="2026-07-22T02:00:00Z")
    resolution = resolver.show("bitcoin", BTC_ADDRESS)

    assert resolution.state == "unattributed"
    assert resolution.operational_tier == "none"
    assert resolution.resolution_policy == "local_override"


def test_local_override_cannot_invent_an_unsupported_value(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    EvidenceService(database, VerifierRegistry()).import_records([_record(entity_id="arkham:gemini")])

    with pytest.raises(ValueError, match="requires existing evidence"):
        ResolverService(database).record_local_override(
            chain_key="bitcoin",
            address=BTC_ADDRESS,
            assertion_type="entity_control",
            asserted_value="unrecorded entity",
            decision="select",
            reviewer_ref="fixture-local-review",
            reason_ref="https://example.test/unsupported",
            reviewed_at="2026-07-22T01:00:00Z",
        )


def test_same_entity_name_from_different_source_ids_corroborates_one_claim(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    evidence = EvidenceService(database, VerifierRegistry())
    provider = _record(entity_id="arkham:okx", source_suffix="provider")
    official = _record(
        entity_id="official:okx", tier="B", source_authority="official", source_suffix="official"
    )
    provider = provider.model_copy(update={"candidate_entity_name": "OKX"})
    official = official.model_copy(update={"candidate_entity_name": "OKX"})
    evidence.import_records([provider, official])

    ResolverService(database).rebuild(as_of="2026-07-22T01:00:00Z")
    resolution = ResolverService(database).show("bitcoin", BTC_ADDRESS)

    assert resolution.state == "resolved"
    assert resolution.operational_tier == "discovery_only"
    assert resolution.conflict_set_id is None


def test_provider_tags_are_evidence_not_competing_primary_address_labels(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    evidence = EvidenceService(database, VerifierRegistry())
    primary = EvidenceInput.model_validate(
        {
            "chain_key": "bitcoin",
            "address": BTC_ADDRESS,
            "assertion_type": "address_label",
            "candidate_label": "Primary label",
            "source_authority": "commercial_provider",
            "evidence_tier": "C",
            "verification_method": "api-observation",
            "source_url": "https://example.test/provider",
            "license_ref": "fixture-license",
            "independence_group": "fixture-provider",
            "observed_at": "2026-07-22T00:00:00Z",
            "imported_by": "fixture",
        }
    )
    tag = primary.model_copy(update={"candidate_label": "Hot", "provider_tag_id": "tag-hot"})
    evidence.import_records([primary, tag])

    ResolverService(database).rebuild(as_of="2026-07-22T01:00:00Z")
    resolution = ResolverService(database).show("bitcoin", BTC_ADDRESS, assertion_type="address_label")

    assert resolution.state == "resolved"
    assert resolution.operational_tier == "discovery_only"
    assert resolution.conflict_set_id is None


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
