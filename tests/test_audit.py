from __future__ import annotations

from datetime import UTC, datetime

from crypto_address_identity.audit import (
    build_provider_reliability_panel,
    seed_official_calibration_candidates,
)
from crypto_address_identity.candidates import CandidateInput, CandidateService
from crypto_address_identity.chains.bitcoin import normalize_bitcoin_address
from crypto_address_identity.evidence import EvidenceInput, EvidenceService, StaticProofVerifier, VerificationResult, VerifierRegistry
from crypto_address_identity.providers.zero_x_router import ProviderEvidenceCandidate
from crypto_address_identity.storage.sqlite import IdentityDatabase


FIRST_ADDRESS = "1BoatSLRHtKNngkdXEeobR76b53LETtpyT"
SECOND_ADDRESS = "3QJmV3qfvL9SuYo34YihAf3sRCW3qSinyC"
PREFIX = "quant_crypto:btc_whale:30d:"


def test_provider_panel_separates_coverage_from_official_conflict(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    CandidateService(database).import_candidates(
        [
            CandidateInput(
                chain_key="bitcoin",
                address=FIRST_ADDRESS,
                reason="replay",
                priority=60,
                source_reference=PREFIX + "unknown_to_unknown",
                requested_at=datetime(2026, 7, 22, tzinfo=UTC),
            ),
            CandidateInput(
                chain_key="bitcoin",
                address=SECOND_ADDRESS,
                reason="replay",
                priority=60,
                source_reference=PREFIX + "unknown_to_exchange_wallet",
                requested_at=datetime(2026, 7, 22, tzinfo=UTC),
            ),
        ]
    )
    _record_provider_observation(database, FIRST_ADDRESS, "obs-first")
    _record_provider_observation(database, SECOND_ADDRESS, "obs-second")
    evidence = EvidenceService(database, VerifierRegistry())
    evidence.append_provider_candidates(
        address=FIRST_ADDRESS,
        observation_id="obs-first",
        candidates=(
            ProviderEvidenceCandidate(
                assertion_type="entity_control",
                candidate_entity_id="arkham:provider",
                candidate_entity_name="Provider Entity",
                candidate_label=None,
                candidate_wallet_role=None,
                provider_entity_id="provider",
                provider_tag_id=None,
                evidence_tier="C",
            ),
        ),
        source_url="https://0xrouter.app",
        artifact_sha256=None,
        observed_at=datetime(2026, 7, 22, tzinfo=UTC),
    )
    official = EvidenceInput.model_validate(
        {
            "chain_key": "bitcoin",
            "address": FIRST_ADDRESS,
            "assertion_type": "entity_control",
            "candidate_entity_id": "official:other",
            "candidate_entity_name": "Other Entity",
            "source_authority": "official",
            "evidence_tier": "A",
            "verification_method": "fixture-proof",
            "verification_result": "valid",
            "source_url": "https://example.test/proof",
            "artifact_sha256": "a" * 64,
            "license_ref": "fixture",
            "independence_group": "fixture-official",
            "observed_at": "2026-07-22T00:00:00Z",
            "imported_by": "fixture",
        }
    )
    EvidenceService(
        database,
        VerifierRegistry([StaticProofVerifier("fixture-proof", VerificationResult.VALID)]),
    ).import_records([official])

    panel = build_provider_reliability_panel(database, source_reference_prefix=PREFIX)

    assert panel["candidate_addresses"] == 2
    assert panel["provider_outcome_counts"] == {"success": 2}
    assert panel["entity_name_supported_count"] == 1
    assert panel["fully_empty_attribution_count"] == 1
    assert panel["formal_wallet_role_supported_count"] == 0
    assert panel["official_entity_comparable_count"] == 1
    assert panel["official_entity_conflict_count"] == 1
    assert panel["interpretation"]["provider_entity_precision_supported"] is True
    assert panel["interpretation"]["provider_entity_precision_passed"] is False
    assert panel["interpretation"]["provider_wallet_role_precision_supported"] is False


def test_official_calibration_seed_is_dry_run_safe_and_preserves_provenance(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    evidence = EvidenceService(database, VerifierRegistry())
    evidence.import_records(
        [
            _official_record(
                FIRST_ADDRESS,
                entity_name="Bitwise Bitcoin ETF (BITB)",
                evidence_tier="B",
                independence_group="bitwise_bitb_public_wallets",
            ),
            _official_record(
                SECOND_ADDRESS,
                entity_name="Bitwise Bitcoin ETF (BITB)",
                evidence_tier="B",
                independence_group="bitwise_bitb_public_wallets",
            ),
        ]
    )

    dry_run = seed_official_calibration_candidates(
        database,
        independence_group="bitwise_bitb_public_wallets",
        source_reference="calibration:bitwise_bitb:2026-07-23",
        requested_at=datetime(2026, 7, 23, tzinfo=UTC),
        dry_run=True,
    )

    assert dry_run.eligible_address_count == 2
    assert dry_run.imported_count == 0
    with database.read_connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM candidate_request").fetchone()[0] == 0

    written = seed_official_calibration_candidates(
        database,
        independence_group="bitwise_bitb_public_wallets",
        source_reference="calibration:bitwise_bitb:2026-07-23",
        requested_at=datetime(2026, 7, 23, tzinfo=UTC),
    )

    assert written.eligible_address_count == 2
    assert written.imported_count == 2
    with database.read_connection() as connection:
        rows = connection.execute(
            "SELECT reason, priority, source_reference FROM candidate_request ORDER BY source_reference"
        ).fetchall()
    assert [tuple(row) for row in rows] == [
        ("official_evidence", 70, "calibration:bitwise_bitb:2026-07-23"),
        ("official_evidence", 70, "calibration:bitwise_bitb:2026-07-23"),
    ]


def test_provider_panel_compares_tier_b_only_when_explicitly_requested(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    CandidateService(database).import_candidates(
        [
            CandidateInput(
                chain_key="bitcoin",
                address=FIRST_ADDRESS,
                reason="official_evidence",
                priority=70,
                source_reference="calibration:bitwise_bitb:2026-07-23",
                requested_at=datetime(2026, 7, 23, tzinfo=UTC),
            )
        ]
    )
    _record_provider_observation(database, FIRST_ADDRESS, "obs-bitwise")
    EvidenceService(database, VerifierRegistry()).append_provider_candidates(
        address=FIRST_ADDRESS,
        observation_id="obs-bitwise",
        candidates=(
            ProviderEvidenceCandidate(
                assertion_type="entity_control",
                candidate_entity_id="arkham:bitwise",
                candidate_entity_name="Bitwise Bitcoin ETF (BITB)",
                candidate_label=None,
                candidate_wallet_role=None,
                provider_entity_id="bitwise",
                provider_tag_id=None,
                evidence_tier="C",
            ),
        ),
        source_url="https://0xrouter.app",
        artifact_sha256=None,
        observed_at=datetime(2026, 7, 23, tzinfo=UTC),
    )
    EvidenceService(database, VerifierRegistry()).import_records(
        [
            _official_record(
                FIRST_ADDRESS,
                entity_name="Bitwise Bitcoin ETF (BITB)",
                evidence_tier="B",
                independence_group="bitwise_bitb_public_wallets",
            )
        ]
    )

    default_panel = build_provider_reliability_panel(
        database, source_reference_prefix="calibration:bitwise_bitb:"
    )
    bitb_panel = build_provider_reliability_panel(
        database,
        source_reference_prefix="calibration:bitwise_bitb:",
        official_evidence_tiers=("B",),
        official_independence_group="bitwise_bitb_public_wallets",
    )

    assert default_panel["official_entity_comparable_count"] == 0
    assert bitb_panel["official_entity_comparable_count"] == 1
    assert bitb_panel["official_entity_match_count"] == 1
    assert bitb_panel["official_evidence_tiers"] == ["B"]
    assert bitb_panel["interpretation"]["official_comparison_basis"] == "tier_b_direct_publication"
    assert bitb_panel["interpretation"]["signature_verified_reference"] is False
    assert bitb_panel["interpretation"]["provider_entity_precision_passed"] is True


def _record_provider_observation(database: IdentityDatabase, address: str, observation_id: str) -> None:
    subject = normalize_bitcoin_address(address)
    with database.write_transaction() as connection:
        connection.execute(
            """
            INSERT INTO source_observation (
                observation_id, source_id, source_version, source_kind,
                endpoint_template, query_profile, requested_at, completed_at,
                http_status, outcome, response_bytes, chain_key, address_id
            ) VALUES (?, '0xrouter', 'fixture', 'provider', '/fixture', 'discovery',
                      '2026-07-22T00:00:00Z', '2026-07-22T00:00:00Z', 200, 'success', 1,
                      'bitcoin', ?)
            """,
            (observation_id, subject.address_id),
        )


def _official_record(
    address: str,
    *,
    entity_name: str,
    evidence_tier: str,
    independence_group: str,
) -> EvidenceInput:
    return EvidenceInput.model_validate(
        {
            "chain_key": "bitcoin",
            "address": address,
            "assertion_type": "entity_control",
            "candidate_entity_id": f"official:{independence_group}",
            "candidate_entity_name": entity_name,
            "source_authority": "official",
            "evidence_tier": evidence_tier,
            "verification_method": "fixture-publication",
            "source_url": "https://example.test/publication",
            "artifact_sha256": "c" * 64,
            "license_ref": "fixture",
            "independence_group": independence_group,
            "observed_at": "2026-07-23T00:00:00Z",
            "imported_by": "fixture",
        }
    )
