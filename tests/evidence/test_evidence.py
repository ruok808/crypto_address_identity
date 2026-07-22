from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from crypto_address_identity.evidence import (
    EvidenceImportError,
    EvidenceInput,
    EvidenceService,
    StaticProofVerifier,
    VerificationResult,
    VerifierRegistry,
)
from crypto_address_identity.storage.sqlite import IdentityDatabase


BTC_ADDRESS = "1BoatSLRHtKNngkdXEeobR76b53LETtpyT"


def _evidence(*, tier: str = "B", verification_result: str | None = None) -> EvidenceInput:
    return EvidenceInput.model_validate(
        {
            "chain_key": "bitcoin",
            "address": BTC_ADDRESS,
            "assertion_type": "entity_control",
            "candidate_entity_id": "official:example-exchange",
            "candidate_entity_name": "Example Exchange",
            "source_authority": "official",
            "evidence_tier": tier,
            "verification_method": "published-list" if tier != "A" else "fixture-proof",
            "verification_result": verification_result,
            "source_url": "https://example.test/proof",
            "artifact_sha256": "a" * 64,
            "license_ref": "example-license",
            "independence_group": "example-official",
            "observed_at": "2026-07-22T00:00:00Z",
            "effective_from": "2026-07-01T00:00:00Z",
            "evidence_status": "valid",
            "imported_by": "fixture",
        }
    )


def test_import_creates_observation_and_append_only_evidence(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    service = EvidenceService(database, VerifierRegistry())

    result = service.import_records([_evidence()])

    assert result.inserted_count == 1
    assert result.duplicate_count == 0
    with database.read_connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM source_observation").fetchone()[0] == 1
        row = connection.execute(
            "SELECT evidence_tier, evidence_status, verification_method FROM identity_evidence"
        ).fetchone()
    assert tuple(row) == ("B", "valid", "published-list")


def test_duplicate_evidence_is_semantically_idempotent(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    service = EvidenceService(database, VerifierRegistry())

    first = service.import_records([_evidence()])
    second = service.import_records([_evidence()])

    assert first.inserted_count == 1
    assert second.inserted_count == 0
    assert second.duplicate_count == 1


@pytest.mark.parametrize("outcome", [VerificationResult.INVALID, VerificationResult.UNSUPPORTED])
def test_tier_a_rejects_non_valid_verifier_outcome(runtime_root, outcome: VerificationResult) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    registry = VerifierRegistry([StaticProofVerifier("fixture-proof", outcome)])
    service = EvidenceService(database, registry)

    with pytest.raises(EvidenceImportError, match="Tier A"):
        service.import_records([_evidence(tier="A", verification_result=outcome.value)])


def test_tier_a_accepts_named_valid_verifier(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    registry = VerifierRegistry([StaticProofVerifier("fixture-proof", VerificationResult.VALID)])
    service = EvidenceService(database, registry)

    result = service.import_records([_evidence(tier="A", verification_result="valid")])

    assert result.inserted_count == 1


def test_missing_provenance_or_secret_bearing_source_url_is_rejected() -> None:
    raw = _evidence().model_dump(mode="json")
    raw["license_ref"] = ""
    with pytest.raises(ValidationError):
        EvidenceInput.model_validate(raw)

    raw = _evidence().model_dump(mode="json")
    raw["source_url"] = "https://example.test/proof?token=bad"
    with pytest.raises(ValidationError):
        EvidenceInput.model_validate(raw)
