"""Append-only evidence imports and proof-verification boundary."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Literal, Protocol
from urllib.parse import urlparse

from pydantic import BaseModel, Field, field_validator, model_validator

from crypto_address_identity.chains.bitcoin import normalize_bitcoin_address
from crypto_address_identity.providers.zero_x_router import ProviderEvidenceCandidate
from crypto_address_identity.storage.sqlite import IdentityDatabase


class EvidenceImportError(ValueError):
    """Raised when a record cannot safely enter the immutable evidence ledger."""


class VerificationResult(StrEnum):
    VALID = "valid"
    INVALID = "invalid"
    UNSUPPORTED = "unsupported"


class ProofVerifier(Protocol):
    name: str

    def verify(self, record: "EvidenceInput") -> VerificationResult: ...


@dataclass(frozen=True)
class StaticProofVerifier:
    """Fixture verifier used to exercise the Tier A verifier boundary."""

    name: str
    result: VerificationResult

    def verify(self, record: "EvidenceInput") -> VerificationResult:
        return self.result


class VerifierRegistry:
    def __init__(self, verifiers: list[ProofVerifier] | None = None) -> None:
        self._verifiers = {verifier.name: verifier for verifier in verifiers or []}

    def verify(self, record: "EvidenceInput") -> VerificationResult:
        verifier = self._verifiers.get(record.verification_method)
        return verifier.verify(record) if verifier else VerificationResult.UNSUPPORTED


AssertionType = Literal[
    "entity_control",
    "address_label",
    "wallet_role",
    "address_kind",
    "relationship",
]
EvidenceTier = Literal["A", "B", "C", "D", "E"]
EvidenceStatus = Literal["valid", "stale", "revoked", "disputed", "superseded"]


class EvidenceInput(BaseModel):
    """Versioned external or local assertion with mandatory provenance."""

    chain_key: str
    address: str
    assertion_type: AssertionType
    candidate_entity_id: str | None = None
    candidate_entity_name: str | None = None
    candidate_label: str | None = None
    candidate_wallet_role: str | None = None
    provider_entity_id: str | None = None
    provider_tag_id: str | None = None
    source_authority: Literal[
        "official", "regulator", "commercial_provider", "public_explorer", "local_inference"
    ]
    evidence_tier: EvidenceTier
    verification_method: str = Field(min_length=1, max_length=128)
    verification_result: VerificationResult | None = None
    source_url: str
    artifact_sha256: str | None = None
    license_ref: str = Field(min_length=1, max_length=512)
    independence_group: str = Field(min_length=1, max_length=256)
    asserted_at: datetime | None = None
    observed_at: datetime
    effective_from: datetime | None = None
    effective_to: datetime | None = None
    expires_at: datetime | None = None
    evidence_status: EvidenceStatus = "valid"
    imported_by: str = Field(min_length=1, max_length=128)

    @field_validator("chain_key")
    @classmethod
    def validate_chain(cls, value: str) -> str:
        if value.strip().lower() != "bitcoin":
            raise ValueError("BTC-first evidence import accepts only bitcoin")
        return "bitcoin"

    @field_validator("source_url")
    @classmethod
    def validate_public_source_url(cls, value: str) -> str:
        parsed = urlparse(value)
        prohibited = ("token", "apikey", "api_key", "authorization", "bearer")
        if (
            parsed.scheme != "https"
            or not parsed.netloc
            or parsed.username
            or parsed.password
            or any(marker in value.lower() for marker in prohibited)
        ):
            raise ValueError("source_url must be a public HTTPS URL without credentials or secrets")
        return value

    @field_validator("artifact_sha256")
    @classmethod
    def validate_artifact_hash(cls, value: str | None) -> str | None:
        if value is not None and (len(value) != 64 or any(char not in "0123456789abcdef" for char in value)):
            raise ValueError("artifact_sha256 must be a lower-case SHA-256 hex digest")
        return value

    @field_validator("observed_at", "asserted_at", "effective_from", "effective_to", "expires_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("evidence timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_contract(self) -> "EvidenceInput":
        normalize_bitcoin_address(self.address)
        if self.assertion_type == "entity_control" and not (
            self.candidate_entity_id or self.candidate_entity_name
        ):
            raise ValueError("entity_control requires a candidate entity")
        if self.assertion_type in {"address_label", "address_kind", "relationship"} and not self.candidate_label:
            raise ValueError(f"{self.assertion_type} requires candidate_label")
        if self.assertion_type == "wallet_role" and not self.candidate_wallet_role:
            raise ValueError("wallet_role requires candidate_wallet_role")
        if self.evidence_tier == "A" and self.verification_result is None:
            raise ValueError("Tier A evidence declares a verification result")
        if self.effective_to and self.effective_from and self.effective_to < self.effective_from:
            raise ValueError("effective_to must not precede effective_from")
        return self

    @property
    def subject(self):
        return normalize_bitcoin_address(self.address)


@dataclass(frozen=True)
class EvidenceImportResult:
    inserted_count: int
    duplicate_count: int
    evidence_ids: tuple[str, ...]


class EvidenceService:
    """Creates import observations then appends deduplicated evidence records."""

    def __init__(self, database: IdentityDatabase, verifiers: VerifierRegistry) -> None:
        self.database = database
        self.verifiers = verifiers

    def import_records(
        self, records: list[EvidenceInput], *, imported_at: datetime | None = None
    ) -> EvidenceImportResult:
        timestamp = _as_utc_string(imported_at or datetime.now(UTC))
        evidence_ids: list[str] = []
        inserted_count = 0
        duplicate_count = 0
        with self.database.write_transaction() as connection:
            for record in records:
                verification_result = self._verify(record)
                subject = record.subject
                self._ensure_subject(connection, subject, timestamp)
                observation_id = str(uuid.uuid4())
                connection.execute(
                    """
                    INSERT INTO source_observation (
                        observation_id, source_id, source_version, source_kind,
                        endpoint_template, query_profile, requested_at, completed_at,
                        http_status, outcome, chain_key, address_id
                    ) VALUES (?, ?, 'import_v1', 'import', '/evidence/import', 'import',
                              ?, ?, NULL, 'success', 'bitcoin', ?)
                    """,
                    (observation_id, record.source_authority, timestamp, timestamp, subject.address_id),
                )
                evidence_id = self._insert_evidence(
                    connection,
                    record=record,
                    subject=subject,
                    observation_id=observation_id,
                    verification_result=verification_result,
                )
                if evidence_id:
                    inserted_count += 1
                    evidence_ids.append(evidence_id)
                else:
                    duplicate_count += 1
        return EvidenceImportResult(inserted_count, duplicate_count, tuple(evidence_ids))

    def append_provider_candidates(
        self,
        *,
        address: str,
        observation_id: str,
        candidates: tuple[ProviderEvidenceCandidate, ...],
        source_url: str,
        artifact_sha256: str | None,
        observed_at: datetime,
    ) -> EvidenceImportResult:
        """Append Tier C candidates linked to an already-persisted provider observation."""

        subject = normalize_bitcoin_address(address)
        timestamp = _as_utc_string(observed_at)
        inserted_count = 0
        duplicate_count = 0
        evidence_ids: list[str] = []
        with self.database.write_transaction() as connection:
            self._ensure_subject(connection, subject, timestamp)
            for candidate in candidates:
                record = EvidenceInput.model_validate(
                    {
                        "chain_key": "bitcoin",
                        "address": subject.normalized_address,
                        "assertion_type": candidate.assertion_type,
                        "candidate_entity_id": candidate.candidate_entity_id,
                        "candidate_entity_name": candidate.candidate_entity_name,
                        "candidate_label": candidate.candidate_label,
                        "candidate_wallet_role": None,
                        "provider_entity_id": candidate.provider_entity_id,
                        "provider_tag_id": candidate.provider_tag_id,
                        "source_authority": "commercial_provider",
                        "evidence_tier": "C",
                        "verification_method": "api-observation",
                        "source_url": source_url,
                        "artifact_sha256": artifact_sha256,
                        "license_ref": "0xrouter_terms",
                        "independence_group": "arkham_0xrouter",
                        "observed_at": timestamp,
                        "evidence_status": "valid",
                        "imported_by": "0xrouter_fetch",
                    }
                )
                evidence_id = self._insert_evidence(
                    connection,
                    record=record,
                    subject=subject,
                    observation_id=observation_id,
                    verification_result=None,
                )
                if evidence_id:
                    inserted_count += 1
                    evidence_ids.append(evidence_id)
                else:
                    duplicate_count += 1
        return EvidenceImportResult(inserted_count, duplicate_count, tuple(evidence_ids))

    def _verify(self, record: EvidenceInput) -> VerificationResult | None:
        if record.evidence_tier != "A":
            return record.verification_result
        result = self.verifiers.verify(record)
        if result is not VerificationResult.VALID:
            raise EvidenceImportError("Tier A evidence requires a named verifier with a valid result")
        return result

    @staticmethod
    def _ensure_subject(connection, subject, timestamp: str) -> None:
        connection.execute(
            """
            INSERT OR IGNORE INTO address_subject (
                address_id, chain_key, normalized_address, display_address,
                address_type, first_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                subject.address_id,
                subject.chain_key,
                subject.normalized_address,
                subject.display_address,
                subject.address_type,
                timestamp,
            ),
        )

    @staticmethod
    def _insert_evidence(
        connection,
        *,
        record: EvidenceInput,
        subject,
        observation_id: str,
        verification_result: VerificationResult | None,
    ) -> str | None:
        fingerprint = _fingerprint(record, subject.address_id, verification_result)
        evidence_id = str(uuid.uuid4())
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO identity_evidence (
                evidence_id, evidence_fingerprint, address_id, observation_id,
                assertion_type, candidate_entity_id, candidate_entity_name,
                candidate_label, candidate_wallet_role, provider_entity_id,
                provider_tag_id, source_authority, evidence_tier,
                verification_method, verification_result, source_url,
                artifact_sha256, license_ref, independence_group, asserted_at,
                observed_at, effective_from, effective_to, expires_at,
                evidence_status, imported_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                evidence_id,
                fingerprint,
                subject.address_id,
                observation_id,
                record.assertion_type,
                record.candidate_entity_id,
                record.candidate_entity_name,
                record.candidate_label,
                record.candidate_wallet_role,
                record.provider_entity_id,
                record.provider_tag_id,
                record.source_authority,
                record.evidence_tier,
                record.verification_method,
                verification_result.value if verification_result else None,
                record.source_url,
                record.artifact_sha256,
                record.license_ref,
                record.independence_group,
                _optional_utc_string(record.asserted_at),
                _as_utc_string(record.observed_at),
                _optional_utc_string(record.effective_from),
                _optional_utc_string(record.effective_to),
                _optional_utc_string(record.expires_at),
                record.evidence_status,
                record.imported_by,
            ),
        )
        return evidence_id if cursor.rowcount == 1 else None


def _fingerprint(
    record: EvidenceInput, address_id: str, verification_result: VerificationResult | None
) -> str:
    payload = {
        "address_id": address_id,
        "assertion_type": record.assertion_type,
        "candidate_entity_id": record.candidate_entity_id,
        "candidate_entity_name": record.candidate_entity_name,
        "candidate_label": record.candidate_label,
        "candidate_wallet_role": record.candidate_wallet_role,
        "provider_entity_id": record.provider_entity_id,
        "provider_tag_id": record.provider_tag_id,
        "source_authority": record.source_authority,
        "evidence_tier": record.evidence_tier,
        "verification_method": record.verification_method,
        "verification_result": verification_result.value if verification_result else None,
        "source_url": record.source_url,
        "artifact_sha256": record.artifact_sha256,
        "license_ref": record.license_ref,
        "independence_group": record.independence_group,
        "asserted_at": _optional_utc_string(record.asserted_at),
        "observed_at": _as_utc_string(record.observed_at),
        "effective_from": _optional_utc_string(record.effective_from),
        "effective_to": _optional_utc_string(record.effective_to),
        "expires_at": _optional_utc_string(record.expires_at),
        "evidence_status": record.evidence_status,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _as_utc_string(value: datetime) -> str:
    if value.tzinfo is None:
        raise EvidenceImportError("Timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _optional_utc_string(value: datetime | None) -> str | None:
    return _as_utc_string(value) if value else None
