"""Stable, source-neutral models for BTC address-universe Phase 1."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_validator,
    model_validator,
)

from crypto_address_identity.chains.bitcoin import normalize_bitcoin_address


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CAMPAIGN_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{2,127}$")


def _require_sha256(value: str, *, field_name: str) -> str:
    if not _SHA256_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be a lower-case SHA-256 digest")
    return value


def _as_utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


class UniverseModel(BaseModel):
    """Strict base class for immutable universe contracts."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class SourceProbeResult(UniverseModel):
    source_kind: Literal["bigquery", "bitcoin_core"]
    status: Literal["accepted", "blocked", "partial"]
    read_only: bool = True
    schema_sha256: str | None = None
    latest_height: int | None = Field(default=None, ge=0)
    latest_hash: str | None = None
    latest_time: datetime | None = None
    finalized_height: int | None = Field(default=None, ge=0)
    finalized_hash: str | None = None
    dry_run_bytes: int | None = Field(default=None, ge=0)
    script_completeness: bool
    capabilities: tuple[str, ...]
    blocking_reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    @field_validator("schema_sha256", "latest_hash", "finalized_hash")
    @classmethod
    def validate_optional_hash(
        cls, value: str | None, info: object
    ) -> str | None:
        if value is None:
            return None
        field_name = getattr(info, "field_name", "hash")
        return _require_sha256(value, field_name=field_name)

    @field_validator("latest_time")
    @classmethod
    def validate_latest_time(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _as_utc(value, field_name="latest_time")

    @field_validator("capabilities", "blocking_reasons", "warnings")
    @classmethod
    def canonicalize_string_tuple(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not item.strip() for item in value):
            raise ValueError("tuple entries must be non-empty")
        return tuple(sorted(set(value)))

    @model_validator(mode="after")
    def validate_read_only(self) -> "SourceProbeResult":
        if not self.read_only:
            raise ValueError("Phase 1 source probes must be read-only")
        return self


class SourceManifest(UniverseModel):
    campaign_id: str
    source_kind: Literal["bigquery", "bitcoin_core", "fixture"]
    source_revision: str
    cutoff_height: int = Field(ge=0)
    cutoff_hash: str
    cutoff_time: datetime
    schema_sha256: str
    query_sha256: str | None
    source_capabilities: tuple[str, ...]
    script_completeness: bool

    @field_validator("campaign_id")
    @classmethod
    def validate_campaign_id(cls, value: str) -> str:
        if not _CAMPAIGN_ID_RE.fullmatch(value):
            raise ValueError("campaign_id contains unsafe characters")
        return value

    @field_validator("cutoff_hash", "schema_sha256")
    @classmethod
    def validate_required_hash(cls, value: str, info: object) -> str:
        return _require_sha256(
            value, field_name=getattr(info, "field_name", "hash")
        )

    @field_validator("query_sha256")
    @classmethod
    def validate_query_hash(cls, value: str | None) -> str | None:
        return (
            None
            if value is None
            else _require_sha256(value, field_name="query_sha256")
        )

    @field_validator("cutoff_time")
    @classmethod
    def validate_cutoff_time(cls, value: datetime) -> datetime:
        return _as_utc(value, field_name="cutoff_time")

    @field_validator("source_capabilities")
    @classmethod
    def validate_capabilities(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not item.strip() for item in value):
            raise ValueError("source_capabilities must contain non-empty values")
        return tuple(value)

    @computed_field
    @property
    def manifest_sha256(self) -> str:
        payload = self.model_dump(mode="json", exclude={"manifest_sha256"})
        payload["source_capabilities"] = sorted(payload["source_capabilities"])
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("ascii")
        return hashlib.sha256(encoded).hexdigest()


class ScriptSubjectRow(UniverseModel):
    script_id: str
    script_hex: str
    script_type: str
    normalized_address: str | None
    address_id: str | None
    provider_enrichable: bool

    @field_validator("script_id", "address_id")
    @classmethod
    def validate_hash(cls, value: str | None, info: object) -> str | None:
        if value is None:
            return None
        return _require_sha256(
            value, field_name=getattr(info, "field_name", "hash")
        )

    @field_validator("script_hex")
    @classmethod
    def validate_script_hex(cls, value: str) -> str:
        if value != value.lower() or len(value) % 2:
            raise ValueError("script_hex must be lower-case, even-length hexadecimal")
        try:
            bytes.fromhex(value)
        except ValueError as exc:
            raise ValueError("script_hex must be valid hexadecimal") from exc
        return value

    @model_validator(mode="after")
    def validate_subject_mapping(self) -> "ScriptSubjectRow":
        expected_script_id = hashlib.sha256(
            b"bitcoin:mainnet\x00" + bytes.fromhex(self.script_hex)
        ).hexdigest()
        if self.script_id != expected_script_id:
            raise ValueError("script_id does not match canonical script bytes")

        has_address = self.normalized_address is not None
        if has_address != (self.address_id is not None):
            raise ValueError("normalized_address and address_id must appear together")
        if has_address:
            subject = normalize_bitcoin_address(self.normalized_address or "")
            if subject.normalized_address != self.normalized_address:
                raise ValueError("normalized_address is not canonical")
            if subject.address_id != self.address_id:
                raise ValueError("address_id does not match normalized_address")
            if not self.provider_enrichable:
                raise ValueError("single canonical addresses must be provider-enrichable")
        elif self.provider_enrichable:
            raise ValueError("scripts without one address are not provider-enrichable")
        return self


class AddressFeatureRow(UniverseModel):
    feature_version: Literal["btc_address_features_v1"] = "btc_address_features_v1"
    address_id: str
    normalized_address: str
    address_type: str
    first_seen_height: int = Field(ge=0)
    last_seen_height: int = Field(ge=0)
    first_seen_time: datetime
    last_seen_time: datetime
    output_count: int = Field(ge=0)
    spent_output_count: int = Field(ge=0)
    transaction_count: int = Field(ge=0)
    current_utxo_sats: int = Field(ge=0)
    lifetime_received_sats: int = Field(ge=0)
    lifetime_spent_sats: int = Field(ge=0)
    max_single_output_sats: int = Field(ge=0)
    max_same_tx_received_sats: int = Field(ge=0)
    inflow_30d_sats: int = Field(ge=0)
    outflow_30d_sats: int = Field(ge=0)
    gross_flow_30d_sats: int = Field(ge=0)
    inflow_90d_sats: int = Field(ge=0)
    outflow_90d_sats: int = Field(ge=0)
    gross_flow_90d_sats: int = Field(ge=0)
    gross_flow_365d_sats: int = Field(ge=0)
    direct_large_counterparty_count: int = Field(ge=0)

    @field_validator("address_id")
    @classmethod
    def validate_address_id(cls, value: str) -> str:
        return _require_sha256(value, field_name="address_id")

    @field_validator("first_seen_time", "last_seen_time")
    @classmethod
    def validate_timestamp(cls, value: datetime, info: object) -> datetime:
        return _as_utc(value, field_name=getattr(info, "field_name", "timestamp"))

    @model_validator(mode="after")
    def validate_feature_invariants(self) -> "AddressFeatureRow":
        subject = normalize_bitcoin_address(self.normalized_address)
        if subject.normalized_address != self.normalized_address:
            raise ValueError("normalized_address is not canonical")
        if subject.address_id != self.address_id:
            raise ValueError("address_id does not match normalized_address")
        if subject.address_type != self.address_type:
            raise ValueError("address_type does not match normalized_address")
        if self.first_seen_height > self.last_seen_height:
            raise ValueError("first_seen_height must not exceed last_seen_height")
        if self.first_seen_time > self.last_seen_time:
            raise ValueError("first_seen_time must not exceed last_seen_time")
        if self.spent_output_count > self.output_count:
            raise ValueError("spent_output_count must not exceed output_count")
        if self.current_utxo_sats != (
            self.lifetime_received_sats - self.lifetime_spent_sats
        ):
            raise ValueError("current_utxo_sats must reconcile lifetime totals")
        if self.gross_flow_30d_sats != (
            self.inflow_30d_sats + self.outflow_30d_sats
        ):
            raise ValueError("gross_flow_30d_sats must reconcile inflow and outflow")
        if self.gross_flow_90d_sats != (
            self.inflow_90d_sats + self.outflow_90d_sats
        ):
            raise ValueError("gross_flow_90d_sats must reconcile inflow and outflow")
        return self


class UniverseCoverageCounters(UniverseModel):
    total_output_rows: int = Field(default=0, ge=0)
    total_input_rows: int = Field(default=0, ge=0)
    distinct_script_subjects: int = Field(default=0, ge=0)
    standard_single_address_rows: int = Field(default=0, ge=0)
    empty_address_rows: int = Field(default=0, ge=0)
    multi_address_rows: int = Field(default=0, ge=0)
    nonstandard_rows: int = Field(default=0, ge=0)
    unmatched_input_rows: int = Field(default=0, ge=0)


class CandidateReasonCount(UniverseModel):
    reason_code: str
    count: int = Field(ge=0)


class CampaignManifest(UniverseModel):
    campaign_id: str
    source_manifest: SourceManifest
    created_at: datetime
    output_fact_materialized: Literal[False] = False
    address_feature_rows: int = Field(default=0, ge=0)
    script_subject_rows: int = Field(default=0, ge=0)
    calibration_anchor_rows: int = Field(default=0, ge=0)
    source_accounting_rows: int = Field(default=0, ge=0)
    artifact_sha256: dict[str, str] = Field(default_factory=dict)

    @field_validator("campaign_id")
    @classmethod
    def validate_campaign_id(cls, value: str) -> str:
        if not _CAMPAIGN_ID_RE.fullmatch(value):
            raise ValueError("campaign_id contains unsafe characters")
        return value

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: datetime) -> datetime:
        return _as_utc(value, field_name="created_at")

    @field_validator("artifact_sha256")
    @classmethod
    def validate_artifact_hashes(cls, value: dict[str, str]) -> dict[str, str]:
        return {
            path: _require_sha256(digest, field_name=f"artifact_sha256[{path}]")
            for path, digest in sorted(value.items())
        }

    @model_validator(mode="after")
    def validate_source_campaign(self) -> "CampaignManifest":
        if self.source_manifest.campaign_id != self.campaign_id:
            raise ValueError("source manifest campaign_id mismatch")
        return self

    @computed_field
    @property
    def manifest_sha256(self) -> str:
        payload = self.model_dump(mode="json", exclude={"manifest_sha256"})
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("ascii")
        return hashlib.sha256(encoded).hexdigest()


class CandidateStatistics(UniverseModel):
    status: Literal["dry_run", "blocked"]
    campaign_id: str
    source_coverage: dict[str, int | str | bool | None]
    quality_status: str
    script_completeness: bool
    output_fact_materialized: Literal[False] = False
    unique_script_subjects: int = Field(ge=0)
    unique_standard_addresses: int = Field(ge=0)
    source_accounting_counts: dict[str, int]
    calibration_anchor_count: int = Field(ge=0)
    anchor_only_count: int = Field(ge=0)
    p0_unique_addresses: int = Field(ge=0)
    p1_unique_addresses: int = Field(ge=0)
    control_unique_addresses: int = Field(ge=0)
    reason_memberships: int = Field(ge=0)
    reason_counts: tuple[CandidateReasonCount, ...]
    cohort_counts: dict[str, int]
    cohort_overlap_counts: dict[str, int]
    duplicate_slots_prevented: int = Field(ge=0)
    rate_limited_capacity: int = Field(ge=0)
    point_limited_capacity: int | None = Field(default=None, ge=0)
    first_wave_unique_addresses: int = Field(ge=0)
    remaining_p0_addresses: int = Field(ge=0)
    projected_minimum_minutes: int = Field(ge=0)
    provider_requests: Literal[0] = 0
    provider_points: Literal[0] = 0
    written_paths: tuple[str, ...] = ()
    blocking_reasons: tuple[str, ...] = ()
