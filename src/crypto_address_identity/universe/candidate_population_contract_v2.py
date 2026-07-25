"""Dual-population admission for immutable BTC importance v1/v2 receipts."""

from __future__ import annotations

import hashlib
import json
import stat
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator, model_validator

from crypto_address_identity.universe.models import UniverseModel


OUTPUT_DEFINED_RECEIPT_FILENAME = (
    "btc-candidate-statistics-20260724-v1.json"
)
POSITIVE_VALUE_RECEIPT_FILENAME = (
    "btc-importance-v2-20260724-quota-recovery-one-shot.json"
)
PINNED_OUTPUT_DEFINED_RECEIPT_SHA256 = (
    "7a657f69f08c8ceb8756ed9e2e37d82b0bd007e843e2cd22a241bc8b9c7cf77b"
)
PINNED_POSITIVE_VALUE_RECEIPT_SHA256 = (
    "c3123159ba77e0bcd5ba4735483027899bc451a50c8648784ec4317dfe20a236"
)
PINNED_CUTOFF_HEIGHT = 959_187
PINNED_CUTOFF_TIME = "2026-07-24T23:59:59.999999Z"
PINNED_SCHEMA_SHA256 = (
    "7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7"
)
PINNED_V1_QUERY_SHA256 = (
    "5dbb2c914448837ac43b20e4943abb33130cf2ce9c1b7c2a72eb5ce4d285012c"
)
PINNED_V2_QUERY_SHA256 = (
    "47b0b8977cc1443578bc3daf3f90a2cf5e0e48ae758a4b7a133d3caa7d301e74"
)
PINNED_PROCESSED_BYTES = 637_999_682_243
PINNED_BILLED_BYTES = 637_999_775_744
PINNED_INPUT_ONLY_COUNT = 3
PINNED_ADDRESS_SCALE_REFERENCE_COUNT = 1_557_951_354
PINNED_MAXIMUM_BYTES_BILLED = 650_000_000_000
STRICT_P0_LIMIT = 120_000
STRICT_COARSE_LIMIT = 5_000_000
STRICT_EDGE_LIMIT = 1_000_000

_V1_EXPECTED_BLOCKERS = (
    "candidate_statistics_input_only_addresses_present",
    "candidate_statistics_source_baseline_mismatch",
)
_V2_EXPECTED_BLOCKERS = (
    "candidate_statistics_v2_source_baseline_mismatch",
)


class StrictV2SCapacity(UniverseModel):
    status: Literal["eligible_for_design", "capacity_blocked"]
    chain_p0_union_count: int = Field(ge=0)
    chain_p1_count: int = Field(ge=0)
    edge_upgrade_frontier_count: int = Field(ge=0)
    coarse_candidate_union_count: int = Field(ge=0)
    excluded_source_address_count: int = Field(ge=0)
    p0_limit: Literal[120_000] = STRICT_P0_LIMIT
    coarse_limit: Literal[5_000_000] = STRICT_COARSE_LIMIT
    edge_limit: Literal[1_000_000] = STRICT_EDGE_LIMIT
    p0_within_limit: bool
    coarse_within_limit: bool
    edge_within_limit: bool

    @model_validator(mode="after")
    def validate_status(self) -> "StrictV2SCapacity":
        within_limits = (
            self.p0_within_limit
            and self.coarse_within_limit
            and self.edge_within_limit
        )
        if (self.status == "eligible_for_design") != within_limits:
            raise ValueError("strict capacity status is inconsistent")
        return self


class CandidatePopulationContractV2Outcome(UniverseModel):
    contract_version: Literal[
        "btc_importance_v2_dual_population_contract_v1"
    ] = "btc_importance_v2_dual_population_contract_v1"
    status: Literal["dry_run", "accepted", "blocked"]
    policy_version: Literal["btc_importance_v2"] = "btc_importance_v2"
    output_defined_receipt_sha256: str
    positive_value_receipt_sha256: str
    receipt_reads: int = Field(ge=0, le=2)
    cutoff_height: int | None = Field(default=None, ge=0)
    cutoff_time: str | None = None
    output_defined_standard_address_count: int | None = Field(
        default=None,
        ge=0,
    )
    positive_value_standard_address_count: int | None = Field(
        default=None,
        ge=0,
    )
    zero_value_only_standard_address_count: int | None = Field(
        default=None,
        ge=0,
    )
    population_relation: Literal[
        "positive_value_subset_of_output_defined"
    ] | None = None
    policy_denominator: Literal["positive_value"] | None = None
    strict_capacity: StrictV2SCapacity | None = None
    allow_population_interpretation: bool = False
    allow_materialization_design: bool = False
    candidate_materialization_allowed: Literal[False] = False
    blocking_reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    network_requests: Literal[0] = 0
    provider_requests: Literal[0] = 0
    provider_points: Literal[0] = 0
    written_paths: tuple[()] = ()

    @field_validator("output_defined_receipt_sha256", "positive_value_receipt_sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        if len(value) != 64 or any(
            character not in "0123456789abcdef" for character in value
        ):
            raise ValueError("receipt checksum must be lower-case SHA-256")
        return value

    @field_validator("blocking_reasons", "warnings")
    @classmethod
    def canonicalize_reasons(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted(set(value)))

    @model_validator(mode="after")
    def validate_outcome(self) -> "CandidatePopulationContractV2Outcome":
        if self.status != "accepted":
            if (
                self.allow_population_interpretation
                or self.allow_materialization_design
            ):
                raise ValueError("non-accepted contract cannot enable use")
            return self
        required = (
            self.cutoff_height,
            self.cutoff_time,
            self.output_defined_standard_address_count,
            self.positive_value_standard_address_count,
            self.zero_value_only_standard_address_count,
            self.population_relation,
            self.policy_denominator,
            self.strict_capacity,
        )
        if (
            any(value is None for value in required)
            or not self.allow_population_interpretation
            or self.blocking_reasons
        ):
            raise ValueError("accepted population contract is incomplete")
        if self.strict_capacity is None:
            raise ValueError("accepted population contract has no capacity")
        assert self.output_defined_standard_address_count is not None
        assert self.positive_value_standard_address_count is not None
        assert self.zero_value_only_standard_address_count is not None
        if (
            self.positive_value_standard_address_count
            > self.output_defined_standard_address_count
            or self.zero_value_only_standard_address_count
            != (
                self.output_defined_standard_address_count
                - self.positive_value_standard_address_count
            )
        ):
            raise ValueError("accepted population relation is inconsistent")
        if self.allow_materialization_design != (
            self.strict_capacity.status == "eligible_for_design"
        ):
            raise ValueError("materialization design gate is inconsistent")
        return self


def preview_candidate_population_contract_v2(
    *,
    receipt_root: Path,
) -> CandidatePopulationContractV2Outcome:
    del receipt_root
    return _outcome(status="dry_run")


def validate_candidate_population_contract_v2(
    *,
    receipt_root: Path,
) -> CandidatePopulationContractV2Outcome:
    output_payload, output_reads, output_reason = _read_receipt(
        receipt_root / OUTPUT_DEFINED_RECEIPT_FILENAME,
        expected_sha256=PINNED_OUTPUT_DEFINED_RECEIPT_SHA256,
        label="output_defined",
    )
    if output_reason is not None:
        return _outcome(
            status="blocked",
            receipt_reads=output_reads,
            blocking_reasons=(output_reason,),
        )
    positive_payload, positive_reads, positive_reason = _read_receipt(
        receipt_root / POSITIVE_VALUE_RECEIPT_FILENAME,
        expected_sha256=PINNED_POSITIVE_VALUE_RECEIPT_SHA256,
        label="positive_value",
    )
    receipt_reads = output_reads + positive_reads
    if positive_reason is not None:
        return _outcome(
            status="blocked",
            receipt_reads=receipt_reads,
            blocking_reasons=(positive_reason,),
        )
    assert output_payload is not None
    assert positive_payload is not None

    output_statistics, output_reasons = _validate_output_defined_receipt(
        output_payload
    )
    positive_statistics, positive_reasons = _validate_positive_value_receipt(
        positive_payload
    )
    if output_reasons or positive_reasons:
        return _outcome(
            status="blocked",
            receipt_reads=receipt_reads,
            blocking_reasons=output_reasons + positive_reasons,
        )
    assert output_statistics is not None
    assert positive_statistics is not None

    output_count = _strict_int(
        output_statistics,
        "source_standard_address_count",
    )
    positive_count = _strict_int(
        positive_statistics,
        "source_standard_address_count",
    )
    if positive_count > output_count:
        return _outcome(
            status="blocked",
            receipt_reads=receipt_reads,
            blocking_reasons=(
                "positive_value_population_exceeds_output_defined_population",
            ),
        )
    if not _same_typed_value(
        positive_payload.get("expected_source_standard_address_count"),
        output_count,
    ):
        return _outcome(
            status="blocked",
            receipt_reads=receipt_reads,
            blocking_reasons=(
                "positive_value_expected_population_not_output_defined",
            ),
        )

    strict = _strict_dict(positive_statistics, "strict_variant")
    capacity_reasons = _validate_strict_partition(
        strict,
        positive_count=positive_count,
    )
    if capacity_reasons:
        return _outcome(
            status="blocked",
            receipt_reads=receipt_reads,
            blocking_reasons=capacity_reasons,
        )
    capacity = _capacity(strict)
    warnings = [
        "source_receipts_remain_immutable_quality_blocked_evidence",
        "zero_value_only_addresses_excluded_from_economic_policy",
        "candidate_materialization_requires_separate_authorization",
    ]
    if capacity.status == "capacity_blocked":
        warnings.append("strict_v2_s_capacity_limits_exceeded")
    return _outcome(
        status="accepted",
        receipt_reads=receipt_reads,
        cutoff_height=PINNED_CUTOFF_HEIGHT,
        cutoff_time=PINNED_CUTOFF_TIME,
        output_defined_count=output_count,
        positive_value_count=positive_count,
        zero_value_only_count=output_count - positive_count,
        strict_capacity=capacity,
        allow_population_interpretation=True,
        allow_materialization_design=(
            capacity.status == "eligible_for_design"
        ),
        warnings=tuple(warnings),
    )


def _read_receipt(
    path: Path,
    *,
    expected_sha256: str,
    label: str,
) -> tuple[dict[str, object] | None, int, str | None]:
    try:
        metadata = path.lstat()
    except OSError:
        return None, 0, f"{label}_receipt_missing"
    if not stat.S_ISREG(metadata.st_mode):
        return None, 0, f"{label}_receipt_not_regular"
    if stat.S_IMODE(metadata.st_mode) != 0o600:
        return None, 0, f"{label}_receipt_mode_invalid"
    try:
        encoded = path.read_bytes()
    except OSError:
        return None, 0, f"{label}_receipt_unreadable"
    if not encoded or len(encoded) > 1_000_000:
        return None, 1, f"{label}_receipt_size_invalid"
    if hashlib.sha256(encoded).hexdigest() != expected_sha256:
        return None, 1, f"{label}_receipt_checksum_mismatch"
    try:
        payload = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, 1, f"{label}_receipt_json_invalid"
    if not isinstance(payload, dict):
        return None, 1, f"{label}_receipt_json_invalid"
    return payload, 1, None


def _validate_output_defined_receipt(
    payload: dict[str, object],
) -> tuple[dict[str, object] | None, tuple[str, ...]]:
    top_level = {
        "schema_version": "btc_candidate_statistics_execution_receipt_v1",
        "status": "quality_blocked",
        "authorization_id": "btc-candidate-statistics-20260724-v1",
        "automatic_retries": 0,
        "candidate_materialized": False,
        "cutoff_height": PINNED_CUTOFF_HEIGHT,
        "cutoff_time": PINNED_CUTOFF_TIME,
        "execution_calls": 1,
        "expected_schema_sha256": PINNED_SCHEMA_SHA256,
        "expected_source_standard_address_count": (
            PINNED_ADDRESS_SCALE_REFERENCE_COUNT
        ),
        "maximum_bytes_billed": PINNED_MAXIMUM_BYTES_BILLED,
        "network_requests": 4,
        "provider_points": 0,
        "provider_requests": 0,
        "query_sha256": PINNED_V1_QUERY_SHA256,
        "receipt_created": True,
        "row_count": 1,
        "total_bytes_processed": PINNED_PROCESSED_BYTES,
        "total_bytes_billed": PINNED_BILLED_BYTES,
    }
    if not _contract_fields_match(payload, top_level):
        return None, ("output_defined_receipt_contract_invalid",)
    if _reason_tuple(payload.get("blocking_reasons")) != _V1_EXPECTED_BLOCKERS:
        return None, ("output_defined_receipt_contract_invalid",)
    if not _quality_matches(payload.get("quality"), _V1_EXPECTED_BLOCKERS):
        return None, ("output_defined_receipt_contract_invalid",)
    try:
        statistics = _strict_dict(payload, "statistics")
        expected_statistics = {
            "contract_version": "btc_candidate_statistics_v1",
            "source_input_only_address_count": PINNED_INPUT_ONLY_COUNT,
            "negative_current_utxo_count": 0,
            "null_value_count": 0,
            "value_cast_failure_count": 0,
            "source_cutoff_height": PINNED_CUTOFF_HEIGHT,
            "source_cutoff_time": PINNED_CUTOFF_TIME,
            "query_sha256": PINNED_V1_QUERY_SHA256,
            "schema_sha256": PINNED_SCHEMA_SHA256,
        }
        if not _contract_fields_match(statistics, expected_statistics):
            raise ValueError
        if _strict_int(statistics, "source_standard_address_count") <= 0:
            raise ValueError
    except (TypeError, ValueError):
        return None, ("output_defined_receipt_contract_invalid",)
    return statistics, ()


def _validate_positive_value_receipt(
    payload: dict[str, object],
) -> tuple[dict[str, object] | None, tuple[str, ...]]:
    top_level = {
        "schema_version": (
            "btc_candidate_statistics_v2_execution_receipt_v1"
        ),
        "status": "quality_blocked",
        "authorization_id": (
            "btc-importance-v2-20260724-quota-recovery-one-shot"
        ),
        "automatic_retries": 0,
        "billing_acknowledged": True,
        "candidate_materialized": False,
        "cutoff_height": PINNED_CUTOFF_HEIGHT,
        "cutoff_time": PINNED_CUTOFF_TIME,
        "execution_calls": 1,
        "expected_dry_run_bytes": PINNED_PROCESSED_BYTES,
        "expected_source_input_only_address_count": (
            PINNED_INPUT_ONLY_COUNT
        ),
        "expected_schema_sha256": PINNED_SCHEMA_SHA256,
        "job_id": (
            "cai_btc_importance_v2_"
            "b2bf4b71772b68d2d2e7f7ec2303745a48d69d74"
        ),
        "maximum_bytes_billed": PINNED_MAXIMUM_BYTES_BILLED,
        "network_requests": 4,
        "provider_points": 0,
        "provider_requests": 0,
        "query_sha256": PINNED_V2_QUERY_SHA256,
        "receipt_created": True,
        "recovery_evidence_validated": True,
        "row_count": 1,
        "total_bytes_processed": PINNED_PROCESSED_BYTES,
        "total_bytes_billed": PINNED_BILLED_BYTES,
    }
    if not _contract_fields_match(payload, top_level):
        return None, ("positive_value_receipt_contract_invalid",)
    if _reason_tuple(payload.get("blocking_reasons")) != _V2_EXPECTED_BLOCKERS:
        return None, ("positive_value_receipt_contract_invalid",)
    if not _quality_matches(payload.get("quality"), _V2_EXPECTED_BLOCKERS):
        return None, ("positive_value_receipt_contract_invalid",)
    try:
        statistics = _strict_dict(payload, "statistics")
        expected_statistics = {
            "contract_version": "btc_candidate_statistics_v2",
            "policy_version": "btc_importance_v2",
            "source_input_only_address_count": PINNED_INPUT_ONLY_COUNT,
            "negative_current_utxo_count": 0,
            "null_value_count": 0,
            "value_cast_failure_count": 0,
            "source_cutoff_height": PINNED_CUTOFF_HEIGHT,
            "source_cutoff_time": PINNED_CUTOFF_TIME,
            "query_sha256": PINNED_V2_QUERY_SHA256,
            "schema_sha256": PINNED_SCHEMA_SHA256,
        }
        if not _contract_fields_match(statistics, expected_statistics):
            raise ValueError
        if _strict_int(statistics, "source_standard_address_count") <= 0:
            raise ValueError
    except (TypeError, ValueError):
        return None, ("positive_value_receipt_contract_invalid",)
    return statistics, ()


def _validate_strict_partition(
    strict: dict[str, object],
    *,
    positive_count: int,
) -> tuple[str, ...]:
    try:
        if strict.get("variant") != "V2-S":
            raise ValueError
        p0 = _strict_int(strict, "chain_p0_union_count")
        p1 = _strict_int(strict, "chain_p1_count")
        edge = _strict_int(strict, "edge_upgrade_frontier_count")
        coarse = _strict_int(strict, "coarse_candidate_union_count")
        excluded = _strict_int(strict, "excluded_source_address_count")
        overlap = _strict_int(strict, "p0_p1_overlap_count")
        if (
            min(p0, p1, edge, coarse, excluded, overlap) < 0
            or overlap != 0
            or p0 + p1 + edge > coarse
            or coarse + excluded != positive_count
        ):
            raise ValueError
    except (TypeError, ValueError):
        return ("strict_v2_s_partition_invalid",)
    return ()


def _capacity(strict: dict[str, object]) -> StrictV2SCapacity:
    p0 = _strict_int(strict, "chain_p0_union_count")
    coarse = _strict_int(strict, "coarse_candidate_union_count")
    edge = _strict_int(strict, "edge_upgrade_frontier_count")
    p0_within = p0 <= STRICT_P0_LIMIT
    coarse_within = coarse <= STRICT_COARSE_LIMIT
    edge_within = edge <= STRICT_EDGE_LIMIT
    return StrictV2SCapacity(
        status=(
            "eligible_for_design"
            if p0_within and coarse_within and edge_within
            else "capacity_blocked"
        ),
        chain_p0_union_count=p0,
        chain_p1_count=_strict_int(strict, "chain_p1_count"),
        edge_upgrade_frontier_count=edge,
        coarse_candidate_union_count=coarse,
        excluded_source_address_count=_strict_int(
            strict,
            "excluded_source_address_count",
        ),
        p0_within_limit=p0_within,
        coarse_within_limit=coarse_within,
        edge_within_limit=edge_within,
    )


def _quality_matches(value: object, expected_reasons: tuple[str, ...]) -> bool:
    if not isinstance(value, dict):
        return False
    return (
        value.get("status") == "blocked"
        and value.get("allow_interpretation") is False
        and _reason_tuple(value.get("blocking_reasons")) == expected_reasons
    )


def _reason_tuple(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(
        isinstance(item, str) for item in value
    ):
        return ()
    return tuple(sorted(value))


def _contract_fields_match(
    payload: dict[str, object],
    expected: dict[str, object],
) -> bool:
    return all(
        _same_typed_value(payload.get(field), value)
        for field, value in expected.items()
    )


def _same_typed_value(actual: object, expected: object) -> bool:
    if isinstance(expected, bool):
        return isinstance(actual, bool) and actual is expected
    if isinstance(expected, int):
        return (
            isinstance(actual, int)
            and not isinstance(actual, bool)
            and actual == expected
        )
    return type(actual) is type(expected) and actual == expected


def _strict_dict(value: dict[str, object], field: str) -> dict[str, object]:
    result = value.get(field)
    if not isinstance(result, dict):
        raise TypeError(f"{field} must be an object")
    return result


def _strict_int(value: dict[str, object], field: str) -> int:
    result = value.get(field)
    if not isinstance(result, int) or isinstance(result, bool):
        raise TypeError(f"{field} must be an integer")
    return result


def _outcome(
    *,
    status: Literal["dry_run", "accepted", "blocked"],
    receipt_reads: int = 0,
    cutoff_height: int | None = None,
    cutoff_time: str | None = None,
    output_defined_count: int | None = None,
    positive_value_count: int | None = None,
    zero_value_only_count: int | None = None,
    strict_capacity: StrictV2SCapacity | None = None,
    allow_population_interpretation: bool = False,
    allow_materialization_design: bool = False,
    blocking_reasons: tuple[str, ...] = (),
    warnings: tuple[str, ...] = (),
) -> CandidatePopulationContractV2Outcome:
    accepted = status == "accepted"
    return CandidatePopulationContractV2Outcome(
        status=status,
        output_defined_receipt_sha256=(
            PINNED_OUTPUT_DEFINED_RECEIPT_SHA256
        ),
        positive_value_receipt_sha256=(
            PINNED_POSITIVE_VALUE_RECEIPT_SHA256
        ),
        receipt_reads=receipt_reads,
        cutoff_height=cutoff_height,
        cutoff_time=cutoff_time,
        output_defined_standard_address_count=output_defined_count,
        positive_value_standard_address_count=positive_value_count,
        zero_value_only_standard_address_count=zero_value_only_count,
        population_relation=(
            "positive_value_subset_of_output_defined" if accepted else None
        ),
        policy_denominator="positive_value" if accepted else None,
        strict_capacity=strict_capacity,
        allow_population_interpretation=allow_population_interpretation,
        allow_materialization_design=allow_materialization_design,
        blocking_reasons=blocking_reasons,
        warnings=warnings,
    )
