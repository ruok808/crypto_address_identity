"""Aggregate-only BTC importance v2 result and cost contracts."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Literal

from pydantic import Field, ValidationError, field_validator

from crypto_address_identity.universe.bigquery import (
    BigQueryBackend,
    MonthlyQueryUsage,
    TableMetadata,
    _transaction_metadata_reasons,
)
from crypto_address_identity.universe.models import UniverseModel
from crypto_address_identity.universe.query_plan import BigQueryQueryPlan


MAX_CANDIDATE_STATISTICS_V2_DRY_RUN_BYTES = 650_000_000_000
PINNED_V2_SOURCE_STANDARD_ADDRESS_COUNT = 1_557_941_780
PINNED_V2_SOURCE_INPUT_ONLY_ADDRESS_COUNT = 3
PINNED_V2_CUTOFF_HEIGHT = 959_187
STRICT_P0_REVIEW_THRESHOLD = 120_000
STRICT_COARSE_REVIEW_THRESHOLD = 2_000_000
INCREMENTAL_RECEIPT_REVIEW_THRESHOLD = 95_906
P0_HARD_STOP = 1_000_000
COARSE_HARD_STOP = 5_000_000
EDGE_HARD_STOP = 1_000_000

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CANDIDATE_V2_TRANSACTION_FIELDS = {
    "block_number": ("INTEGER", "REQUIRED"),
    "block_hash": ("STRING", "REQUIRED"),
    "hash": ("STRING", "REQUIRED"),
    "block_timestamp": ("TIMESTAMP", "REQUIRED"),
    "block_timestamp_month": ("DATE", "REQUIRED"),
    "inputs": ("RECORD", "REPEATED"),
    "outputs": ("RECORD", "REPEATED"),
}
_CANDIDATE_V2_IO_FIELDS = {
    "addresses": ("STRING", "REPEATED"),
    "value": ("NUMERIC", "NULLABLE"),
}


def _validate_sha256(value: str, *, field_name: str) -> str:
    if not _SHA256_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be a lower-case SHA-256 digest")
    return value


def _as_utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


class CandidateStatisticsV2OverlapBucket(UniverseModel):
    mask: int = Field(ge=0, le=15)
    address_count: int = Field(ge=0)


class CandidateStatisticsV2SupportBucket(UniverseModel):
    mask: int = Field(ge=0, le=7)
    address_count: int = Field(ge=0)


class CandidateStatisticsV2ScoreBucket(UniverseModel):
    score: int = Field(ge=0)
    address_count: int = Field(ge=0)


class CandidateStatisticsV2Variant(UniverseModel):
    variant: Literal["V2-S", "V2-B", "V2-R"]
    p0_utxo_ge_100_btc_count: int = Field(ge=0)
    p0_sustained_residual_gross_90d_ge_1000_btc_count: int = Field(ge=0)
    p0_receipt_rule_count: int = Field(ge=0)
    p0_lifetime_ge_10000_active_supported_90d_count: int = Field(ge=0)
    chain_p0_union_count: int = Field(ge=0)
    incremental_receipt_p0_count: int = Field(ge=0)
    p0_overlap_distribution: tuple[
        CandidateStatisticsV2OverlapBucket, ...
    ]
    chain_p1_count: int = Field(ge=0)
    p0_p1_overlap_count: int = Field(ge=0)
    edge_upgrade_frontier_count: int = Field(ge=0)
    coarse_candidate_union_count: int = Field(ge=0)
    excluded_source_address_count: int = Field(ge=0)

    @field_validator("p0_overlap_distribution")
    @classmethod
    def validate_unique_masks(
        cls,
        value: tuple[CandidateStatisticsV2OverlapBucket, ...],
    ) -> tuple[CandidateStatisticsV2OverlapBucket, ...]:
        if len({bucket.mask for bucket in value}) != len(value):
            raise ValueError("P0 overlap masks must be unique")
        return tuple(sorted(value, key=lambda bucket: bucket.mask))


class CandidateStatisticsV2Result(UniverseModel):
    """One identifier-free aggregate result row."""

    contract_version: Literal["btc_candidate_statistics_v2"]
    policy_version: Literal["btc_importance_v2"]
    source_standard_address_count: int = Field(ge=0)
    source_input_only_address_count: int = Field(ge=0)
    negative_current_utxo_count: int = Field(ge=0)
    null_value_count: int = Field(ge=0)
    value_cast_failure_count: int = Field(ge=0)
    max_observed_activity_time: datetime
    source_cutoff_height: int = Field(ge=0)
    source_cutoff_time: datetime
    query_sha256: str
    schema_sha256: str
    utxo_ge_1_btc_count: int = Field(ge=0)
    utxo_ge_10_btc_count: int = Field(ge=0)
    utxo_ge_100_btc_count: int = Field(ge=0)
    utxo_ge_1000_btc_count: int = Field(ge=0)
    raw_gross_90d_ge_10_btc_count: int = Field(ge=0)
    raw_gross_90d_ge_100_btc_count: int = Field(ge=0)
    raw_gross_90d_ge_1000_btc_count: int = Field(ge=0)
    raw_gross_90d_ge_10000_btc_count: int = Field(ge=0)
    residual_gross_90d_ge_10_btc_count: int = Field(ge=0)
    residual_gross_90d_ge_100_btc_count: int = Field(ge=0)
    residual_gross_90d_ge_1000_btc_count: int = Field(ge=0)
    residual_gross_90d_ge_10000_btc_count: int = Field(ge=0)
    recency_le_30d_count: int = Field(ge=0)
    recency_le_90d_count: int = Field(ge=0)
    recency_le_365d_count: int = Field(ge=0)
    lifetime_max_receipt_ge_500_btc_count: int = Field(ge=0)
    lifetime_max_receipt_ge_1000_btc_count: int = Field(ge=0)
    lifetime_max_receipt_ge_5000_btc_count: int = Field(ge=0)
    max_receipt_365d_ge_500_btc_count: int = Field(ge=0)
    max_receipt_365d_ge_1000_btc_count: int = Field(ge=0)
    max_receipt_365d_ge_5000_btc_count: int = Field(ge=0)
    max_receipt_90d_ge_500_btc_count: int = Field(ge=0)
    max_receipt_90d_ge_1000_btc_count: int = Field(ge=0)
    max_receipt_90d_ge_5000_btc_count: int = Field(ge=0)
    receipt_count_lifetime_ge_1_count: int = Field(ge=0)
    receipt_count_lifetime_ge_2_count: int = Field(ge=0)
    receipt_count_lifetime_ge_3_count: int = Field(ge=0)
    receipt_count_365d_ge_1_count: int = Field(ge=0)
    receipt_count_365d_ge_2_count: int = Field(ge=0)
    receipt_count_365d_ge_3_count: int = Field(ge=0)
    receipt_count_90d_ge_1_count: int = Field(ge=0)
    receipt_count_90d_ge_2_count: int = Field(ge=0)
    receipt_count_90d_ge_3_count: int = Field(ge=0)
    recent_receipt_retained_count: int = Field(ge=0)
    recent_receipt_repeated_count: int = Field(ge=0)
    recent_receipt_sustained_activity_count: int = Field(ge=0)
    strict_supported_receipt_count: int = Field(ge=0)
    unsupported_recent_singleton_count: int = Field(ge=0)
    stale_lifetime_singleton_count: int = Field(ge=0)
    receipt_support_overlap_distribution: tuple[
        CandidateStatisticsV2SupportBucket, ...
    ]
    score_histogram: tuple[CandidateStatisticsV2ScoreBucket, ...]
    strict_variant: CandidateStatisticsV2Variant
    balanced_variant: CandidateStatisticsV2Variant
    retention_variant: CandidateStatisticsV2Variant

    @field_validator("max_observed_activity_time", "source_cutoff_time")
    @classmethod
    def validate_utc_time(cls, value: datetime, info: object) -> datetime:
        return _as_utc(
            value,
            field_name=getattr(info, "field_name", "datetime"),
        )

    @field_validator("query_sha256", "schema_sha256")
    @classmethod
    def validate_hash(cls, value: str, info: object) -> str:
        return _validate_sha256(
            value,
            field_name=getattr(info, "field_name", "sha256"),
        )

    @field_validator("receipt_support_overlap_distribution")
    @classmethod
    def validate_unique_support_masks(
        cls,
        value: tuple[CandidateStatisticsV2SupportBucket, ...],
    ) -> tuple[CandidateStatisticsV2SupportBucket, ...]:
        if len({bucket.mask for bucket in value}) != len(value):
            raise ValueError("receipt support masks must be unique")
        return tuple(sorted(value, key=lambda bucket: bucket.mask))

    @field_validator("score_histogram")
    @classmethod
    def validate_unique_scores(
        cls,
        value: tuple[CandidateStatisticsV2ScoreBucket, ...],
    ) -> tuple[CandidateStatisticsV2ScoreBucket, ...]:
        if len({bucket.score for bucket in value}) != len(value):
            raise ValueError("score histogram values must be unique")
        return tuple(sorted(value, key=lambda bucket: bucket.score))


class CandidateStatisticsV2QualityReport(UniverseModel):
    status: Literal["allow", "warn", "blocked"]
    allow_interpretation: bool
    blocking_reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    @field_validator("blocking_reasons", "warnings")
    @classmethod
    def canonicalize_reasons(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted(set(value)))


class CandidateStatisticsV2CostEstimate(UniverseModel):
    source_kind: Literal["bigquery"] = "bigquery"
    query_kind: Literal["btc_candidate_statistics_v2"] = (
        "btc_candidate_statistics_v2"
    )
    policy_version: Literal["btc_importance_v2"] = "btc_importance_v2"
    status: Literal["within_budget", "blocked"]
    read_only: Literal[True] = True
    network_requests: int = Field(ge=0, le=3)
    schema_sha256: str | None = None
    query_sha256: str
    dry_run_bytes: int | None = Field(default=None, ge=0)
    sandbox_budget_bytes: int = Field(gt=0)
    reserve_bytes: int = Field(ge=0)
    successful_query_jobs: int | None = Field(default=None, ge=0)
    month_to_date_billed_bytes: int | None = Field(default=None, ge=0)
    projected_month_to_date_bytes: int | None = Field(default=None, ge=0)
    projected_reserve_bytes: int | None = None
    within_budget: bool | None = None
    blocking_reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    @field_validator("schema_sha256", "query_sha256")
    @classmethod
    def validate_hash(cls, value: str | None, info: object) -> str | None:
        if value is None:
            return None
        return _validate_sha256(
            value,
            field_name=getattr(info, "field_name", "sha256"),
        )

    @field_validator("blocking_reasons", "warnings")
    @classmethod
    def canonicalize_reasons(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted(set(value)))


def parse_candidate_statistics_v2_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    expected_query_sha256: str,
    expected_schema_sha256: str,
    expected_source_standard_address_count: int,
    expected_source_input_only_address_count: int,
    expected_cutoff_height: int,
    expected_cutoff_time: datetime,
    now: datetime,
    max_source_age: timedelta,
) -> tuple[
    CandidateStatisticsV2Result | None,
    CandidateStatisticsV2QualityReport,
]:
    """Validate one aggregate-only v2 row without exposing identifiers."""

    if len(rows) != 1:
        return None, _blocked(
            "candidate_statistics_v2_result_row_count_invalid"
        )
    if (
        expected_source_standard_address_count < 0
        or expected_source_input_only_address_count < 0
        or expected_cutoff_height < 0
        or max_source_age <= timedelta(0)
    ):
        return None, _blocked(
            "candidate_statistics_v2_expected_contract_invalid"
        )
    try:
        result = CandidateStatisticsV2Result.model_validate(rows[0])
        expected_query_sha256 = _validate_sha256(
            expected_query_sha256,
            field_name="expected_query_sha256",
        )
        expected_schema_sha256 = _validate_sha256(
            expected_schema_sha256,
            field_name="expected_schema_sha256",
        )
        expected_cutoff_time = _as_utc(
            expected_cutoff_time,
            field_name="expected_cutoff_time",
        )
        now = _as_utc(now, field_name="now")
    except (ValidationError, ValueError):
        return None, _blocked("candidate_statistics_v2_result_invalid")

    reasons: list[str] = []
    warnings: list[str] = []
    source_count = result.source_standard_address_count

    if result.query_sha256 != expected_query_sha256:
        reasons.append("candidate_statistics_v2_query_hash_mismatch")
    if result.schema_sha256 != expected_schema_sha256:
        reasons.append("candidate_statistics_v2_schema_hash_mismatch")
    if source_count != expected_source_standard_address_count:
        reasons.append("candidate_statistics_v2_source_baseline_mismatch")
    if (
        result.source_input_only_address_count
        != expected_source_input_only_address_count
    ):
        reasons.append(
            "candidate_statistics_v2_input_only_baseline_mismatch"
        )
    elif result.source_input_only_address_count:
        warnings.append(
            "candidate_statistics_v2_known_input_only_subjects_excluded"
        )
    if (
        result.source_cutoff_height != expected_cutoff_height
        or result.source_cutoff_time != expected_cutoff_time
    ):
        reasons.append("candidate_statistics_v2_source_cutoff_mismatch")
    if result.max_observed_activity_time > result.source_cutoff_time:
        reasons.append("candidate_statistics_v2_activity_after_cutoff")
    source_age = now - result.max_observed_activity_time
    if source_age > max_source_age:
        reasons.append("candidate_statistics_v2_source_activity_stale")
    elif source_age > timedelta(hours=24):
        warnings.append(
            "candidate_statistics_v2_source_activity_older_than_24h"
        )
    if result.negative_current_utxo_count:
        reasons.append("candidate_statistics_v2_negative_balance")
    if result.null_value_count:
        reasons.append("candidate_statistics_v2_null_value")
    if result.value_cast_failure_count:
        reasons.append("candidate_statistics_v2_value_cast_failure")

    if not _threshold_ladders_are_valid(result):
        reasons.append("candidate_statistics_v2_threshold_ladder_invalid")
    raw_gross_counts = (
        result.raw_gross_90d_ge_10_btc_count,
        result.raw_gross_90d_ge_100_btc_count,
        result.raw_gross_90d_ge_1000_btc_count,
        result.raw_gross_90d_ge_10000_btc_count,
    )
    residual_gross_counts = (
        result.residual_gross_90d_ge_10_btc_count,
        result.residual_gross_90d_ge_100_btc_count,
        result.residual_gross_90d_ge_1000_btc_count,
        result.residual_gross_90d_ge_10000_btc_count,
    )
    if any(
        residual > raw
        for raw, residual in zip(raw_gross_counts, residual_gross_counts)
    ):
        reasons.append(
            "candidate_statistics_v2_economic_counter_reconciliation_failed"
        )
    if not _receipt_funnel_is_valid(result):
        reasons.append("candidate_statistics_v2_receipt_funnel_invalid")
    if not _receipt_support_reconciles(result):
        reasons.append(
            "candidate_statistics_v2_receipt_support_reconciliation_failed"
        )
    if sum(
        bucket.address_count for bucket in result.score_histogram
    ) != source_count:
        reasons.append(
            "candidate_statistics_v2_score_histogram_reconciliation_failed"
        )

    variants = (
        result.strict_variant,
        result.balanced_variant,
        result.retention_variant,
    )
    if (
        result.strict_variant.variant != "V2-S"
        or result.balanced_variant.variant != "V2-B"
        or result.retention_variant.variant != "V2-R"
    ):
        reasons.append("candidate_statistics_v2_variant_name_mismatch")
    if any(
        not _variant_reconciles(variant, source_count=source_count)
        for variant in variants
    ):
        reasons.append(
            "candidate_statistics_v2_variant_reconciliation_failed"
        )
    if any(variant.p0_p1_overlap_count for variant in variants):
        reasons.append("candidate_statistics_v2_p0_p1_overlap")

    base_reason_tuples = {
        (
            variant.p0_utxo_ge_100_btc_count,
            variant.p0_sustained_residual_gross_90d_ge_1000_btc_count,
            variant.p0_lifetime_ge_10000_active_supported_90d_count,
        )
        for variant in variants
    }
    if len(base_reason_tuples) != 1:
        reasons.append(
            "candidate_statistics_v2_variant_base_reason_mismatch"
        )
    if any(
        variant.p0_utxo_ge_100_btc_count
        != result.utxo_ge_100_btc_count
        for variant in variants
    ):
        reasons.append(
            "candidate_statistics_v2_policy_counter_reconciliation_failed"
        )
    if (
        result.strict_variant.p0_receipt_rule_count
        != result.strict_supported_receipt_count
    ):
        reasons.append(
            "candidate_statistics_v2_strict_receipt_counter_mismatch"
        )
    if (
        result.balanced_variant.p0_receipt_rule_count
        < result.strict_variant.p0_receipt_rule_count
        or result.balanced_variant.chain_p0_union_count
        < result.strict_variant.chain_p0_union_count
        or result.balanced_variant.coarse_candidate_union_count
        < result.strict_variant.coarse_candidate_union_count
    ):
        reasons.append("candidate_statistics_v2_balanced_not_superset")
    if (
        result.retention_variant.p0_receipt_rule_count
        > result.lifetime_max_receipt_ge_500_btc_count
    ):
        reasons.append(
            "candidate_statistics_v2_retention_receipt_counter_invalid"
        )

    bounded_counts = _top_level_bounded_counts(result)
    bounded_counts.extend(
        value
        for variant in variants
        for value in (
            variant.p0_utxo_ge_100_btc_count,
            variant.p0_sustained_residual_gross_90d_ge_1000_btc_count,
            variant.p0_receipt_rule_count,
            variant.p0_lifetime_ge_10000_active_supported_90d_count,
            variant.chain_p0_union_count,
            variant.incremental_receipt_p0_count,
            variant.chain_p1_count,
            variant.edge_upgrade_frontier_count,
            variant.coarse_candidate_union_count,
            variant.excluded_source_address_count,
        )
    )
    if any(value > source_count for value in bounded_counts):
        reasons.append("candidate_statistics_v2_count_exceeds_source")

    if result.strict_variant.chain_p0_union_count > P0_HARD_STOP:
        reasons.append("candidate_statistics_v2_strict_p0_too_large")
    if any(
        variant.coarse_candidate_union_count > COARSE_HARD_STOP
        for variant in variants
    ):
        reasons.append("candidate_statistics_v2_coarse_union_too_large")
    if any(
        variant.edge_upgrade_frontier_count > EDGE_HARD_STOP
        for variant in variants
    ):
        reasons.append("candidate_statistics_v2_edge_frontier_too_large")

    strict = result.strict_variant
    if strict.chain_p0_union_count > STRICT_P0_REVIEW_THRESHOLD:
        warnings.append("candidate_statistics_v2_strict_p0_large")
    if strict.coarse_candidate_union_count > STRICT_COARSE_REVIEW_THRESHOLD:
        warnings.append(
            "candidate_statistics_v2_strict_coarse_union_large"
        )
    if (
        strict.incremental_receipt_p0_count
        > INCREMENTAL_RECEIPT_REVIEW_THRESHOLD
    ):
        warnings.append(
            "candidate_statistics_v2_incremental_receipt_budget_exceeded"
        )
    if strict.chain_p0_union_count:
        reason_counts = (
            strict.p0_utxo_ge_100_btc_count,
            strict.p0_sustained_residual_gross_90d_ge_1000_btc_count,
            strict.p0_receipt_rule_count,
            strict.p0_lifetime_ge_10000_active_supported_90d_count,
        )
        if strict.p0_receipt_rule_count * 100 > strict.chain_p0_union_count * 60:
            warnings.append(
                "candidate_statistics_v2_supported_receipt_concentrated"
            )
        if any(
            count * 100 > strict.chain_p0_union_count * 80
            for count in reason_counts
        ):
            warnings.append("candidate_statistics_v2_p0_reason_concentrated")
    if (
        result.balanced_variant.chain_p0_union_count
        > max(result.strict_variant.chain_p0_union_count * 2, 0)
    ):
        warnings.append(
            "candidate_statistics_v2_balanced_materially_larger"
        )

    unique_reasons = tuple(sorted(set(reasons)))
    unique_warnings = tuple(sorted(set(warnings)))
    if unique_reasons:
        return result, CandidateStatisticsV2QualityReport(
            status="blocked",
            allow_interpretation=False,
            blocking_reasons=unique_reasons,
            warnings=unique_warnings,
        )
    return result, CandidateStatisticsV2QualityReport(
        status="warn" if unique_warnings else "allow",
        allow_interpretation=True,
        warnings=unique_warnings,
    )


def _threshold_ladders_are_valid(
    result: CandidateStatisticsV2Result,
) -> bool:
    ladders = (
        (
            result.utxo_ge_1_btc_count,
            result.utxo_ge_10_btc_count,
            result.utxo_ge_100_btc_count,
            result.utxo_ge_1000_btc_count,
        ),
        (
            result.raw_gross_90d_ge_10_btc_count,
            result.raw_gross_90d_ge_100_btc_count,
            result.raw_gross_90d_ge_1000_btc_count,
            result.raw_gross_90d_ge_10000_btc_count,
        ),
        (
            result.residual_gross_90d_ge_10_btc_count,
            result.residual_gross_90d_ge_100_btc_count,
            result.residual_gross_90d_ge_1000_btc_count,
            result.residual_gross_90d_ge_10000_btc_count,
        ),
        (
            result.recency_le_365d_count,
            result.recency_le_90d_count,
            result.recency_le_30d_count,
        ),
        (
            result.lifetime_max_receipt_ge_500_btc_count,
            result.lifetime_max_receipt_ge_1000_btc_count,
            result.lifetime_max_receipt_ge_5000_btc_count,
        ),
        (
            result.max_receipt_365d_ge_500_btc_count,
            result.max_receipt_365d_ge_1000_btc_count,
            result.max_receipt_365d_ge_5000_btc_count,
        ),
        (
            result.max_receipt_90d_ge_500_btc_count,
            result.max_receipt_90d_ge_1000_btc_count,
            result.max_receipt_90d_ge_5000_btc_count,
        ),
    )
    return all(
        all(left >= right for left, right in zip(ladder, ladder[1:]))
        for ladder in ladders
    )


def _receipt_funnel_is_valid(
    result: CandidateStatisticsV2Result,
) -> bool:
    max_windows = (
        (
            result.lifetime_max_receipt_ge_500_btc_count,
            result.max_receipt_365d_ge_500_btc_count,
            result.max_receipt_90d_ge_500_btc_count,
        ),
        (
            result.lifetime_max_receipt_ge_1000_btc_count,
            result.max_receipt_365d_ge_1000_btc_count,
            result.max_receipt_90d_ge_1000_btc_count,
        ),
        (
            result.lifetime_max_receipt_ge_5000_btc_count,
            result.max_receipt_365d_ge_5000_btc_count,
            result.max_receipt_90d_ge_5000_btc_count,
        ),
    )
    count_ladders = (
        (
            result.receipt_count_lifetime_ge_1_count,
            result.receipt_count_lifetime_ge_2_count,
            result.receipt_count_lifetime_ge_3_count,
        ),
        (
            result.receipt_count_365d_ge_1_count,
            result.receipt_count_365d_ge_2_count,
            result.receipt_count_365d_ge_3_count,
        ),
        (
            result.receipt_count_90d_ge_1_count,
            result.receipt_count_90d_ge_2_count,
            result.receipt_count_90d_ge_3_count,
        ),
    )
    count_windows = tuple(
        (
            count_ladders[0][index],
            count_ladders[1][index],
            count_ladders[2][index],
        )
        for index in range(3)
    )
    exact_first_counts = (
        result.receipt_count_lifetime_ge_1_count
        == result.lifetime_max_receipt_ge_500_btc_count
        and result.receipt_count_365d_ge_1_count
        == result.max_receipt_365d_ge_500_btc_count
        and result.receipt_count_90d_ge_1_count
        == result.max_receipt_90d_ge_500_btc_count
    )
    support_bounds = (
        result.recent_receipt_retained_count
        <= result.receipt_count_90d_ge_1_count
        and result.recent_receipt_repeated_count
        == result.receipt_count_90d_ge_2_count
        and result.recent_receipt_sustained_activity_count
        <= result.receipt_count_90d_ge_1_count
        and result.strict_supported_receipt_count
        + result.unsupported_recent_singleton_count
        == result.receipt_count_90d_ge_1_count
        and result.stale_lifetime_singleton_count
        <= result.receipt_count_lifetime_ge_1_count
    )
    return (
        all(
            all(left >= right for left, right in zip(ladder, ladder[1:]))
            for ladder in (*max_windows, *count_ladders, *count_windows)
        )
        and exact_first_counts
        and support_bounds
    )


def _receipt_support_reconciles(
    result: CandidateStatisticsV2Result,
) -> bool:
    buckets = result.receipt_support_overlap_distribution
    if sum(bucket.address_count for bucket in buckets) != (
        result.source_standard_address_count
    ):
        return False
    expected_bits = {
        1: result.recent_receipt_retained_count,
        2: result.recent_receipt_repeated_count,
        4: result.recent_receipt_sustained_activity_count,
    }
    actual_bits = {
        bit: sum(
            bucket.address_count
            for bucket in buckets
            if bucket.mask & bit
        )
        for bit in expected_bits
    }
    union = sum(
        bucket.address_count for bucket in buckets if bucket.mask != 0
    )
    return (
        actual_bits == expected_bits
        and union == result.strict_supported_receipt_count
    )


def _variant_reconciles(
    variant: CandidateStatisticsV2Variant,
    *,
    source_count: int,
) -> bool:
    buckets = variant.p0_overlap_distribution
    if sum(bucket.address_count for bucket in buckets) != source_count:
        return False
    reason_counts = {
        1: variant.p0_utxo_ge_100_btc_count,
        2: variant.p0_sustained_residual_gross_90d_ge_1000_btc_count,
        4: variant.p0_receipt_rule_count,
        8: variant.p0_lifetime_ge_10000_active_supported_90d_count,
    }
    actual_bits = {
        bit: sum(
            bucket.address_count
            for bucket in buckets
            if bucket.mask & bit
        )
        for bit in reason_counts
    }
    union = sum(
        bucket.address_count for bucket in buckets if bucket.mask != 0
    )
    incremental_receipt = sum(
        bucket.address_count for bucket in buckets if bucket.mask == 4
    )
    return (
        actual_bits == reason_counts
        and union == variant.chain_p0_union_count
        and incremental_receipt == variant.incremental_receipt_p0_count
        and variant.coarse_candidate_union_count
        + variant.excluded_source_address_count
        == source_count
        and variant.chain_p0_union_count + variant.chain_p1_count
        <= source_count
    )


def _top_level_bounded_counts(
    result: CandidateStatisticsV2Result,
) -> list[int]:
    return [
        result.utxo_ge_1_btc_count,
        result.utxo_ge_10_btc_count,
        result.utxo_ge_100_btc_count,
        result.utxo_ge_1000_btc_count,
        result.raw_gross_90d_ge_10_btc_count,
        result.raw_gross_90d_ge_100_btc_count,
        result.raw_gross_90d_ge_1000_btc_count,
        result.raw_gross_90d_ge_10000_btc_count,
        result.residual_gross_90d_ge_10_btc_count,
        result.residual_gross_90d_ge_100_btc_count,
        result.residual_gross_90d_ge_1000_btc_count,
        result.residual_gross_90d_ge_10000_btc_count,
        result.recency_le_30d_count,
        result.recency_le_90d_count,
        result.recency_le_365d_count,
        result.lifetime_max_receipt_ge_500_btc_count,
        result.lifetime_max_receipt_ge_1000_btc_count,
        result.lifetime_max_receipt_ge_5000_btc_count,
        result.max_receipt_365d_ge_500_btc_count,
        result.max_receipt_365d_ge_1000_btc_count,
        result.max_receipt_365d_ge_5000_btc_count,
        result.max_receipt_90d_ge_500_btc_count,
        result.max_receipt_90d_ge_1000_btc_count,
        result.max_receipt_90d_ge_5000_btc_count,
        result.receipt_count_lifetime_ge_1_count,
        result.receipt_count_lifetime_ge_2_count,
        result.receipt_count_lifetime_ge_3_count,
        result.receipt_count_365d_ge_1_count,
        result.receipt_count_365d_ge_2_count,
        result.receipt_count_365d_ge_3_count,
        result.receipt_count_90d_ge_1_count,
        result.receipt_count_90d_ge_2_count,
        result.receipt_count_90d_ge_3_count,
        result.recent_receipt_retained_count,
        result.recent_receipt_repeated_count,
        result.recent_receipt_sustained_activity_count,
        result.strict_supported_receipt_count,
        result.unsupported_recent_singleton_count,
        result.stale_lifetime_singleton_count,
    ]


def _blocked(*reasons: str) -> CandidateStatisticsV2QualityReport:
    return CandidateStatisticsV2QualityReport(
        status="blocked",
        allow_interpretation=False,
        blocking_reasons=tuple(reasons),
    )


class BigQueryCandidateStatisticsV2Probe:
    """Metadata, monthly usage, and BigQuery dry run only."""

    def __init__(
        self,
        *,
        backend: BigQueryBackend,
        dataset: str,
        max_source_age: timedelta,
        now: datetime | None = None,
    ) -> None:
        self._backend = backend
        self._plan = BigQueryQueryPlan.load(dataset)
        self._max_source_age = max_source_age
        self._now = _as_utc(now or datetime.now(UTC), field_name="now")

    def run(
        self,
        *,
        cutoff_height: int,
        cutoff_time: datetime,
        expected_query_sha256: str,
        sandbox_budget_bytes: int,
        reserve_bytes: int,
    ) -> CandidateStatisticsV2CostEstimate:
        if cutoff_height < 0:
            raise ValueError("cutoff height must be non-negative")
        cutoff_time = _as_utc(cutoff_time, field_name="cutoff_time")
        if sandbox_budget_bytes <= 0:
            raise ValueError("sandbox budget must be positive")
        if reserve_bytes < 0 or reserve_bytes >= sandbox_budget_bytes:
            raise ValueError("reserve must be non-negative and below budget")
        expected_query_sha256 = _validate_sha256(
            expected_query_sha256,
            field_name="expected_query_sha256",
        )
        if expected_query_sha256 != (
            self._plan.candidate_statistics_v2_sha256
        ):
            return self._blocked(
                sandbox_budget_bytes=sandbox_budget_bytes,
                reserve_bytes=reserve_bytes,
                reasons=("candidate_statistics_v2_query_hash_mismatch",),
                network_requests=0,
            )

        try:
            transactions = self._backend.table_metadata(
                self._plan.transactions_table_id
            )
        except Exception:
            return self._blocked(
                sandbox_budget_bytes=sandbox_budget_bytes,
                reserve_bytes=reserve_bytes,
                reasons=("bigquery_v2_metadata_unavailable",),
                network_requests=1,
            )

        schema_sha256 = transactions.schema_sha256
        metadata_reasons = _transaction_metadata_reasons(
            transactions=transactions,
            now=self._now,
            max_source_age=self._max_source_age,
            required_fields=_CANDIDATE_V2_TRANSACTION_FIELDS,
            required_nested_fields={
                "inputs": _CANDIDATE_V2_IO_FIELDS,
                "outputs": _CANDIDATE_V2_IO_FIELDS,
            },
        )
        if metadata_reasons:
            return self._blocked(
                sandbox_budget_bytes=sandbox_budget_bytes,
                reserve_bytes=reserve_bytes,
                reasons=metadata_reasons,
                schema_sha256=schema_sha256,
                network_requests=1,
            )

        month_start, month_end = self._month_bounds(self._now)
        try:
            usage = self._backend.monthly_successful_query_usage(
                month_start=month_start,
                month_end=month_end,
            )
        except Exception:
            return self._blocked(
                sandbox_budget_bytes=sandbox_budget_bytes,
                reserve_bytes=reserve_bytes,
                reasons=("bigquery_v2_monthly_usage_unavailable",),
                schema_sha256=schema_sha256,
                network_requests=2,
            )

        try:
            estimate = self._backend.dry_run(
                self._plan.candidate_statistics_v2_sql,
                {
                    "cutoff_height": cutoff_height,
                    "cutoff_time": cutoff_time,
                    "query_sha256": (
                        self._plan.candidate_statistics_v2_sha256
                    ),
                    "schema_sha256": schema_sha256,
                },
                0,
            )
        except Exception:
            return self._blocked(
                sandbox_budget_bytes=sandbox_budget_bytes,
                reserve_bytes=reserve_bytes,
                reasons=("bigquery_candidate_statistics_v2_dry_run_failed",),
                schema_sha256=schema_sha256,
                usage=usage,
                network_requests=3,
            )

        projected = usage.total_bytes_billed + estimate.total_bytes_processed
        projected_reserve = sandbox_budget_bytes - projected
        blocking: list[str] = []
        if (
            estimate.total_bytes_processed
            > MAX_CANDIDATE_STATISTICS_V2_DRY_RUN_BYTES
        ):
            blocking.append(
                "candidate_statistics_v2_dry_run_limit_exceeded"
            )
        if projected > sandbox_budget_bytes - reserve_bytes:
            blocking.append(
                "candidate_statistics_v2_monthly_budget_exceeded"
            )
        if projected_reserve < reserve_bytes:
            blocking.append(
                "candidate_statistics_v2_monthly_reserve_insufficient"
            )
        warnings = (
            ("bigquery_transactions_older_than_24h",)
            if self._now - transactions.modified_at > timedelta(hours=24)
            else ()
        )
        return CandidateStatisticsV2CostEstimate(
            status="blocked" if blocking else "within_budget",
            network_requests=3,
            schema_sha256=schema_sha256,
            query_sha256=self._plan.candidate_statistics_v2_sha256,
            dry_run_bytes=estimate.total_bytes_processed,
            sandbox_budget_bytes=sandbox_budget_bytes,
            reserve_bytes=reserve_bytes,
            successful_query_jobs=usage.successful_query_jobs,
            month_to_date_billed_bytes=usage.total_bytes_billed,
            projected_month_to_date_bytes=projected,
            projected_reserve_bytes=projected_reserve,
            within_budget=not blocking,
            blocking_reasons=tuple(blocking),
            warnings=warnings,
        )

    def _blocked(
        self,
        *,
        sandbox_budget_bytes: int,
        reserve_bytes: int,
        reasons: tuple[str, ...],
        network_requests: int,
        schema_sha256: str | None = None,
        usage: MonthlyQueryUsage | None = None,
    ) -> CandidateStatisticsV2CostEstimate:
        return CandidateStatisticsV2CostEstimate(
            status="blocked",
            network_requests=network_requests,
            schema_sha256=schema_sha256,
            query_sha256=self._plan.candidate_statistics_v2_sha256,
            sandbox_budget_bytes=sandbox_budget_bytes,
            reserve_bytes=reserve_bytes,
            successful_query_jobs=getattr(
                usage, "successful_query_jobs", None
            ),
            month_to_date_billed_bytes=getattr(
                usage, "total_bytes_billed", None
            ),
            within_budget=None,
            blocking_reasons=reasons,
        )

    @staticmethod
    def _month_bounds(value: datetime) -> tuple[datetime, datetime]:
        start = datetime(value.year, value.month, 1, tzinfo=UTC)
        if value.month == 12:
            end = datetime(value.year + 1, 1, 1, tzinfo=UTC)
        else:
            end = datetime(value.year, value.month + 1, 1, tzinfo=UTC)
        return start, end
