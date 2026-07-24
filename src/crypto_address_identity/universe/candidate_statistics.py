"""Aggregate-only BTC candidate census contracts and cost probe."""

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


MAX_CANDIDATE_STATISTICS_DRY_RUN_BYTES = 650_000_000_000
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CANDIDATE_TRANSACTION_FIELDS = {
    "block_number": ("INTEGER", "REQUIRED"),
    "block_hash": ("STRING", "REQUIRED"),
    "hash": ("STRING", "REQUIRED"),
    "block_timestamp": ("TIMESTAMP", "REQUIRED"),
    "block_timestamp_month": ("DATE", "REQUIRED"),
    "inputs": ("RECORD", "REPEATED"),
    "outputs": ("RECORD", "REPEATED"),
}
_CANDIDATE_IO_FIELDS = {
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


class P0OverlapBucket(UniverseModel):
    mask: int = Field(ge=0, le=15)
    address_count: int = Field(ge=0)


class ScoreHistogramBucket(UniverseModel):
    score: int = Field(ge=0)
    address_count: int = Field(ge=0)


class CandidateStatisticsResult(UniverseModel):
    """One identifier-free aggregate row from the fixed census query."""

    contract_version: Literal["btc_candidate_statistics_v1"]
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
    same_tx_receive_ge_100_btc_count: int = Field(ge=0)
    same_tx_receive_ge_500_btc_count: int = Field(ge=0)
    same_tx_receive_ge_1000_btc_count: int = Field(ge=0)
    same_tx_receive_ge_5000_btc_count: int = Field(ge=0)
    gross_90d_ge_10_btc_count: int = Field(ge=0)
    gross_90d_ge_100_btc_count: int = Field(ge=0)
    gross_90d_ge_1000_btc_count: int = Field(ge=0)
    gross_90d_ge_10000_btc_count: int = Field(ge=0)
    recency_le_30d_count: int = Field(ge=0)
    recency_le_90d_count: int = Field(ge=0)
    recency_le_365d_count: int = Field(ge=0)
    lifetime_ge_10000_active_365d_count: int = Field(ge=0)
    p0_utxo_ge_100_btc_count: int = Field(ge=0)
    p0_same_tx_receive_ge_500_btc_count: int = Field(ge=0)
    p0_gross_90d_ge_1000_btc_count: int = Field(ge=0)
    p0_lifetime_ge_10000_active_365d_count: int = Field(ge=0)
    chain_p0_union_count: int = Field(ge=0)
    p0_overlap_distribution: tuple[P0OverlapBucket, ...]
    chain_p1_count: int = Field(ge=0)
    p0_p1_overlap_count: int = Field(ge=0)
    score_histogram: tuple[ScoreHistogramBucket, ...]
    edge_upgrade_frontier_count: int = Field(ge=0)
    positive_economic_component_count: int = Field(ge=0)
    coarse_candidate_union_count: int = Field(ge=0)
    excluded_source_address_count: int = Field(ge=0)
    current_capital_count: int = Field(ge=0)
    historical_large_receipt_count: int = Field(ge=0)
    high_turnover_count: int = Field(ge=0)
    dormant_holder_count: int = Field(ge=0)

    @field_validator("query_sha256", "schema_sha256")
    @classmethod
    def validate_hash(cls, value: str, info: object) -> str:
        return _validate_sha256(
            value,
            field_name=getattr(info, "field_name", "sha256"),
        )

    @field_validator("max_observed_activity_time", "source_cutoff_time")
    @classmethod
    def validate_timestamp(cls, value: datetime, info: object) -> datetime:
        return _as_utc(
            value,
            field_name=getattr(info, "field_name", "timestamp"),
        )

    @field_validator("p0_overlap_distribution")
    @classmethod
    def validate_p0_buckets(
        cls, value: tuple[P0OverlapBucket, ...]
    ) -> tuple[P0OverlapBucket, ...]:
        masks = [bucket.mask for bucket in value]
        if len(masks) != len(set(masks)):
            raise ValueError("P0 overlap masks must be unique")
        return tuple(sorted(value, key=lambda bucket: bucket.mask))

    @field_validator("score_histogram")
    @classmethod
    def validate_score_buckets(
        cls, value: tuple[ScoreHistogramBucket, ...]
    ) -> tuple[ScoreHistogramBucket, ...]:
        scores = [bucket.score for bucket in value]
        if len(scores) != len(set(scores)):
            raise ValueError("score histogram values must be unique")
        return tuple(sorted(value, key=lambda bucket: bucket.score))


class CandidateStatisticsQualityReport(UniverseModel):
    status: Literal["allow", "warn", "blocked"]
    allow_interpretation: bool
    blocking_reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    @field_validator("blocking_reasons", "warnings")
    @classmethod
    def canonicalize_reasons(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted(set(value)))


class CandidateStatisticsCostEstimate(UniverseModel):
    source_kind: Literal["bigquery"] = "bigquery"
    query_kind: Literal["btc_candidate_statistics"] = "btc_candidate_statistics"
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


def parse_candidate_statistics_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    expected_query_sha256: str,
    expected_schema_sha256: str,
    expected_source_standard_address_count: int,
    expected_cutoff_height: int,
    expected_cutoff_time: datetime,
    now: datetime,
    max_source_age: timedelta,
) -> tuple[CandidateStatisticsResult | None, CandidateStatisticsQualityReport]:
    """Validate one aggregate row without exposing source identifiers."""

    if len(rows) != 1:
        return None, _blocked("candidate_statistics_result_row_count_invalid")
    if (
        expected_source_standard_address_count < 0
        or expected_cutoff_height < 0
        or max_source_age <= timedelta(0)
    ):
        return None, _blocked("candidate_statistics_expected_contract_invalid")
    try:
        result = CandidateStatisticsResult.model_validate(rows[0])
        expected_query_sha256 = _validate_sha256(
            expected_query_sha256, field_name="expected_query_sha256"
        )
        expected_schema_sha256 = _validate_sha256(
            expected_schema_sha256, field_name="expected_schema_sha256"
        )
        expected_cutoff_time = _as_utc(
            expected_cutoff_time, field_name="expected_cutoff_time"
        )
        now = _as_utc(now, field_name="now")
    except (ValidationError, ValueError):
        return None, _blocked("candidate_statistics_result_invalid")

    reasons: list[str] = []
    warnings: list[str] = []
    source_count = result.source_standard_address_count

    if result.query_sha256 != expected_query_sha256:
        reasons.append("candidate_statistics_query_hash_mismatch")
    if result.schema_sha256 != expected_schema_sha256:
        reasons.append("candidate_statistics_schema_hash_mismatch")
    if source_count != expected_source_standard_address_count:
        reasons.append("candidate_statistics_source_baseline_mismatch")
    if (
        result.source_cutoff_height != expected_cutoff_height
        or result.source_cutoff_time != expected_cutoff_time
    ):
        reasons.append("candidate_statistics_source_cutoff_mismatch")
    if result.max_observed_activity_time > result.source_cutoff_time:
        reasons.append("candidate_statistics_activity_after_cutoff")
    source_age = now - result.max_observed_activity_time
    if source_age > max_source_age:
        reasons.append("candidate_statistics_source_activity_stale")
    elif source_age > timedelta(hours=24):
        warnings.append("candidate_statistics_source_activity_older_than_24h")
    if result.source_input_only_address_count:
        reasons.append("candidate_statistics_input_only_addresses_present")
    if result.negative_current_utxo_count:
        reasons.append("candidate_statistics_negative_balance")
    if result.null_value_count:
        reasons.append("candidate_statistics_null_value")
    if result.value_cast_failure_count:
        reasons.append("candidate_statistics_value_cast_failure")
    if result.p0_p1_overlap_count:
        reasons.append("candidate_statistics_p0_p1_overlap")

    p0_counts = {
        1: result.p0_utxo_ge_100_btc_count,
        2: result.p0_same_tx_receive_ge_500_btc_count,
        4: result.p0_gross_90d_ge_1000_btc_count,
        8: result.p0_lifetime_ge_10000_active_365d_count,
    }
    overlap_total = sum(
        bucket.address_count for bucket in result.p0_overlap_distribution
    )
    p0_union_from_masks = sum(
        bucket.address_count
        for bucket in result.p0_overlap_distribution
        if bucket.mask != 0
    )
    bit_counts = {
        bit: sum(
            bucket.address_count
            for bucket in result.p0_overlap_distribution
            if bucket.mask & bit
        )
        for bit in p0_counts
    }
    if (
        overlap_total != source_count
        or p0_union_from_masks != result.chain_p0_union_count
        or bit_counts != p0_counts
    ):
        reasons.append("candidate_statistics_p0_overlap_reconciliation_failed")
    if sum(bucket.address_count for bucket in result.score_histogram) != source_count:
        reasons.append("candidate_statistics_score_histogram_reconciliation_failed")
    if (
        result.coarse_candidate_union_count
        + result.excluded_source_address_count
        != source_count
    ):
        reasons.append("candidate_statistics_candidate_union_reconciliation_failed")

    ladders = (
        (
            result.utxo_ge_1_btc_count,
            result.utxo_ge_10_btc_count,
            result.utxo_ge_100_btc_count,
            result.utxo_ge_1000_btc_count,
        ),
        (
            result.same_tx_receive_ge_100_btc_count,
            result.same_tx_receive_ge_500_btc_count,
            result.same_tx_receive_ge_1000_btc_count,
            result.same_tx_receive_ge_5000_btc_count,
        ),
        (
            result.gross_90d_ge_10_btc_count,
            result.gross_90d_ge_100_btc_count,
            result.gross_90d_ge_1000_btc_count,
            result.gross_90d_ge_10000_btc_count,
        ),
        (
            result.recency_le_365d_count,
            result.recency_le_90d_count,
            result.recency_le_30d_count,
        ),
    )
    if any(
        any(left < right for left, right in zip(ladder, ladder[1:]))
        for ladder in ladders
    ):
        reasons.append("candidate_statistics_threshold_ladder_invalid")

    bounded_counts = (
        *[value for ladder in ladders for value in ladder],
        result.lifetime_ge_10000_active_365d_count,
        *p0_counts.values(),
        result.chain_p0_union_count,
        result.chain_p1_count,
        result.edge_upgrade_frontier_count,
        result.positive_economic_component_count,
        result.coarse_candidate_union_count,
        result.current_capital_count,
        result.historical_large_receipt_count,
        result.high_turnover_count,
        result.dormant_holder_count,
    )
    if any(value > source_count for value in bounded_counts):
        reasons.append("candidate_statistics_count_exceeds_source")
    if result.chain_p0_union_count + result.chain_p1_count > source_count:
        reasons.append("candidate_statistics_policy_union_invalid")
    if (
        result.p0_utxo_ge_100_btc_count != result.utxo_ge_100_btc_count
        or result.p0_same_tx_receive_ge_500_btc_count
        != result.same_tx_receive_ge_500_btc_count
        or result.p0_gross_90d_ge_1000_btc_count
        != result.gross_90d_ge_1000_btc_count
        or result.p0_lifetime_ge_10000_active_365d_count
        != result.lifetime_ge_10000_active_365d_count
        or result.current_capital_count != result.utxo_ge_1_btc_count
        or result.historical_large_receipt_count
        != result.same_tx_receive_ge_100_btc_count
        or result.high_turnover_count != result.gross_90d_ge_10_btc_count
    ):
        reasons.append("candidate_statistics_policy_counter_reconciliation_failed")
    if (
        result.positive_economic_component_count
        > result.coarse_candidate_union_count
    ):
        reasons.append("candidate_statistics_positive_component_union_invalid")

    if result.coarse_candidate_union_count > 5_000_000:
        warnings.append("candidate_statistics_coarse_union_large")
    if result.chain_p0_union_count > 120_000:
        warnings.append("candidate_statistics_p0_union_large")
    if result.edge_upgrade_frontier_count > 1_000_000:
        warnings.append("candidate_statistics_edge_frontier_large")
    if result.coarse_candidate_union_count:
        cohorts = (
            result.current_capital_count,
            result.historical_large_receipt_count,
            result.high_turnover_count,
            result.dormant_holder_count,
        )
        if any(
            cohort * 100 > result.coarse_candidate_union_count * 40
            for cohort in cohorts
        ):
            warnings.append("candidate_statistics_cohort_concentrated")

    unique_reasons = tuple(sorted(set(reasons)))
    unique_warnings = tuple(sorted(set(warnings)))
    if unique_reasons:
        return result, CandidateStatisticsQualityReport(
            status="blocked",
            allow_interpretation=False,
            blocking_reasons=unique_reasons,
            warnings=unique_warnings,
        )
    return result, CandidateStatisticsQualityReport(
        status="warn" if unique_warnings else "allow",
        allow_interpretation=True,
        warnings=unique_warnings,
    )


def _blocked(*reasons: str) -> CandidateStatisticsQualityReport:
    return CandidateStatisticsQualityReport(
        status="blocked",
        allow_interpretation=False,
        blocking_reasons=tuple(reasons),
    )


class BigQueryCandidateStatisticsProbe:
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
    ) -> CandidateStatisticsCostEstimate:
        if cutoff_height < 0:
            raise ValueError("cutoff height must be non-negative")
        cutoff_time = _as_utc(cutoff_time, field_name="cutoff_time")
        if sandbox_budget_bytes <= 0:
            raise ValueError("sandbox budget must be positive")
        if reserve_bytes < 0 or reserve_bytes >= sandbox_budget_bytes:
            raise ValueError("reserve must be non-negative and below budget")
        expected_query_sha256 = _validate_sha256(
            expected_query_sha256, field_name="expected_query_sha256"
        )
        if expected_query_sha256 != self._plan.candidate_statistics_sha256:
            return self._blocked(
                sandbox_budget_bytes=sandbox_budget_bytes,
                reserve_bytes=reserve_bytes,
                reasons=("candidate_statistics_query_hash_mismatch",),
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
                reasons=("bigquery_metadata_unavailable",),
                network_requests=1,
            )

        schema_sha256 = transactions.schema_sha256
        reasons = _transaction_metadata_reasons(
            transactions=transactions,
            now=self._now,
            max_source_age=self._max_source_age,
            required_fields=_CANDIDATE_TRANSACTION_FIELDS,
            required_nested_fields={
                "inputs": _CANDIDATE_IO_FIELDS,
                "outputs": _CANDIDATE_IO_FIELDS,
            },
        )
        if reasons:
            return self._blocked(
                sandbox_budget_bytes=sandbox_budget_bytes,
                reserve_bytes=reserve_bytes,
                reasons=reasons,
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
                reasons=("bigquery_monthly_usage_unavailable",),
                schema_sha256=schema_sha256,
                network_requests=2,
            )

        try:
            estimate = self._backend.dry_run(
                self._plan.candidate_statistics_sql,
                {
                    "cutoff_height": cutoff_height,
                    "cutoff_time": cutoff_time,
                    "query_sha256": self._plan.candidate_statistics_sha256,
                    "schema_sha256": schema_sha256,
                },
                0,
            )
        except Exception:
            return self._blocked(
                sandbox_budget_bytes=sandbox_budget_bytes,
                reserve_bytes=reserve_bytes,
                reasons=("bigquery_candidate_statistics_dry_run_failed",),
                schema_sha256=schema_sha256,
                usage=usage,
                network_requests=3,
            )

        projected = usage.total_bytes_billed + estimate.total_bytes_processed
        projected_reserve = sandbox_budget_bytes - projected
        blocking: list[str] = []
        if (
            estimate.total_bytes_processed
            > MAX_CANDIDATE_STATISTICS_DRY_RUN_BYTES
        ):
            blocking.append("candidate_statistics_dry_run_limit_exceeded")
        if projected > sandbox_budget_bytes - reserve_bytes:
            blocking.append("candidate_statistics_monthly_budget_exceeded")
        if projected_reserve < reserve_bytes:
            blocking.append("candidate_statistics_monthly_reserve_insufficient")
        warnings = (
            ("bigquery_transactions_older_than_24h",)
            if self._now - transactions.modified_at > timedelta(hours=24)
            else ()
        )
        return CandidateStatisticsCostEstimate(
            status="blocked" if blocking else "within_budget",
            network_requests=3,
            schema_sha256=schema_sha256,
            query_sha256=self._plan.candidate_statistics_sha256,
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
    ) -> CandidateStatisticsCostEstimate:
        successful_jobs = getattr(usage, "successful_query_jobs", None)
        billed_bytes = getattr(usage, "total_bytes_billed", None)
        return CandidateStatisticsCostEstimate(
            status="blocked",
            network_requests=network_requests,
            schema_sha256=schema_sha256,
            query_sha256=self._plan.candidate_statistics_sha256,
            sandbox_budget_bytes=sandbox_budget_bytes,
            reserve_bytes=reserve_bytes,
            successful_query_jobs=successful_jobs,
            month_to_date_billed_bytes=billed_bytes,
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
