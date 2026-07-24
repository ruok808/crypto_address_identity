from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import Any

from crypto_address_identity.universe.bigquery import (
    MonthlyQueryUsage,
    QueryEstimate,
)
from crypto_address_identity.universe.candidate_statistics_v2 import (
    BigQueryCandidateStatisticsV2Probe,
    CandidateStatisticsV2Result,
    parse_candidate_statistics_v2_rows,
)
from crypto_address_identity.universe.query_plan import BigQueryQueryPlan
from tests.universe.test_bigquery_probe import table_metadata


CUTOFF_TIME = datetime(2026, 7, 24, 23, 59, 59, tzinfo=UTC)
QUERY_SHA256 = "33" * 32
SCHEMA_SHA256 = "44" * 32


def _variant(
    name: str,
    *,
    union: int,
    receipt_reason: int,
    incremental_receipt: int,
    overlap: list[dict[str, int]],
    p1: int,
    edge: int,
    coarse: int,
) -> dict[str, Any]:
    return {
        "variant": name,
        "p0_utxo_ge_100_btc_count": 5,
        "p0_sustained_residual_gross_90d_ge_1000_btc_count": 5,
        "p0_receipt_rule_count": receipt_reason,
        "p0_lifetime_ge_10000_active_supported_90d_count": 3,
        "chain_p0_union_count": union,
        "incremental_receipt_p0_count": incremental_receipt,
        "p0_overlap_distribution": overlap,
        "chain_p1_count": p1,
        "p0_p1_overlap_count": 0,
        "edge_upgrade_frontier_count": edge,
        "coarse_candidate_union_count": coarse,
        "excluded_source_address_count": 100 - coarse,
    }


def valid_result_row() -> dict[str, Any]:
    strict_overlap = [
        {"mask": 0, "address_count": 90},
        {"mask": 1, "address_count": 2},
        {"mask": 2, "address_count": 1},
        {"mask": 4, "address_count": 1},
        {"mask": 8, "address_count": 1},
        {"mask": 3, "address_count": 1},
        {"mask": 5, "address_count": 1},
        {"mask": 6, "address_count": 1},
        {"mask": 10, "address_count": 1},
        {"mask": 15, "address_count": 1},
    ]
    balanced_overlap = [
        {"mask": 0, "address_count": 88},
        {"mask": 1, "address_count": 2},
        {"mask": 2, "address_count": 1},
        {"mask": 4, "address_count": 3},
        {"mask": 8, "address_count": 1},
        {"mask": 3, "address_count": 1},
        {"mask": 5, "address_count": 1},
        {"mask": 6, "address_count": 1},
        {"mask": 10, "address_count": 1},
        {"mask": 15, "address_count": 1},
    ]
    retention_overlap = [
        {"mask": 0, "address_count": 91},
        {"mask": 1, "address_count": 2},
        {"mask": 2, "address_count": 1},
        {"mask": 8, "address_count": 1},
        {"mask": 3, "address_count": 1},
        {"mask": 5, "address_count": 1},
        {"mask": 6, "address_count": 1},
        {"mask": 10, "address_count": 1},
        {"mask": 11, "address_count": 1},
    ]
    return {
        "contract_version": "btc_candidate_statistics_v2",
        "policy_version": "btc_importance_v2",
        "source_standard_address_count": 100,
        "source_input_only_address_count": 3,
        "negative_current_utxo_count": 0,
        "null_value_count": 0,
        "value_cast_failure_count": 0,
        "max_observed_activity_time": datetime(
            2026, 7, 24, 22, tzinfo=UTC
        ),
        "source_cutoff_height": 959_187,
        "source_cutoff_time": CUTOFF_TIME,
        "query_sha256": QUERY_SHA256,
        "schema_sha256": SCHEMA_SHA256,
        "utxo_ge_1_btc_count": 20,
        "utxo_ge_10_btc_count": 10,
        "utxo_ge_100_btc_count": 5,
        "utxo_ge_1000_btc_count": 1,
        "raw_gross_90d_ge_10_btc_count": 20,
        "raw_gross_90d_ge_100_btc_count": 12,
        "raw_gross_90d_ge_1000_btc_count": 6,
        "raw_gross_90d_ge_10000_btc_count": 1,
        "residual_gross_90d_ge_10_btc_count": 15,
        "residual_gross_90d_ge_100_btc_count": 9,
        "residual_gross_90d_ge_1000_btc_count": 5,
        "residual_gross_90d_ge_10000_btc_count": 1,
        "recency_le_30d_count": 30,
        "recency_le_90d_count": 50,
        "recency_le_365d_count": 80,
        "lifetime_max_receipt_ge_500_btc_count": 20,
        "lifetime_max_receipt_ge_1000_btc_count": 10,
        "lifetime_max_receipt_ge_5000_btc_count": 2,
        "max_receipt_365d_ge_500_btc_count": 12,
        "max_receipt_365d_ge_1000_btc_count": 6,
        "max_receipt_365d_ge_5000_btc_count": 1,
        "max_receipt_90d_ge_500_btc_count": 8,
        "max_receipt_90d_ge_1000_btc_count": 4,
        "max_receipt_90d_ge_5000_btc_count": 1,
        "receipt_count_lifetime_ge_1_count": 20,
        "receipt_count_lifetime_ge_2_count": 8,
        "receipt_count_lifetime_ge_3_count": 3,
        "receipt_count_365d_ge_1_count": 12,
        "receipt_count_365d_ge_2_count": 5,
        "receipt_count_365d_ge_3_count": 2,
        "receipt_count_90d_ge_1_count": 8,
        "receipt_count_90d_ge_2_count": 3,
        "receipt_count_90d_ge_3_count": 1,
        "recent_receipt_retained_count": 2,
        "recent_receipt_repeated_count": 3,
        "recent_receipt_sustained_activity_count": 2,
        "strict_supported_receipt_count": 4,
        "unsupported_recent_singleton_count": 4,
        "stale_lifetime_singleton_count": 5,
        "receipt_support_overlap_distribution": [
            {"mask": 0, "address_count": 96},
            {"mask": 1, "address_count": 1},
            {"mask": 2, "address_count": 1},
            {"mask": 6, "address_count": 1},
            {"mask": 7, "address_count": 1},
        ],
        "score_histogram": [
            {"score": 0, "address_count": 40},
            {"score": 10, "address_count": 20},
            {"score": 20, "address_count": 20},
            {"score": 25, "address_count": 15},
            {"score": 40, "address_count": 5},
        ],
        "strict_variant": _variant(
            "V2-S",
            union=10,
            receipt_reason=4,
            incremental_receipt=1,
            overlap=strict_overlap,
            p1=5,
            edge=10,
            coarse=60,
        ),
        "balanced_variant": _variant(
            "V2-B",
            union=12,
            receipt_reason=6,
            incremental_receipt=3,
            overlap=balanced_overlap,
            p1=4,
            edge=9,
            coarse=62,
        ),
        "retention_variant": _variant(
            "V2-R",
            union=9,
            receipt_reason=2,
            incremental_receipt=0,
            overlap=retention_overlap,
            p1=6,
            edge=11,
            coarse=59,
        ),
    }


def _parse(
    row: dict[str, Any],
) -> tuple[CandidateStatisticsV2Result | None, Any]:
    return parse_candidate_statistics_v2_rows(
        [row],
        expected_query_sha256=QUERY_SHA256,
        expected_schema_sha256=SCHEMA_SHA256,
        expected_source_standard_address_count=100,
        expected_source_input_only_address_count=3,
        expected_cutoff_height=959_187,
        expected_cutoff_time=CUTOFF_TIME,
        now=datetime(2026, 7, 25, tzinfo=UTC),
        max_source_age=timedelta(hours=48),
    )


def test_v2_sql_is_fixed_aggregate_only_and_correlated_evidence_safe() -> None:
    plan = BigQueryQueryPlan.load("bigquery-public-data.crypto_bitcoin")
    repeated = BigQueryQueryPlan.load("bigquery-public-data.crypto_bitcoin")

    assert (
        plan.candidate_statistics_v2_sha256
        == repeated.candidate_statistics_v2_sha256
    )
    sql = plan.candidate_statistics_v2_sql
    assert sql.count("bigquery-public-data.crypto_bitcoin.transactions") == 1
    assert "ARRAY_CONCAT" in sql
    assert "address_transaction" in sql
    assert "max_same_tx_received_90d_sats" in sql
    assert "max_same_tx_received_365d_sats" in sql
    assert "gross_flow_90d_sats - max_same_tx_received_90d_sats" in sql
    assert "recent_receipt_retained" in sql
    assert "recent_receipt_repeated" in sql
    assert "recent_receipt_sustained_activity" in sql
    assert "balanced_receipt_supported" in sql
    assert "retention_receipt_supported" in sql
    assert "strict_variant" in sql
    assert "balanced_variant" in sql
    assert "retention_variant" in sql
    final_select = sql.rsplit("SELECT", 1)[-1]
    assert "normalized_address" not in final_select
    assert "transaction_hash" not in final_select


def test_valid_v2_result_is_interpretable_with_known_source_warning() -> None:
    result, quality = _parse(valid_result_row())

    assert result is not None
    assert result.strict_variant.chain_p0_union_count == 10
    assert quality.status == "warn"
    assert quality.allow_interpretation is True
    assert quality.blocking_reasons == ()
    assert quality.warnings == (
        "candidate_statistics_v2_known_input_only_subjects_excluded",
    )


def test_v2_result_rejects_identifier_fields_and_wrong_row_count() -> None:
    row = valid_result_row()
    row["normalized_address"] = "not-allowed"

    result, quality = _parse(row)
    assert result is None
    assert quality.blocking_reasons == (
        "candidate_statistics_v2_result_invalid",
    )

    result, quality = parse_candidate_statistics_v2_rows(
        [],
        expected_query_sha256=QUERY_SHA256,
        expected_schema_sha256=SCHEMA_SHA256,
        expected_source_standard_address_count=100,
        expected_source_input_only_address_count=3,
        expected_cutoff_height=959_187,
        expected_cutoff_time=CUTOFF_TIME,
        now=datetime(2026, 7, 25, tzinfo=UTC),
        max_source_age=timedelta(hours=48),
    )
    assert result is None
    assert quality.blocking_reasons == (
        "candidate_statistics_v2_result_row_count_invalid",
    )


def test_v2_quality_blocks_source_and_receipt_funnel_drift() -> None:
    row = valid_result_row()
    row["source_standard_address_count"] = 101
    row["source_input_only_address_count"] = 4
    row["receipt_count_90d_ge_2_count"] = 9

    result, quality = _parse(row)

    assert result is not None
    assert quality.allow_interpretation is False
    assert "candidate_statistics_v2_input_only_baseline_mismatch" in (
        quality.blocking_reasons
    )
    assert "candidate_statistics_v2_receipt_funnel_invalid" in (
        quality.blocking_reasons
    )
    assert "candidate_statistics_v2_source_baseline_mismatch" in (
        quality.blocking_reasons
    )


def test_v2_quality_blocks_impossible_residual_turnover_counts() -> None:
    row = valid_result_row()
    row["residual_gross_90d_ge_10_btc_count"] = (
        row["raw_gross_90d_ge_10_btc_count"] + 1
    )

    result, quality = _parse(row)

    assert result is not None
    assert quality.allow_interpretation is False
    assert quality.blocking_reasons == (
        "candidate_statistics_v2_economic_counter_reconciliation_failed",
    )


def test_v2_quality_blocks_policy_counter_drift() -> None:
    row = valid_result_row()
    row["utxo_ge_100_btc_count"] = 4
    row["balanced_variant"] = deepcopy(row["balanced_variant"])
    row["balanced_variant"]["coarse_candidate_union_count"] = 59
    row["balanced_variant"]["excluded_source_address_count"] = 41

    result, quality = _parse(row)

    assert result is not None
    assert quality.allow_interpretation is False
    assert "candidate_statistics_v2_policy_counter_reconciliation_failed" in (
        quality.blocking_reasons
    )
    assert "candidate_statistics_v2_balanced_not_superset" in (
        quality.blocking_reasons
    )


def test_v2_quality_blocks_variant_reconciliation_and_size_hard_stops() -> None:
    row = valid_result_row()
    row["balanced_variant"] = deepcopy(row["balanced_variant"])
    row["balanced_variant"]["chain_p0_union_count"] = 9
    row["balanced_variant"]["coarse_candidate_union_count"] = 5_000_001
    row["balanced_variant"]["excluded_source_address_count"] = 0
    row["strict_variant"] = deepcopy(row["strict_variant"])
    row["strict_variant"]["edge_upgrade_frontier_count"] = 1_000_001

    result, quality = _parse(row)

    assert result is not None
    assert quality.allow_interpretation is False
    assert (
        "candidate_statistics_v2_balanced_not_superset"
        in quality.blocking_reasons
    )
    assert "candidate_statistics_v2_coarse_union_too_large" in (
        quality.blocking_reasons
    )
    assert "candidate_statistics_v2_edge_frontier_too_large" in (
        quality.blocking_reasons
    )
    assert "candidate_statistics_v2_variant_reconciliation_failed" in (
        quality.blocking_reasons
    )


def test_v2_quality_warns_on_review_thresholds() -> None:
    row = valid_result_row()
    row["source_standard_address_count"] = 2_500_000
    row["utxo_ge_1_btc_count"] = 70_000
    row["utxo_ge_10_btc_count"] = 70_000
    row["utxo_ge_100_btc_count"] = 70_000
    for variant_name in (
        "strict_variant",
        "balanced_variant",
        "retention_variant",
    ):
        variant = deepcopy(row[variant_name])
        variant["p0_overlap_distribution"] = [
            {"mask": 0, "address_count": 2_300_000},
            {"mask": 1, "address_count": 70_000},
            {"mask": 2, "address_count": 30_000},
            {"mask": 4, "address_count": 100_000},
        ]
        variant["p0_utxo_ge_100_btc_count"] = 70_000
        variant[
            "p0_sustained_residual_gross_90d_ge_1000_btc_count"
        ] = 30_000
        variant["p0_receipt_rule_count"] = 100_000
        variant[
            "p0_lifetime_ge_10000_active_supported_90d_count"
        ] = 0
        variant["chain_p0_union_count"] = 200_000
        variant["incremental_receipt_p0_count"] = 100_000
        variant["coarse_candidate_union_count"] = 2_100_000
        variant["excluded_source_address_count"] = 400_000
        row[variant_name] = variant
    row["score_histogram"] = [
        {"score": 0, "address_count": 2_500_000},
    ]
    row["lifetime_max_receipt_ge_500_btc_count"] = 100_000
    row["max_receipt_365d_ge_500_btc_count"] = 100_000
    row["max_receipt_90d_ge_500_btc_count"] = 100_000
    row["receipt_count_lifetime_ge_1_count"] = 100_000
    row["receipt_count_lifetime_ge_2_count"] = 0
    row["receipt_count_lifetime_ge_3_count"] = 0
    row["receipt_count_365d_ge_1_count"] = 100_000
    row["receipt_count_365d_ge_2_count"] = 0
    row["receipt_count_365d_ge_3_count"] = 0
    row["receipt_count_90d_ge_1_count"] = 100_000
    row["receipt_count_90d_ge_2_count"] = 0
    row["receipt_count_90d_ge_3_count"] = 0
    row["recent_receipt_retained_count"] = 100_000
    row["recent_receipt_repeated_count"] = 0
    row["recent_receipt_sustained_activity_count"] = 0
    row["strict_supported_receipt_count"] = 100_000
    row["unsupported_recent_singleton_count"] = 0
    row["stale_lifetime_singleton_count"] = 0
    row["receipt_support_overlap_distribution"] = [
        {"mask": 0, "address_count": 2_400_000},
        {"mask": 1, "address_count": 100_000},
    ]

    result, quality = parse_candidate_statistics_v2_rows(
        [row],
        expected_query_sha256=QUERY_SHA256,
        expected_schema_sha256=SCHEMA_SHA256,
        expected_source_standard_address_count=2_500_000,
        expected_source_input_only_address_count=3,
        expected_cutoff_height=959_187,
        expected_cutoff_time=CUTOFF_TIME,
        now=datetime(2026, 7, 25, tzinfo=UTC),
        max_source_age=timedelta(hours=48),
    )

    assert result is not None
    assert quality.allow_interpretation is True
    assert quality.status == "warn"
    assert "candidate_statistics_v2_strict_p0_large" in quality.warnings
    assert "candidate_statistics_v2_strict_coarse_union_large" in (
        quality.warnings
    )
    assert "candidate_statistics_v2_incremental_receipt_budget_exceeded" in (
        quality.warnings
    )


class FakeV2ProbeBackend:
    def __init__(self, *, dry_run_bytes: int = 900) -> None:
        self.tables = table_metadata()
        self.dry_run_bytes = dry_run_bytes
        self.calls: list[str] = []

    def table_metadata(self, table_id: str):
        self.calls.append("table_metadata")
        return self.tables[table_id.rsplit(".", 1)[-1]]

    def monthly_successful_query_usage(
        self,
        *,
        month_start: datetime,
        month_end: datetime,
    ) -> MonthlyQueryUsage:
        self.calls.append("monthly_successful_query_usage")
        return MonthlyQueryUsage(
            successful_query_jobs=2,
            total_bytes_billed=100,
        )

    def dry_run(
        self,
        sql: str,
        parameters: dict[str, object],
        maximum_bytes_billed: int,
    ) -> QueryEstimate:
        self.calls.append("dry_run")
        assert "btc_candidate_statistics_v2" in sql
        assert maximum_bytes_billed == 0
        return QueryEstimate(
            total_bytes_processed=self.dry_run_bytes,
            cache_hit=False,
        )


def test_v2_probe_is_cost_only_and_never_executes() -> None:
    plan = BigQueryQueryPlan.load("bigquery-public-data.crypto_bitcoin")
    backend = FakeV2ProbeBackend()

    result = BigQueryCandidateStatisticsV2Probe(
        backend=backend,
        dataset="bigquery-public-data.crypto_bitcoin",
        max_source_age=timedelta(hours=48),
        now=datetime(2026, 7, 24, 1, tzinfo=UTC),
    ).run(
        cutoff_height=959_187,
        cutoff_time=CUTOFF_TIME,
        expected_query_sha256=plan.candidate_statistics_v2_sha256,
        sandbox_budget_bytes=2_000,
        reserve_bytes=500,
    )

    assert result.status == "within_budget"
    assert result.query_kind == "btc_candidate_statistics_v2"
    assert result.network_requests == 3
    assert result.within_budget is True
    assert backend.calls == [
        "table_metadata",
        "monthly_successful_query_usage",
        "dry_run",
    ]
    assert not hasattr(backend, "execute_aggregate_at_most_two_no_retry")


def test_v2_probe_blocks_wrong_hash_before_network() -> None:
    backend = FakeV2ProbeBackend()

    result = BigQueryCandidateStatisticsV2Probe(
        backend=backend,
        dataset="bigquery-public-data.crypto_bitcoin",
        max_source_age=timedelta(hours=48),
        now=datetime(2026, 7, 24, 1, tzinfo=UTC),
    ).run(
        cutoff_height=959_187,
        cutoff_time=CUTOFF_TIME,
        expected_query_sha256="ff" * 32,
        sandbox_budget_bytes=2_000,
        reserve_bytes=500,
    )

    assert result.status == "blocked"
    assert result.blocking_reasons == (
        "candidate_statistics_v2_query_hash_mismatch",
    )
    assert backend.calls == []
