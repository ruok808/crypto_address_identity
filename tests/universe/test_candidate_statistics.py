from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from crypto_address_identity.universe.bigquery import (
    MonthlyQueryUsage,
    QueryEstimate,
    TableMetadata,
)
from crypto_address_identity.universe.candidate_statistics import (
    BigQueryCandidateStatisticsProbe,
    CandidateStatisticsResult,
    parse_candidate_statistics_rows,
)
from crypto_address_identity.universe.query_plan import BigQueryQueryPlan
from tests.universe.test_bigquery_probe import table_metadata


CUTOFF_TIME = datetime(2026, 7, 24, 23, 59, 59, tzinfo=UTC)
QUERY_SHA256 = "11" * 32
SCHEMA_SHA256 = "22" * 32


def valid_result_row() -> dict[str, Any]:
    return {
        "contract_version": "btc_candidate_statistics_v1",
        "source_standard_address_count": 100,
        "source_input_only_address_count": 0,
        "negative_current_utxo_count": 0,
        "null_value_count": 0,
        "value_cast_failure_count": 0,
        "max_observed_activity_time": datetime(2026, 7, 24, 22, tzinfo=UTC),
        "source_cutoff_height": 959_187,
        "source_cutoff_time": CUTOFF_TIME,
        "query_sha256": QUERY_SHA256,
        "schema_sha256": SCHEMA_SHA256,
        "utxo_ge_1_btc_count": 20,
        "utxo_ge_10_btc_count": 10,
        "utxo_ge_100_btc_count": 5,
        "utxo_ge_1000_btc_count": 1,
        "same_tx_receive_ge_100_btc_count": 12,
        "same_tx_receive_ge_500_btc_count": 4,
        "same_tx_receive_ge_1000_btc_count": 2,
        "same_tx_receive_ge_5000_btc_count": 1,
        "gross_90d_ge_10_btc_count": 15,
        "gross_90d_ge_100_btc_count": 8,
        "gross_90d_ge_1000_btc_count": 3,
        "gross_90d_ge_10000_btc_count": 1,
        "recency_le_30d_count": 30,
        "recency_le_90d_count": 50,
        "recency_le_365d_count": 80,
        "lifetime_ge_10000_active_365d_count": 2,
        "p0_utxo_ge_100_btc_count": 5,
        "p0_same_tx_receive_ge_500_btc_count": 4,
        "p0_gross_90d_ge_1000_btc_count": 3,
        "p0_lifetime_ge_10000_active_365d_count": 2,
        "chain_p0_union_count": 10,
        "p0_overlap_distribution": [
            {"mask": 0, "address_count": 90},
            {"mask": 1, "address_count": 3},
            {"mask": 2, "address_count": 2},
            {"mask": 3, "address_count": 1},
            {"mask": 4, "address_count": 2},
            {"mask": 8, "address_count": 1},
            {"mask": 15, "address_count": 1},
        ],
        "chain_p1_count": 5,
        "p0_p1_overlap_count": 0,
        "score_histogram": [
            {"score": 0, "address_count": 40},
            {"score": 3, "address_count": 20},
            {"score": 10, "address_count": 15},
            {"score": 20, "address_count": 10},
            {"score": 25, "address_count": 10},
            {"score": 30, "address_count": 5},
        ],
        "edge_upgrade_frontier_count": 10,
        "positive_economic_component_count": 25,
        "coarse_candidate_union_count": 60,
        "excluded_source_address_count": 40,
        "current_capital_count": 20,
        "historical_large_receipt_count": 12,
        "high_turnover_count": 15,
        "dormant_holder_count": 3,
    }


def parse_row(
    row: dict[str, Any],
    *,
    expected_source_count: int = 100,
) -> tuple[CandidateStatisticsResult | None, Any]:
    return parse_candidate_statistics_rows(
        [row],
        expected_query_sha256=QUERY_SHA256,
        expected_schema_sha256=SCHEMA_SHA256,
        expected_source_standard_address_count=expected_source_count,
        expected_cutoff_height=959_187,
        expected_cutoff_time=CUTOFF_TIME,
        now=datetime(2026, 7, 25, tzinfo=UTC),
        max_source_age=timedelta(hours=48),
    )


def test_candidate_statistics_sql_is_fixed_and_aggregate_only() -> None:
    plan = BigQueryQueryPlan.load("bigquery-public-data.crypto_bitcoin")
    repeated = BigQueryQueryPlan.load("bigquery-public-data.crypto_bitcoin")

    assert plan.candidate_statistics_sha256 == repeated.candidate_statistics_sha256
    assert (
        plan.candidate_statistics_sql.count(
            "bigquery-public-data.crypto_bitcoin.transactions"
        )
        == 1
    )
    assert "ARRAY_CONCAT" in plan.candidate_statistics_sql
    assert "SAFE_CAST(io.value AS BIGNUMERIC)" in plan.candidate_statistics_sql
    assert (
        "block_hash, transaction_hash, normalized_address, row_kind"
        in plan.candidate_statistics_sql
    )
    assert "10000000000" in plan.candidate_statistics_sql
    assert "50000000000" in plan.candidate_statistics_sql
    assert "100000000000" in plan.candidate_statistics_sql
    assert "1000000000000" in plan.candidate_statistics_sql
    assert "TIMESTAMP_DIFF(@cutoff_time, last_seen_time, DAY) <= 365" in (
        plan.candidate_statistics_sql
    )
    assert "p0_overlap_distribution" in plan.candidate_statistics_sql
    assert "score_histogram" in plan.candidate_statistics_sql
    final_select = plan.candidate_statistics_sql.rsplit("SELECT", 1)[-1]
    assert "normalized_address" not in final_select
    assert "transaction_hash" not in final_select


def test_valid_candidate_statistics_result_allows_interpretation() -> None:
    result, quality = parse_row(valid_result_row())

    assert result is not None
    assert result.chain_p0_union_count == 10
    assert quality.status == "allow"
    assert quality.allow_interpretation is True
    assert quality.blocking_reasons == ()
    assert quality.warnings == ()


def test_result_model_rejects_identifier_fields() -> None:
    row = valid_result_row()
    row["normalized_address"] = "bc1qforbidden"

    result, quality = parse_row(row)

    assert result is None
    assert quality.status == "blocked"
    assert quality.blocking_reasons == ("candidate_statistics_result_invalid",)


def test_result_parser_requires_exactly_one_row() -> None:
    arguments = {
        "expected_query_sha256": QUERY_SHA256,
        "expected_schema_sha256": SCHEMA_SHA256,
        "expected_source_standard_address_count": 100,
        "expected_cutoff_height": 959_187,
        "expected_cutoff_time": CUTOFF_TIME,
        "now": datetime(2026, 7, 25, tzinfo=UTC),
        "max_source_age": timedelta(hours=48),
    }

    missing_result, missing_quality = parse_candidate_statistics_rows([], **arguments)
    extra_result, extra_quality = parse_candidate_statistics_rows(
        [valid_result_row(), valid_result_row()], **arguments
    )

    assert missing_result is None
    assert missing_quality.blocking_reasons == (
        "candidate_statistics_result_row_count_invalid",
    )
    assert extra_result is None
    assert extra_quality.blocking_reasons == (
        "candidate_statistics_result_row_count_invalid",
    )


def test_result_quality_blocks_source_and_policy_failures() -> None:
    row = valid_result_row()
    row.update(
        {
            "source_input_only_address_count": 1,
            "negative_current_utxo_count": 1,
            "null_value_count": 1,
            "value_cast_failure_count": 1,
            "p0_p1_overlap_count": 1,
            "source_cutoff_height": 959_188,
            "query_sha256": "33" * 32,
            "schema_sha256": "44" * 32,
        }
    )

    _, quality = parse_row(row, expected_source_count=101)

    assert quality.allow_interpretation is False
    assert quality.blocking_reasons == (
        "candidate_statistics_input_only_addresses_present",
        "candidate_statistics_negative_balance",
        "candidate_statistics_null_value",
        "candidate_statistics_p0_p1_overlap",
        "candidate_statistics_query_hash_mismatch",
        "candidate_statistics_schema_hash_mismatch",
        "candidate_statistics_source_baseline_mismatch",
        "candidate_statistics_source_cutoff_mismatch",
        "candidate_statistics_value_cast_failure",
    )


def test_result_quality_blocks_reconciliation_drift() -> None:
    row = valid_result_row()
    row["excluded_source_address_count"] = 69
    row["p0_overlap_distribution"] = [
        {"mask": 0, "address_count": 91},
        *row["p0_overlap_distribution"][1:],
    ]
    row["score_histogram"] = [{"score": 0, "address_count": 99}]

    _, quality = parse_row(row)

    assert quality.blocking_reasons == (
        "candidate_statistics_candidate_union_reconciliation_failed",
        "candidate_statistics_p0_overlap_reconciliation_failed",
        "candidate_statistics_score_histogram_reconciliation_failed",
    )


def test_result_quality_blocks_policy_counter_drift() -> None:
    row = valid_result_row()
    row["current_capital_count"] = 19

    _, quality = parse_row(row)

    assert quality.blocking_reasons == (
        "candidate_statistics_policy_counter_reconciliation_failed",
    )


def test_result_quality_emits_scale_and_freshness_warnings() -> None:
    row = valid_result_row()
    row.update(
        {
            "source_standard_address_count": 10_000_000,
            "coarse_candidate_union_count": 6_000_000,
            "excluded_source_address_count": 4_000_000,
            "chain_p0_union_count": 120_001,
            "edge_upgrade_frontier_count": 1_000_001,
            "current_capital_count": 3_000_001,
            "utxo_ge_1_btc_count": 3_000_001,
            "utxo_ge_10_btc_count": 500_000,
            "utxo_ge_100_btc_count": 120_001,
            "p0_overlap_distribution": [
                {"mask": 0, "address_count": 9_879_999},
                {"mask": 1, "address_count": 120_001},
            ],
            "p0_utxo_ge_100_btc_count": 120_001,
            "p0_same_tx_receive_ge_500_btc_count": 0,
            "p0_gross_90d_ge_1000_btc_count": 0,
            "p0_lifetime_ge_10000_active_365d_count": 0,
            "same_tx_receive_ge_500_btc_count": 0,
            "same_tx_receive_ge_1000_btc_count": 0,
            "same_tx_receive_ge_5000_btc_count": 0,
            "gross_90d_ge_1000_btc_count": 0,
            "gross_90d_ge_10000_btc_count": 0,
            "lifetime_ge_10000_active_365d_count": 0,
            "score_histogram": [
                {"score": 0, "address_count": 10_000_000},
            ],
            "max_observed_activity_time": datetime(2026, 7, 23, 12, tzinfo=UTC),
        }
    )

    result, quality = parse_row(row, expected_source_count=10_000_000)

    assert result is not None
    assert quality.status == "warn"
    assert quality.warnings == (
        "candidate_statistics_coarse_union_large",
        "candidate_statistics_cohort_concentrated",
        "candidate_statistics_edge_frontier_large",
        "candidate_statistics_p0_union_large",
        "candidate_statistics_source_activity_older_than_24h",
    )


class FakeCandidateStatisticsBackend:
    def __init__(
        self,
        *,
        dry_run_bytes: int = 638,
        billed_bytes: int = 100,
    ) -> None:
        self.tables = table_metadata()
        self.dry_run_bytes = dry_run_bytes
        self.billed_bytes = billed_bytes
        self.calls: list[str] = []

    def table_metadata(self, table_id: str) -> TableMetadata:
        self.calls.append("table_metadata")
        return self.tables[table_id.rsplit(".", 1)[-1]]

    def monthly_successful_query_usage(
        self, *, month_start: datetime, month_end: datetime
    ) -> MonthlyQueryUsage:
        self.calls.append("monthly_successful_query_usage")
        assert month_start == datetime(2026, 7, 1, tzinfo=UTC)
        assert month_end == datetime(2026, 8, 1, tzinfo=UTC)
        return MonthlyQueryUsage(
            successful_query_jobs=2,
            total_bytes_billed=self.billed_bytes,
        )

    def dry_run(
        self,
        sql: str,
        parameters: dict[str, object],
        maximum_bytes_billed: int,
    ) -> QueryEstimate:
        self.calls.append("dry_run")
        assert maximum_bytes_billed == 0
        assert parameters["cutoff_height"] == 959_187
        assert parameters["query_sha256"] == BigQueryQueryPlan.hash_sql(sql)
        return QueryEstimate(
            total_bytes_processed=self.dry_run_bytes,
            cache_hit=False,
        )

    def query_one(self, *args: object, **kwargs: object) -> Any:
        raise AssertionError("candidate statistics probe must not execute a query")

    def stream_arrow_batches(self, *args: object, **kwargs: object) -> Any:
        raise AssertionError("candidate statistics probe must not stream rows")


def test_candidate_statistics_probe_is_cost_only_and_within_budget() -> None:
    backend = FakeCandidateStatisticsBackend()
    plan = BigQueryQueryPlan.load("bigquery-public-data.crypto_bitcoin")

    result = BigQueryCandidateStatisticsProbe(
        backend=backend,
        dataset="bigquery-public-data.crypto_bitcoin",
        max_source_age=timedelta(hours=48),
        now=datetime(2026, 7, 24, 1, tzinfo=UTC),
    ).run(
        cutoff_height=959_187,
        cutoff_time=CUTOFF_TIME,
        expected_query_sha256=plan.candidate_statistics_sha256,
        sandbox_budget_bytes=1_000,
        reserve_bytes=200,
    )

    assert result.status == "within_budget"
    assert result.dry_run_bytes == 638
    assert result.month_to_date_billed_bytes == 100
    assert result.projected_month_to_date_bytes == 738
    assert result.projected_reserve_bytes == 262
    assert result.within_budget is True
    assert result.network_requests == 3
    assert backend.calls == [
        "table_metadata",
        "monthly_successful_query_usage",
        "dry_run",
    ]


def test_candidate_statistics_probe_blocks_query_cap_and_reserve() -> None:
    plan = BigQueryQueryPlan.load("bigquery-public-data.crypto_bitcoin")
    backend = FakeCandidateStatisticsBackend(
        dry_run_bytes=650_000_000_001,
        billed_bytes=200_000_000_000,
    )

    result = BigQueryCandidateStatisticsProbe(
        backend=backend,
        dataset="bigquery-public-data.crypto_bitcoin",
        max_source_age=timedelta(hours=48),
        now=datetime(2026, 7, 24, 1, tzinfo=UTC),
    ).run(
        cutoff_height=959_187,
        cutoff_time=CUTOFF_TIME,
        expected_query_sha256=plan.candidate_statistics_sha256,
        sandbox_budget_bytes=1_000_000_000_000,
        reserve_bytes=250_000_000_000,
    )

    assert result.status == "blocked"
    assert result.within_budget is False
    assert result.blocking_reasons == (
        "candidate_statistics_dry_run_limit_exceeded",
        "candidate_statistics_monthly_budget_exceeded",
        "candidate_statistics_monthly_reserve_insufficient",
    )


def test_candidate_statistics_probe_blocks_query_hash_before_network() -> None:
    backend = FakeCandidateStatisticsBackend()

    result = BigQueryCandidateStatisticsProbe(
        backend=backend,
        dataset="bigquery-public-data.crypto_bitcoin",
        max_source_age=timedelta(hours=48),
        now=datetime(2026, 7, 24, 1, tzinfo=UTC),
    ).run(
        cutoff_height=959_187,
        cutoff_time=CUTOFF_TIME,
        expected_query_sha256="ff" * 32,
        sandbox_budget_bytes=1_000,
        reserve_bytes=200,
    )

    assert result.status == "blocked"
    assert result.blocking_reasons == (
        "candidate_statistics_query_hash_mismatch",
    )
    assert result.network_requests == 0
    assert backend.calls == []
