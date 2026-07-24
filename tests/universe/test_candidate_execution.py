from __future__ import annotations

import json
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from crypto_address_identity.universe.bigquery import (
    AggregateQueryExecution,
    MonthlyQueryUsage,
    QueryEstimate,
)
from crypto_address_identity.universe.candidate_execution import (
    CANDIDATE_STATISTICS_MAXIMUM_BYTES_BILLED,
    PINNED_CANDIDATE_QUERY_SHA256,
    PINNED_CANDIDATE_SCHEMA_SHA256,
    PINNED_CUTOFF_HEIGHT,
    PINNED_SOURCE_STANDARD_ADDRESS_COUNT,
    CandidateStatisticsExecutionAlreadyAttempted,
    CandidateStatisticsExecutionRequest,
    CandidateStatisticsOneShotExecutor,
    preview_candidate_statistics_execution,
)
from crypto_address_identity.universe.query_plan import BigQueryQueryPlan
from tests.universe.test_bigquery_probe import table_metadata
from tests.universe.test_candidate_statistics import valid_result_row


NOW = datetime(2026, 7, 24, 1, tzinfo=UTC)
CUTOFF_DATE = date(2026, 7, 24)
CUTOFF_TIME = datetime.combine(CUTOFF_DATE, time.max, tzinfo=UTC)


class PinnedTableMetadata:
    def __init__(self) -> None:
        source = table_metadata()["transactions"]
        self.table_id = source.table_id
        self.fields = source.fields
        self.partition_field = source.partition_field
        self.partition_type = source.partition_type
        self.modified_at = datetime(2026, 7, 23, 23, tzinfo=UTC)
        self.schema_sha256 = PINNED_CANDIDATE_SCHEMA_SHA256


def execution_result_row() -> dict[str, Any]:
    row = valid_result_row()
    source_count = PINNED_SOURCE_STANDARD_ADDRESS_COUNT
    row.update(
        {
            "source_standard_address_count": source_count,
            "max_observed_activity_time": datetime(
                2026, 7, 22, 23, 51, 20, tzinfo=UTC
            ),
            "source_cutoff_height": PINNED_CUTOFF_HEIGHT,
            "source_cutoff_time": CUTOFF_TIME,
            "query_sha256": PINNED_CANDIDATE_QUERY_SHA256,
            "schema_sha256": PINNED_CANDIDATE_SCHEMA_SHA256,
            "excluded_source_address_count": source_count - 60,
            "p0_overlap_distribution": [
                {"mask": 0, "address_count": source_count - 10},
                *valid_result_row()["p0_overlap_distribution"][1:],
            ],
            "score_histogram": [
                {"score": 0, "address_count": source_count - 60},
                *valid_result_row()["score_histogram"][1:],
            ],
        }
    )
    return row


def execution_request(
    *,
    authorization_id: str = "btc-candidate-statistics-20260724-v1",
    maximum_bytes_billed: int = CANDIDATE_STATISTICS_MAXIMUM_BYTES_BILLED,
    expected_source_address_count: int = PINNED_SOURCE_STANDARD_ADDRESS_COUNT,
) -> CandidateStatisticsExecutionRequest:
    return CandidateStatisticsExecutionRequest(
        authorization_id=authorization_id,
        as_of_date=CUTOFF_DATE,
        cutoff_height=PINNED_CUTOFF_HEIGHT,
        expected_query_sha256=PINNED_CANDIDATE_QUERY_SHA256,
        expected_schema_sha256=PINNED_CANDIDATE_SCHEMA_SHA256,
        expected_source_standard_address_count=expected_source_address_count,
        maximum_bytes_billed=maximum_bytes_billed,
        sandbox_budget_bytes=1_099_511_627_776,
        reserve_bytes=250_000_000_000,
    )


class FakeExecutionBackend:
    def __init__(
        self,
        *,
        rows: tuple[dict[str, Any], ...] | None = None,
        dry_run_bytes: int = 637_999_682_243,
        billed_bytes: int = 199_007_141_888,
        fail_execution: bool = False,
    ) -> None:
        self.rows = rows if rows is not None else (execution_result_row(),)
        self.dry_run_bytes = dry_run_bytes
        self.billed_bytes = billed_bytes
        self.fail_execution = fail_execution
        self.calls: list[str] = []
        self.receipt_path: Path | None = None

    def table_metadata(self, table_id: str) -> PinnedTableMetadata:
        self.calls.append("table_metadata")
        assert table_id.endswith(".transactions")
        return PinnedTableMetadata()

    def monthly_successful_query_usage(
        self, *, month_start: datetime, month_end: datetime
    ) -> MonthlyQueryUsage:
        self.calls.append("monthly_successful_query_usage")
        assert month_start == datetime(2026, 7, 1, tzinfo=UTC)
        assert month_end == datetime(2026, 8, 1, tzinfo=UTC)
        return MonthlyQueryUsage(
            successful_query_jobs=3,
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
        assert parameters["cutoff_height"] == PINNED_CUTOFF_HEIGHT
        assert parameters["query_sha256"] == PINNED_CANDIDATE_QUERY_SHA256
        return QueryEstimate(
            total_bytes_processed=self.dry_run_bytes,
            cache_hit=False,
        )

    def execute_aggregate_at_most_two_no_retry(
        self,
        sql: str,
        parameters: dict[str, object],
        *,
        maximum_bytes_billed: int,
        job_id: str,
    ) -> AggregateQueryExecution:
        self.calls.append("execute_aggregate_at_most_two_no_retry")
        assert self.receipt_path is not None
        assert json.loads(self.receipt_path.read_text(encoding="utf-8"))[
            "status"
        ] == "started"
        assert maximum_bytes_billed == CANDIDATE_STATISTICS_MAXIMUM_BYTES_BILLED
        assert job_id.startswith("cai_btc_candidate_statistics_")
        assert parameters["schema_sha256"] == PINNED_CANDIDATE_SCHEMA_SHA256
        if self.fail_execution:
            raise RuntimeError("raw upstream error must not escape")
        return AggregateQueryExecution(
            rows=self.rows,
            total_bytes_processed=self.dry_run_bytes,
            total_bytes_billed=self.dry_run_bytes,
        )


def executor(
    tmp_path: Path,
    backend: FakeExecutionBackend,
) -> CandidateStatisticsOneShotExecutor:
    receipt_root = tmp_path / "executions"
    backend.receipt_path = (
        receipt_root / "btc-candidate-statistics-20260724-v1.json"
    )
    return CandidateStatisticsOneShotExecutor(
        backend=backend,
        dataset="bigquery-public-data.crypto_bitcoin",
        receipt_root=receipt_root,
        max_source_age=timedelta(hours=48),
        now=NOW,
    )


def test_preview_validates_fixed_contract_without_backend_or_write(
    tmp_path: Path,
) -> None:
    outcome = preview_candidate_statistics_execution(
        execution_request(),
        dataset="bigquery-public-data.crypto_bitcoin",
        receipt_root=tmp_path / "executions",
    )

    assert outcome.status == "dry_run"
    assert outcome.execution_calls == 0
    assert outcome.automatic_retries == 0
    assert outcome.written_paths == ()
    assert outcome.candidate_materialized is False
    assert not (tmp_path / "executions").exists()


@pytest.mark.parametrize(
    "changes",
    [
        {"maximum_bytes_billed": 649_999_999_999},
        {"expected_source_address_count": 1_557_951_353},
    ],
)
def test_request_rejects_execution_contract_drift(changes: dict[str, int]) -> None:
    with pytest.raises(ValidationError):
        execution_request(**changes)


def test_existing_receipt_blocks_before_any_backend_call(tmp_path: Path) -> None:
    backend = FakeExecutionBackend()
    service = executor(tmp_path, backend)
    assert backend.receipt_path is not None
    backend.receipt_path.parent.mkdir(parents=True)
    backend.receipt_path.write_text('{"status":"started"}\n', encoding="utf-8")

    with pytest.raises(CandidateStatisticsExecutionAlreadyAttempted):
        service.run(execution_request())

    assert backend.calls == []


def test_cost_gate_blocks_before_receipt_and_execution(tmp_path: Path) -> None:
    backend = FakeExecutionBackend(dry_run_bytes=650_000_000_001)
    service = executor(tmp_path, backend)

    outcome = service.run(execution_request())

    assert outcome.status == "preflight_blocked"
    assert outcome.execution_calls == 0
    assert "candidate_statistics_dry_run_limit_exceeded" in (
        outcome.blocking_reasons
    )
    assert backend.calls == [
        "table_metadata",
        "monthly_successful_query_usage",
        "dry_run",
    ]
    assert backend.receipt_path is not None
    assert not backend.receipt_path.exists()


def test_valid_result_executes_once_and_completes_exclusive_receipt(
    tmp_path: Path,
) -> None:
    backend = FakeExecutionBackend()
    service = executor(tmp_path, backend)

    outcome = service.run(execution_request())

    assert outcome.status == "completed"
    assert outcome.row_count == 1
    assert outcome.execution_calls == 1
    assert outcome.automatic_retries == 0
    assert outcome.statistics is not None
    assert outcome.statistics.chain_p0_union_count == 10
    assert outcome.quality is not None
    assert outcome.quality.allow_interpretation is True
    assert outcome.candidate_materialized is False
    assert backend.calls == [
        "table_metadata",
        "monthly_successful_query_usage",
        "dry_run",
        "execute_aggregate_at_most_two_no_retry",
    ]
    assert backend.receipt_path is not None
    receipt = json.loads(backend.receipt_path.read_text(encoding="utf-8"))
    assert receipt["status"] == "completed"
    assert receipt["row_count"] == 1
    assert receipt["candidate_materialized"] is False
    assert backend.receipt_path.stat().st_mode & 0o777 == 0o600

    with pytest.raises(CandidateStatisticsExecutionAlreadyAttempted):
        service.run(execution_request())
    assert backend.calls.count("execute_aggregate_at_most_two_no_retry") == 1


@pytest.mark.parametrize(
    "rows",
    [
        (),
        (execution_result_row(), execution_result_row()),
        ({**execution_result_row(), "normalized_address": "bc1qforbidden"},),
    ],
)
def test_invalid_result_shape_is_quality_blocked_without_materialization(
    tmp_path: Path,
    rows: tuple[dict[str, Any], ...],
) -> None:
    backend = FakeExecutionBackend(rows=rows)
    service = executor(tmp_path, backend)

    outcome = service.run(execution_request())

    assert outcome.status == "quality_blocked"
    assert outcome.quality is not None
    assert outcome.quality.allow_interpretation is False
    assert outcome.candidate_materialized is False
    assert outcome.execution_calls == 1
    assert backend.receipt_path is not None
    receipt_text = backend.receipt_path.read_text(encoding="utf-8")
    receipt = json.loads(receipt_text)
    assert receipt["status"] == "quality_blocked"
    assert receipt["candidate_materialized"] is False
    assert "bc1qforbidden" not in receipt_text


def test_execution_failure_is_recorded_without_retry_or_raw_error(
    tmp_path: Path,
) -> None:
    backend = FakeExecutionBackend(fail_execution=True)
    service = executor(tmp_path, backend)

    outcome = service.run(execution_request())

    assert outcome.status == "failed"
    assert outcome.execution_calls == 1
    assert outcome.automatic_retries == 0
    assert outcome.blocking_reasons == (
        "candidate_statistics_execution_failed",
    )
    assert backend.calls.count("execute_aggregate_at_most_two_no_retry") == 1
    assert backend.receipt_path is not None
    receipt_text = backend.receipt_path.read_text(encoding="utf-8")
    assert "raw upstream error" not in receipt_text
    assert json.loads(receipt_text)["status"] == "failed"
