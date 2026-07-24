from __future__ import annotations

import hashlib
import json
from copy import deepcopy
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
from crypto_address_identity.universe.candidate_execution_v2 import (
    CANDIDATE_STATISTICS_V2_AUTHORIZATION_ID,
    CANDIDATE_STATISTICS_V2_MAXIMUM_BYTES_BILLED,
    CANDIDATE_STATISTICS_V2_RECOVERY_AUTHORIZATION_ID,
    PINNED_V2_CANDIDATE_QUERY_SHA256,
    PINNED_V2_CANDIDATE_SCHEMA_SHA256,
    PINNED_V2_CUTOFF_DATE,
    PINNED_V2_DRY_RUN_BYTES,
    PINNED_V2_FAILED_JOB_ERROR_REASON,
    PINNED_V2_FAILED_JOB_ID,
    PINNED_V2_FAILED_RECEIPT_SHA256,
    PINNED_V2_MONTHLY_PROCESSING_BUDGET_BYTES,
    PINNED_V2_MONTHLY_RESERVE_BYTES,
    CandidateStatisticsV2ExecutionAlreadyAttempted,
    CandidateStatisticsV2ExistingJobNotReconcilable,
    CandidateStatisticsV2ExistingJobReconciler,
    CandidateStatisticsV2RecoveryEvidenceInvalid,
    CandidateStatisticsV2ExecutionRequest,
    CandidateStatisticsV2OneShotExecutor,
    preview_candidate_statistics_v2_execution,
)
from crypto_address_identity.universe.candidate_statistics_v2 import (
    PINNED_V2_CUTOFF_HEIGHT,
    PINNED_V2_SOURCE_INPUT_ONLY_ADDRESS_COUNT,
    PINNED_V2_SOURCE_STANDARD_ADDRESS_COUNT,
)
from tests.universe.test_bigquery_probe import table_metadata
from tests.universe.test_candidate_statistics_v2 import valid_result_row


NOW = datetime(2026, 7, 25, 1, tzinfo=UTC)
CUTOFF_TIME = datetime.combine(
    PINNED_V2_CUTOFF_DATE,
    time.max,
    tzinfo=UTC,
)
EXPECTED_SUCCESSFUL_QUERY_JOBS = 5
EXPECTED_MONTH_TO_DATE_BILLED_BYTES = 838_768_525_312


class PinnedV2TableMetadata:
    def __init__(self) -> None:
        source = table_metadata()["transactions"]
        self.table_id = source.table_id
        self.fields = source.fields
        self.partition_field = source.partition_field
        self.partition_type = source.partition_type
        self.modified_at = datetime(2026, 7, 24, 23, tzinfo=UTC)
        self.schema_sha256 = PINNED_V2_CANDIDATE_SCHEMA_SHA256


def execution_result_row() -> dict[str, Any]:
    row = deepcopy(valid_result_row())
    source_count = PINNED_V2_SOURCE_STANDARD_ADDRESS_COUNT
    row.update(
        {
            "source_standard_address_count": source_count,
            "source_input_only_address_count": (
                PINNED_V2_SOURCE_INPUT_ONLY_ADDRESS_COUNT
            ),
            "max_observed_activity_time": datetime(
                2026,
                7,
                24,
                22,
                tzinfo=UTC,
            ),
            "source_cutoff_height": PINNED_V2_CUTOFF_HEIGHT,
            "source_cutoff_time": CUTOFF_TIME,
            "query_sha256": PINNED_V2_CANDIDATE_QUERY_SHA256,
            "schema_sha256": PINNED_V2_CANDIDATE_SCHEMA_SHA256,
        }
    )
    row["receipt_support_overlap_distribution"][0]["address_count"] = (
        source_count - 4
    )
    row["score_histogram"][0]["address_count"] = source_count - 60
    for variant_name in (
        "strict_variant",
        "balanced_variant",
        "retention_variant",
    ):
        variant = row[variant_name]
        nonzero = sum(
            bucket["address_count"]
            for bucket in variant["p0_overlap_distribution"]
            if bucket["mask"] != 0
        )
        variant["p0_overlap_distribution"][0]["address_count"] = (
            source_count - nonzero
        )
        variant["excluded_source_address_count"] = (
            source_count - variant["coarse_candidate_union_count"]
        )
    return row


def execution_request(
    **changes: object,
) -> CandidateStatisticsV2ExecutionRequest:
    values: dict[str, object] = {
        "authorization_id": CANDIDATE_STATISTICS_V2_AUTHORIZATION_ID,
        "billing_acknowledged": True,
        "as_of_date": PINNED_V2_CUTOFF_DATE,
        "cutoff_height": PINNED_V2_CUTOFF_HEIGHT,
        "expected_query_sha256": PINNED_V2_CANDIDATE_QUERY_SHA256,
        "expected_schema_sha256": PINNED_V2_CANDIDATE_SCHEMA_SHA256,
        "expected_source_standard_address_count": (
            PINNED_V2_SOURCE_STANDARD_ADDRESS_COUNT
        ),
        "expected_source_input_only_address_count": (
            PINNED_V2_SOURCE_INPUT_ONLY_ADDRESS_COUNT
        ),
        "expected_dry_run_bytes": PINNED_V2_DRY_RUN_BYTES,
        "expected_successful_query_jobs": EXPECTED_SUCCESSFUL_QUERY_JOBS,
        "expected_month_to_date_billed_bytes": (
            EXPECTED_MONTH_TO_DATE_BILLED_BYTES
        ),
        "maximum_bytes_billed": (
            CANDIDATE_STATISTICS_V2_MAXIMUM_BYTES_BILLED
        ),
        "monthly_processing_budget_bytes": (
            PINNED_V2_MONTHLY_PROCESSING_BUDGET_BYTES
        ),
        "reserve_bytes": PINNED_V2_MONTHLY_RESERVE_BYTES,
    }
    values.update(changes)
    return CandidateStatisticsV2ExecutionRequest.model_validate(values)


def recovery_request(
    *,
    previous_receipt_sha256: str,
    **changes: object,
) -> CandidateStatisticsV2ExecutionRequest:
    values: dict[str, object] = {
        **execution_request().model_dump(mode="python"),
        "authorization_id": CANDIDATE_STATISTICS_V2_RECOVERY_AUTHORIZATION_ID,
        "recovery_from_authorization_id": (
            CANDIDATE_STATISTICS_V2_AUTHORIZATION_ID
        ),
        "expected_previous_receipt_sha256": previous_receipt_sha256,
        "expected_previous_job_id": PINNED_V2_FAILED_JOB_ID,
        "expected_previous_job_error_reason": (
            PINNED_V2_FAILED_JOB_ERROR_REASON
        ),
        "expected_previous_job_total_bytes_processed": 0,
        "expected_previous_job_total_bytes_billed": 0,
    }
    values.update(changes)
    return CandidateStatisticsV2ExecutionRequest.model_validate(values)


class FakeV2ExecutionBackend:
    def __init__(
        self,
        *,
        rows: tuple[dict[str, Any], ...] | None = None,
        dry_run_bytes: int = PINNED_V2_DRY_RUN_BYTES,
        successful_query_jobs: int = EXPECTED_SUCCESSFUL_QUERY_JOBS,
        month_to_date_billed_bytes: int = (
            EXPECTED_MONTH_TO_DATE_BILLED_BYTES
        ),
        execution_processed_bytes: int = PINNED_V2_DRY_RUN_BYTES,
        execution_billed_bytes: int = PINNED_V2_DRY_RUN_BYTES,
        fail_execution: bool = False,
        fail_existing_job_fetch: bool = False,
    ) -> None:
        self.rows = rows if rows is not None else (execution_result_row(),)
        self.dry_run_bytes = dry_run_bytes
        self.successful_query_jobs = successful_query_jobs
        self.month_to_date_billed_bytes = month_to_date_billed_bytes
        self.execution_processed_bytes = execution_processed_bytes
        self.execution_billed_bytes = execution_billed_bytes
        self.fail_execution = fail_execution
        self.fail_existing_job_fetch = fail_existing_job_fetch
        self.calls: list[str] = []
        self.receipt_path: Path | None = None

    def table_metadata(self, table_id: str) -> PinnedV2TableMetadata:
        self.calls.append("table_metadata")
        assert table_id.endswith(".transactions")
        return PinnedV2TableMetadata()

    def monthly_successful_query_usage(
        self,
        *,
        month_start: datetime,
        month_end: datetime,
    ) -> MonthlyQueryUsage:
        self.calls.append("monthly_successful_query_usage")
        assert month_start == datetime(2026, 7, 1, tzinfo=UTC)
        assert month_end == datetime(2026, 8, 1, tzinfo=UTC)
        return MonthlyQueryUsage(
            successful_query_jobs=self.successful_query_jobs,
            total_bytes_billed=self.month_to_date_billed_bytes,
        )

    def dry_run(
        self,
        sql: str,
        parameters: dict[str, object],
        maximum_bytes_billed: int,
    ) -> QueryEstimate:
        self.calls.append("dry_run")
        assert maximum_bytes_billed == 0
        assert parameters["cutoff_height"] == PINNED_V2_CUTOFF_HEIGHT
        assert parameters["query_sha256"] == (
            PINNED_V2_CANDIDATE_QUERY_SHA256
        )
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
        receipt = json.loads(
            self.receipt_path.read_text(encoding="utf-8")
        )
        assert receipt["status"] == "started"
        assert maximum_bytes_billed == (
            CANDIDATE_STATISTICS_V2_MAXIMUM_BYTES_BILLED
        )
        assert job_id.startswith("cai_btc_importance_v2_")
        assert parameters["schema_sha256"] == (
            PINNED_V2_CANDIDATE_SCHEMA_SHA256
        )
        if self.fail_execution:
            raise RuntimeError("raw upstream error must not escape")
        return AggregateQueryExecution(
            rows=self.rows,
            total_bytes_processed=self.execution_processed_bytes,
            total_bytes_billed=self.execution_billed_bytes,
        )

    def fetch_existing_aggregate_at_most_two_no_retry(
        self,
        *,
        job_id: str,
        timeout_seconds: float,
    ) -> AggregateQueryExecution:
        self.calls.append("fetch_existing_aggregate_at_most_two_no_retry")
        assert job_id.startswith("cai_btc_importance_v2_")
        assert timeout_seconds == 120.0
        if self.fail_existing_job_fetch:
            raise RuntimeError("raw upstream error must not escape")
        return AggregateQueryExecution(
            rows=self.rows,
            total_bytes_processed=self.execution_processed_bytes,
            total_bytes_billed=self.execution_billed_bytes,
        )


def executor(
    tmp_path: Path,
    backend: FakeV2ExecutionBackend,
    *,
    authorization_id: str = CANDIDATE_STATISTICS_V2_AUTHORIZATION_ID,
) -> CandidateStatisticsV2OneShotExecutor:
    receipt_root = tmp_path / "executions"
    backend.receipt_path = (
        receipt_root / f"{authorization_id}.json"
    )
    return CandidateStatisticsV2OneShotExecutor(
        backend=backend,
        dataset="bigquery-public-data.crypto_bitcoin",
        receipt_root=receipt_root,
        max_source_age=timedelta(hours=48),
        now=NOW,
    )


def write_started_recovery_receipt(
    tmp_path: Path,
    *,
    changes: dict[str, object] | None = None,
) -> Path:
    request = recovery_request(
        previous_receipt_sha256=PINNED_V2_FAILED_RECEIPT_SHA256,
    )
    receipt_path = (
        tmp_path
        / "executions"
        / f"{request.authorization_id}.json"
    )
    payload: dict[str, object] = {
        "schema_version": (
            "btc_candidate_statistics_v2_execution_receipt_v1"
        ),
        "status": "started",
        "authorization_id": request.authorization_id,
        "job_id": (
            "cai_btc_importance_v2_"
            "b2bf4b71772b68d2d2e7f7ec2303745a48d69d74"
        ),
        "created_at": NOW.isoformat().replace("+00:00", "Z"),
        "billing_acknowledged": True,
        "query_sha256": request.expected_query_sha256,
        "expected_schema_sha256": request.expected_schema_sha256,
        "expected_source_standard_address_count": (
            request.expected_source_standard_address_count
        ),
        "expected_source_input_only_address_count": (
            request.expected_source_input_only_address_count
        ),
        "expected_dry_run_bytes": request.expected_dry_run_bytes,
        "expected_successful_query_jobs": (
            request.expected_successful_query_jobs
        ),
        "expected_month_to_date_billed_bytes": (
            request.expected_month_to_date_billed_bytes
        ),
        "cutoff_height": request.cutoff_height,
        "cutoff_time": request.cutoff_time.isoformat().replace(
            "+00:00",
            "Z",
        ),
        "maximum_bytes_billed": request.maximum_bytes_billed,
        "monthly_processing_budget_bytes": (
            request.monthly_processing_budget_bytes
        ),
        "reserve_bytes": request.reserve_bytes,
        "cost_estimate": {
            "source_kind": "bigquery",
            "query_kind": "btc_candidate_statistics_v2",
            "policy_version": "btc_importance_v2",
            "status": "within_budget",
            "query_sha256": request.expected_query_sha256,
            "schema_sha256": request.expected_schema_sha256,
            "dry_run_bytes": request.expected_dry_run_bytes,
            "successful_query_jobs": (
                request.expected_successful_query_jobs
            ),
            "month_to_date_billed_bytes": (
                request.expected_month_to_date_billed_bytes
            ),
            "sandbox_budget_bytes": (
                request.monthly_processing_budget_bytes
            ),
            "reserve_bytes": request.reserve_bytes,
            "projected_month_to_date_bytes": (
                request.expected_month_to_date_billed_bytes
                + request.expected_dry_run_bytes
            ),
            "projected_reserve_bytes": (
                request.monthly_processing_budget_bytes
                - request.expected_month_to_date_billed_bytes
                - request.expected_dry_run_bytes
            ),
            "within_budget": True,
            "read_only": True,
            "network_requests": 3,
            "blocking_reasons": [],
            "warnings": [],
        },
        "automatic_retries": 0,
        "candidate_materialized": False,
        "recovery_from_authorization_id": (
            request.recovery_from_authorization_id
        ),
        "expected_previous_receipt_sha256": (
            request.expected_previous_receipt_sha256
        ),
        "expected_previous_job_id": request.expected_previous_job_id,
        "expected_previous_job_error_reason": (
            request.expected_previous_job_error_reason
        ),
        "expected_previous_job_total_bytes_processed": 0,
        "expected_previous_job_total_bytes_billed": 0,
        "recovery_evidence_validated": True,
    }
    payload.update(changes or {})
    receipt_path.parent.mkdir(parents=True)
    receipt_path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="ascii",
    )
    receipt_path.chmod(0o600)
    return receipt_path


def reconciler(
    tmp_path: Path,
    backend: FakeV2ExecutionBackend,
) -> CandidateStatisticsV2ExistingJobReconciler:
    return CandidateStatisticsV2ExistingJobReconciler(
        backend=backend,
        dataset="bigquery-public-data.crypto_bitcoin",
        receipt_root=tmp_path / "executions",
        max_source_age=timedelta(hours=48),
        now=NOW,
    )


def test_v2_preview_is_offline_and_writes_nothing(tmp_path: Path) -> None:
    outcome = preview_candidate_statistics_v2_execution(
        execution_request(),
        dataset="bigquery-public-data.crypto_bitcoin",
        receipt_root=tmp_path / "executions",
    )

    assert outcome.status == "dry_run"
    assert outcome.billing_acknowledged is True
    assert outcome.execution_calls == 0
    assert outcome.automatic_retries == 0
    assert outcome.candidate_materialized is False
    assert outcome.written_paths == ()
    assert not (tmp_path / "executions").exists()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("billing_acknowledged", False),
        ("authorization_id", "another-authorization"),
        ("expected_query_sha256", "11" * 32),
        ("expected_dry_run_bytes", PINNED_V2_DRY_RUN_BYTES - 1),
        ("maximum_bytes_billed", 649_999_999_999),
        ("monthly_processing_budget_bytes", 1_999_999_999_999),
    ],
)
def test_v2_request_rejects_contract_drift(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValidationError):
        execution_request(**{field: value})


def test_v2_existing_receipt_blocks_before_network(tmp_path: Path) -> None:
    backend = FakeV2ExecutionBackend()
    service = executor(tmp_path, backend)
    assert backend.receipt_path is not None
    backend.receipt_path.parent.mkdir(parents=True)
    backend.receipt_path.write_text('{"status":"started"}\n', encoding="utf-8")

    with pytest.raises(CandidateStatisticsV2ExecutionAlreadyAttempted):
        service.run(execution_request())

    assert backend.calls == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("expected_previous_receipt_sha256", "11" * 32),
        ("expected_previous_job_id", "different_job"),
        ("expected_previous_job_error_reason", "different_reason"),
        ("expected_previous_job_total_bytes_processed", 1),
        ("expected_previous_job_total_bytes_billed", 1),
    ],
)
def test_v2_recovery_requires_exact_pinned_failure_evidence(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValidationError):
        recovery_request(
            previous_receipt_sha256=PINNED_V2_FAILED_RECEIPT_SHA256,
            **{field: value},
        )


def test_v2_recovery_validates_previous_receipt_before_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous_receipt = {
        "schema_version": "btc_candidate_statistics_v2_execution_receipt_v1",
        "status": "failed",
        "authorization_id": CANDIDATE_STATISTICS_V2_AUTHORIZATION_ID,
        "job_id": PINNED_V2_FAILED_JOB_ID,
        "query_sha256": PINNED_V2_CANDIDATE_QUERY_SHA256,
        "expected_schema_sha256": PINNED_V2_CANDIDATE_SCHEMA_SHA256,
        "expected_dry_run_bytes": PINNED_V2_DRY_RUN_BYTES,
        "maximum_bytes_billed": (
            CANDIDATE_STATISTICS_V2_MAXIMUM_BYTES_BILLED
        ),
        "execution_calls": 1,
        "automatic_retries": 0,
        "candidate_materialized": False,
    }
    encoded = (
        json.dumps(
            previous_receipt,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("ascii")
    previous_sha256 = hashlib.sha256(encoded).hexdigest()
    monkeypatch.setattr(
        "crypto_address_identity.universe.candidate_execution_v2."
        "PINNED_V2_FAILED_RECEIPT_SHA256",
        previous_sha256,
    )
    previous_path = (
        tmp_path
        / "executions"
        / f"{CANDIDATE_STATISTICS_V2_AUTHORIZATION_ID}.json"
    )
    previous_path.parent.mkdir(parents=True)
    previous_path.write_bytes(encoded)
    previous_path.chmod(0o600)
    request = recovery_request(
        previous_receipt_sha256=previous_sha256,
    )
    backend = FakeV2ExecutionBackend()
    service = executor(
        tmp_path,
        backend,
        authorization_id=CANDIDATE_STATISTICS_V2_RECOVERY_AUTHORIZATION_ID,
    )

    outcome = service.run(request)

    assert outcome.status == "completed"
    assert outcome.authorization_id == (
        CANDIDATE_STATISTICS_V2_RECOVERY_AUTHORIZATION_ID
    )
    assert outcome.recovery_from_authorization_id == (
        CANDIDATE_STATISTICS_V2_AUTHORIZATION_ID
    )
    assert outcome.recovery_evidence_validated is True
    assert outcome.job_id != PINNED_V2_FAILED_JOB_ID
    assert backend.calls == [
        "table_metadata",
        "monthly_successful_query_usage",
        "dry_run",
        "execute_aggregate_at_most_two_no_retry",
    ]
    assert backend.receipt_path is not None
    receipt = json.loads(
        backend.receipt_path.read_text(encoding="utf-8")
    )
    assert receipt["recovery_evidence_validated"] is True
    assert receipt["expected_previous_receipt_sha256"] == previous_sha256

    with pytest.raises(CandidateStatisticsV2ExecutionAlreadyAttempted):
        service.run(request)
    assert backend.calls.count(
        "execute_aggregate_at_most_two_no_retry"
    ) == 1


def test_v2_recovery_missing_previous_receipt_blocks_before_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous_sha256 = "22" * 32
    monkeypatch.setattr(
        "crypto_address_identity.universe.candidate_execution_v2."
        "PINNED_V2_FAILED_RECEIPT_SHA256",
        previous_sha256,
    )
    backend = FakeV2ExecutionBackend()
    service = executor(
        tmp_path,
        backend,
        authorization_id=CANDIDATE_STATISTICS_V2_RECOVERY_AUTHORIZATION_ID,
    )

    with pytest.raises(CandidateStatisticsV2RecoveryEvidenceInvalid):
        service.run(
            recovery_request(previous_receipt_sha256=previous_sha256)
        )

    assert backend.calls == []
    assert backend.receipt_path is not None
    assert not backend.receipt_path.exists()


def test_v2_reconciler_finishes_only_the_existing_started_job(
    tmp_path: Path,
) -> None:
    receipt_path = write_started_recovery_receipt(tmp_path)
    backend = FakeV2ExecutionBackend()

    outcome = reconciler(tmp_path, backend).run(
        recovery_request(
            previous_receipt_sha256=PINNED_V2_FAILED_RECEIPT_SHA256,
        )
    )

    assert outcome.status == "completed"
    assert outcome.row_count == 1
    assert outcome.execution_calls == 1
    assert outcome.automatic_retries == 0
    assert outcome.total_bytes_processed == PINNED_V2_DRY_RUN_BYTES
    assert outcome.candidate_materialized is False
    assert backend.calls == [
        "fetch_existing_aggregate_at_most_two_no_retry",
    ]
    receipt = json.loads(receipt_path.read_text(encoding="ascii"))
    assert receipt["status"] == "completed"
    assert receipt["job_id"] == outcome.job_id
    assert receipt["row_count"] == 1
    assert receipt_path.stat().st_mode & 0o777 == 0o600


def test_v2_reconciler_rejects_receipt_drift_before_network(
    tmp_path: Path,
) -> None:
    receipt_path = write_started_recovery_receipt(
        tmp_path,
        changes={"job_id": "different_job"},
    )
    original = receipt_path.read_bytes()
    backend = FakeV2ExecutionBackend()

    with pytest.raises(CandidateStatisticsV2ExistingJobNotReconcilable):
        reconciler(tmp_path, backend).run(
            recovery_request(
                previous_receipt_sha256=PINNED_V2_FAILED_RECEIPT_SHA256,
            )
        )

    assert backend.calls == []
    assert receipt_path.read_bytes() == original


def test_v2_reconciler_fetch_failure_leaves_started_receipt_retryable(
    tmp_path: Path,
) -> None:
    receipt_path = write_started_recovery_receipt(tmp_path)
    original = receipt_path.read_bytes()
    backend = FakeV2ExecutionBackend(fail_existing_job_fetch=True)

    with pytest.raises(CandidateStatisticsV2ExistingJobNotReconcilable):
        reconciler(tmp_path, backend).run(
            recovery_request(
                previous_receipt_sha256=PINNED_V2_FAILED_RECEIPT_SHA256,
            )
        )

    assert backend.calls == [
        "fetch_existing_aggregate_at_most_two_no_retry",
    ]
    assert receipt_path.read_bytes() == original


def test_v2_reconciler_applies_quality_gate_before_finishing_receipt(
    tmp_path: Path,
) -> None:
    receipt_path = write_started_recovery_receipt(tmp_path)
    backend = FakeV2ExecutionBackend(
        execution_processed_bytes=PINNED_V2_DRY_RUN_BYTES - 1,
    )

    outcome = reconciler(tmp_path, backend).run(
        recovery_request(
            previous_receipt_sha256=PINNED_V2_FAILED_RECEIPT_SHA256,
        )
    )

    assert outcome.status == "quality_blocked"
    assert (
        "candidate_statistics_v2_execution_processed_bytes_mismatch"
        in outcome.blocking_reasons
    )
    receipt = json.loads(receipt_path.read_text(encoding="ascii"))
    assert receipt["status"] == "quality_blocked"
    assert receipt["candidate_materialized"] is False


@pytest.mark.parametrize(
    "backend",
    [
        FakeV2ExecutionBackend(dry_run_bytes=PINNED_V2_DRY_RUN_BYTES + 1),
        FakeV2ExecutionBackend(successful_query_jobs=6),
        FakeV2ExecutionBackend(
            month_to_date_billed_bytes=(
                EXPECTED_MONTH_TO_DATE_BILLED_BYTES + 1
            )
        ),
    ],
)
def test_v2_checkpoint_drift_blocks_before_receipt_and_execution(
    tmp_path: Path,
    backend: FakeV2ExecutionBackend,
) -> None:
    service = executor(tmp_path, backend)

    outcome = service.run(execution_request())

    assert outcome.status == "preflight_blocked"
    assert outcome.execution_calls == 0
    assert backend.calls == [
        "table_metadata",
        "monthly_successful_query_usage",
        "dry_run",
    ]
    assert backend.receipt_path is not None
    assert not backend.receipt_path.exists()


def test_v2_valid_result_executes_once_and_finishes_receipt(
    tmp_path: Path,
) -> None:
    backend = FakeV2ExecutionBackend()
    service = executor(tmp_path, backend)

    outcome = service.run(execution_request())

    assert outcome.status == "completed"
    assert outcome.row_count == 1
    assert outcome.execution_calls == 1
    assert outcome.automatic_retries == 0
    assert outcome.statistics is not None
    assert outcome.statistics.strict_variant.chain_p0_union_count == 10
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
    receipt = json.loads(
        backend.receipt_path.read_text(encoding="utf-8")
    )
    assert receipt["status"] == "completed"
    assert receipt["row_count"] == 1
    assert receipt["candidate_materialized"] is False
    assert receipt["job_id"].startswith("cai_btc_importance_v2_")
    assert outcome.job_id == receipt["job_id"]
    assert backend.receipt_path.stat().st_mode & 0o777 == 0o600

    with pytest.raises(CandidateStatisticsV2ExecutionAlreadyAttempted):
        service.run(execution_request())
    assert backend.calls.count(
        "execute_aggregate_at_most_two_no_retry"
    ) == 1


@pytest.mark.parametrize(
    "rows",
    [
        (),
        (execution_result_row(), execution_result_row()),
        (
            {
                **execution_result_row(),
                "normalized_address": "bc1qforbidden",
            },
        ),
    ],
)
def test_v2_invalid_result_is_quality_blocked_without_identifier_receipt(
    tmp_path: Path,
    rows: tuple[dict[str, Any], ...],
) -> None:
    backend = FakeV2ExecutionBackend(rows=rows)
    service = executor(tmp_path, backend)

    outcome = service.run(execution_request())

    assert outcome.status == "quality_blocked"
    assert outcome.execution_calls == 1
    assert outcome.candidate_materialized is False
    assert backend.receipt_path is not None
    receipt_text = backend.receipt_path.read_text(encoding="utf-8")
    assert "bc1qforbidden" not in receipt_text
    assert json.loads(receipt_text)["status"] == "quality_blocked"


def test_v2_execution_failure_is_safe_and_never_retried(
    tmp_path: Path,
) -> None:
    backend = FakeV2ExecutionBackend(fail_execution=True)
    service = executor(tmp_path, backend)

    outcome = service.run(execution_request())

    assert outcome.status == "failed"
    assert outcome.execution_calls == 1
    assert outcome.automatic_retries == 0
    assert outcome.blocking_reasons == (
        "candidate_statistics_v2_execution_failed",
    )
    assert backend.calls.count(
        "execute_aggregate_at_most_two_no_retry"
    ) == 1
    assert backend.receipt_path is not None
    receipt_text = backend.receipt_path.read_text(encoding="utf-8")
    assert "raw upstream error" not in receipt_text
    assert json.loads(receipt_text)["status"] == "failed"


def test_v2_post_execution_byte_drift_is_quality_blocked(
    tmp_path: Path,
) -> None:
    quality_blocked_row = execution_result_row()
    quality_blocked_row["null_value_count"] = 1
    backend = FakeV2ExecutionBackend(
        rows=(quality_blocked_row,),
        execution_processed_bytes=PINNED_V2_DRY_RUN_BYTES - 1,
    )
    service = executor(tmp_path, backend)

    outcome = service.run(execution_request())

    assert outcome.status == "quality_blocked"
    assert outcome.quality is not None
    assert outcome.quality.allow_interpretation is False
    assert "candidate_statistics_v2_null_value" in outcome.blocking_reasons
    assert (
        "candidate_statistics_v2_execution_processed_bytes_mismatch"
        in outcome.blocking_reasons
    )
