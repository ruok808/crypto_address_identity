from __future__ import annotations

import json
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from crypto_address_identity.universe.bigquery import (
    MonthlyQueryUsage,
    QueryEstimate,
)
from crypto_address_identity.universe.candidate_materialization_execution_v2_s import (
    STRICT_V2_S_AUTHORIZATION_ID,
    STRICT_V2_S_DESTINATION_TABLE_ID,
    STRICT_V2_S_EXPECTED_DRY_RUN_BYTES,
    CandidateDestinationMetadata,
    StrictV2SCloudExecution,
    StrictV2SMaterializationAlreadyAttempted,
    StrictV2SMaterializationExecutionRequest,
    StrictV2SMaterializationOneShotExecutor,
    candidate_destination_schema_sha256,
    preview_strict_v2_s_materialization_execution,
)
from crypto_address_identity.universe.candidate_materialization_v2_s import (
    EXPECTED_STRICT_V2_S_COARSE_COUNT,
    PINNED_STRICT_V2_S_QUERY_SHA256,
    STRICT_V2_S_CANDIDATE_SCHEMA,
    STRICT_V2_S_CANDIDATE_SCHEMA_SHA256,
)
from tests.universe.test_candidate_execution_v2 import (
    PinnedV2TableMetadata,
)
from tests.universe.test_candidate_population_contract_v2 import (
    _v1_receipt,
    _v2_receipt,
    _write_pair,
)


NOW = datetime(2026, 7, 25, 2, tzinfo=UTC)
DATASET = "bigquery-public-data.crypto_bitcoin"
SOURCE_SCHEMA_SHA256 = (
    "7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7"
)


def _write_exact_population_receipts(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    return _write_pair(
        root,
        monkeypatch,
        v1=_v1_receipt(output_defined_count=1_557_941_780),
        v2=_v2_receipt(
            positive_value_count=1_531_420_608,
            p0_count=21_736,
            p1_count=2_143,
            edge_count=133_730,
            coarse_count=1_090_411,
        ),
    )


def _request(
    *,
    expected_jobs: int = 6,
    expected_billed_bytes: int = 1_476_768_301_056,
    budget_bytes: int = 2_400_000_000_000,
) -> StrictV2SMaterializationExecutionRequest:
    return StrictV2SMaterializationExecutionRequest(
        authorization_id=STRICT_V2_S_AUTHORIZATION_ID,
        billing_acknowledged=True,
        destination_table_id=STRICT_V2_S_DESTINATION_TABLE_ID,
        expected_query_sha256=PINNED_STRICT_V2_S_QUERY_SHA256,
        expected_result_schema_sha256=(
            STRICT_V2_S_CANDIDATE_SCHEMA_SHA256
        ),
        expected_source_schema_sha256=SOURCE_SCHEMA_SHA256,
        expected_dry_run_bytes=STRICT_V2_S_EXPECTED_DRY_RUN_BYTES,
        expected_successful_query_jobs=expected_jobs,
        expected_month_to_date_billed_bytes=expected_billed_bytes,
        maximum_bytes_billed=650_000_000_000,
        monthly_processing_budget_bytes=budget_bytes,
        reserve_bytes=250_000_000_000,
        expected_candidate_rows=EXPECTED_STRICT_V2_S_COARSE_COUNT,
        destination_expiration_hours=168,
    )


class FakeMaterializationBackend:
    def __init__(
        self,
        *,
        dry_run_bytes: int = STRICT_V2_S_EXPECTED_DRY_RUN_BYTES,
        successful_query_jobs: int = 6,
        billed_bytes: int = 1_476_768_301_056,
        result_rows: int = EXPECTED_STRICT_V2_S_COARSE_COUNT,
        preparation_error: bool = False,
        execution_error: bool = False,
    ) -> None:
        self.dry_run_bytes = dry_run_bytes
        self.successful_query_jobs = successful_query_jobs
        self.billed_bytes = billed_bytes
        self.result_rows = result_rows
        self.preparation_error = preparation_error
        self.execution_error = execution_error
        self.calls: list[str] = []
        self.destination: CandidateDestinationMetadata | None = None
        self.execution_calls = 0
        self.fetch_calls = 0

    def table_metadata(self, table_id: str):
        self.calls.append("table_metadata")
        assert table_id == "bigquery-public-data.crypto_bitcoin.transactions"
        return PinnedV2TableMetadata()

    def monthly_successful_query_usage(
        self,
        *,
        month_start: datetime,
        month_end: datetime,
    ) -> MonthlyQueryUsage:
        self.calls.append("monthly_successful_query_usage")
        return MonthlyQueryUsage(
            successful_query_jobs=self.successful_query_jobs,
            total_bytes_billed=self.billed_bytes,
        )

    def dry_run(
        self,
        sql: str,
        parameters: dict[str, object],
        maximum_bytes_billed: int,
    ) -> QueryEstimate:
        self.calls.append("dry_run")
        assert "btc_strict_v2_s_candidate_materialization" in sql
        assert maximum_bytes_billed == 0
        return QueryEstimate(
            total_bytes_processed=self.dry_run_bytes,
            cache_hit=False,
        )

    def prepare_destination_table_no_retry(
        self,
        *,
        table_id: str,
        fields: tuple[object, ...],
        expires_at: datetime,
    ) -> CandidateDestinationMetadata:
        self.calls.append("prepare_destination")
        if self.preparation_error:
            raise RuntimeError("sanitized fake preparation failure")
        assert table_id == STRICT_V2_S_DESTINATION_TABLE_ID
        assert fields == STRICT_V2_S_CANDIDATE_SCHEMA
        self.destination = CandidateDestinationMetadata(
            table_id=table_id,
            result_schema_sha256=STRICT_V2_S_CANDIDATE_SCHEMA_SHA256,
            row_count=0,
            expires_at=expires_at,
        )
        return self.destination

    def execute_query_to_destination_no_retry(
        self,
        sql: str,
        parameters: dict[str, object],
        *,
        maximum_bytes_billed: int,
        job_id: str,
        destination_table_id: str,
    ) -> StrictV2SCloudExecution:
        self.calls.append("execute_query_to_destination")
        self.execution_calls += 1
        if self.execution_error:
            raise RuntimeError("sanitized fake failure")
        self.destination = CandidateDestinationMetadata(
            table_id=destination_table_id,
            result_schema_sha256=STRICT_V2_S_CANDIDATE_SCHEMA_SHA256,
            row_count=self.result_rows,
            expires_at=NOW + timedelta(hours=168),
        )
        return StrictV2SCloudExecution(
            job_id=job_id,
            destination_table_id=destination_table_id,
            result_schema_sha256=STRICT_V2_S_CANDIDATE_SCHEMA_SHA256,
            row_count=self.result_rows,
            total_bytes_processed=STRICT_V2_S_EXPECTED_DRY_RUN_BYTES,
            total_bytes_billed=STRICT_V2_S_EXPECTED_DRY_RUN_BYTES,
        )

    def fetch_existing_destination_job_no_retry(
        self,
        *,
        job_id: str,
        destination_table_id: str,
        timeout_seconds: float,
    ) -> StrictV2SCloudExecution:
        self.calls.append("fetch_existing_destination_job")
        self.fetch_calls += 1
        return StrictV2SCloudExecution(
            job_id=job_id,
            destination_table_id=destination_table_id,
            result_schema_sha256=STRICT_V2_S_CANDIDATE_SCHEMA_SHA256,
            row_count=self.result_rows,
            total_bytes_processed=STRICT_V2_S_EXPECTED_DRY_RUN_BYTES,
            total_bytes_billed=STRICT_V2_S_EXPECTED_DRY_RUN_BYTES,
        )


def test_execution_request_is_exactly_pinned() -> None:
    request = _request()

    assert request.authorization_id == STRICT_V2_S_AUTHORIZATION_ID
    assert request.destination_table_id == STRICT_V2_S_DESTINATION_TABLE_ID
    assert request.expected_candidate_rows == 1_090_411
    assert request.maximum_bytes_billed == 650_000_000_000
    assert request.destination_expiration_hours == 168

    with pytest.raises(ValueError, match="destination table"):
        _request().model_copy(
            update={
                "destination_table_id": (
                    "other-project.private.candidates"
                )
            }
        ).model_validate(
            {
                **_request().model_dump(),
                "destination_table_id": "other-project.private.candidates",
            }
        )


def test_destination_schema_normalizes_bigquery_integer_alias() -> None:
    api_fields = tuple(
        type(field)(
            name=field.name,
            bigquery_type=(
                "INTEGER"
                if field.bigquery_type == "INT64"
                else field.bigquery_type
            ),
            mode=field.mode,
        )
        for field in STRICT_V2_S_CANDIDATE_SCHEMA
    )

    assert candidate_destination_schema_sha256(api_fields) == (
        STRICT_V2_S_CANDIDATE_SCHEMA_SHA256
    )


def test_execution_preview_is_offline_and_writes_nothing(
    tmp_path: Path,
) -> None:
    outcome = preview_strict_v2_s_materialization_execution(
        _request(),
        dataset=DATASET,
        receipt_root=tmp_path,
    )

    assert outcome.status == "dry_run"
    assert outcome.execution_calls == 0
    assert outcome.network_requests == 0
    assert outcome.receipt_created is False
    assert outcome.candidate_materialized is False
    assert outcome.written_paths == ()
    assert list(tmp_path.iterdir()) == []


def test_execution_preflight_drift_blocks_before_receipt_or_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt_root = _write_exact_population_receipts(tmp_path, monkeypatch)
    backend = FakeMaterializationBackend(dry_run_bytes=1)

    outcome = StrictV2SMaterializationOneShotExecutor(
        backend=backend,
        dataset=DATASET,
        receipt_root=receipt_root,
        max_source_age=timedelta(hours=48),
        now=NOW,
    ).run(_request())

    assert outcome.status == "preflight_blocked"
    assert "strict_v2_s_execution_dry_run_bytes_mismatch" in (
        outcome.blocking_reasons
    )
    assert outcome.execution_calls == 0
    assert outcome.receipt_created is False
    assert "prepare_destination" not in backend.calls
    assert "execute_query_to_destination" not in backend.calls


def test_execution_writes_exact_destination_once_and_seals_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt_root = _write_exact_population_receipts(tmp_path, monkeypatch)
    backend = FakeMaterializationBackend()
    executor = StrictV2SMaterializationOneShotExecutor(
        backend=backend,
        dataset=DATASET,
        receipt_root=receipt_root,
        max_source_age=timedelta(hours=48),
        now=NOW,
    )

    outcome = executor.run(_request())

    assert outcome.status == "completed"
    assert outcome.destination_table_id == STRICT_V2_S_DESTINATION_TABLE_ID
    assert outcome.candidate_rows == 1_090_411
    assert outcome.execution_calls == 1
    assert outcome.automatic_retries == 0
    assert outcome.candidate_materialized is True
    assert outcome.provider_requests == 0
    assert outcome.provider_points == 0
    assert backend.execution_calls == 1
    assert backend.calls == [
        "table_metadata",
        "monthly_successful_query_usage",
        "dry_run",
        "prepare_destination",
        "execute_query_to_destination",
    ]

    receipt_path = Path(outcome.receipt_path)
    assert stat.S_IMODE(receipt_path.stat().st_mode) == 0o600
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["status"] == "completed"
    assert receipt["candidate_rows"] == 1_090_411
    assert receipt["candidate_materialized"] is True
    assert receipt["billing_acknowledged"] is True
    assert (
        receipt["expected_dry_run_bytes"]
        == STRICT_V2_S_EXPECTED_DRY_RUN_BYTES
    )
    assert receipt["maximum_bytes_billed"] == 650_000_000_000
    assert receipt["monthly_processing_budget_bytes"] == 2_400_000_000_000
    assert receipt["reserve_bytes"] == 250_000_000_000
    assert "normalized_address" not in receipt

    with pytest.raises(StrictV2SMaterializationAlreadyAttempted):
        executor.run(_request())
    assert backend.execution_calls == 1


def test_wrong_destination_row_count_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt_root = _write_exact_population_receipts(tmp_path, monkeypatch)
    backend = FakeMaterializationBackend(result_rows=1_090_410)

    outcome = StrictV2SMaterializationOneShotExecutor(
        backend=backend,
        dataset=DATASET,
        receipt_root=receipt_root,
        max_source_age=timedelta(hours=48),
        now=NOW,
    ).run(_request())

    assert outcome.status == "quality_blocked"
    assert outcome.candidate_materialized is False
    assert "strict_v2_s_destination_row_count_mismatch" in (
        outcome.blocking_reasons
    )
    assert backend.execution_calls == 1


def test_destination_preparation_failure_cannot_claim_query_submission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt_root = _write_exact_population_receipts(tmp_path, monkeypatch)
    backend = FakeMaterializationBackend(preparation_error=True)

    outcome = StrictV2SMaterializationOneShotExecutor(
        backend=backend,
        dataset=DATASET,
        receipt_root=receipt_root,
        max_source_age=timedelta(hours=48),
        now=NOW,
    ).run(_request())

    assert outcome.status == "preparation_failed"
    assert outcome.execution_calls == 0
    assert outcome.candidate_materialized is False
    assert backend.execution_calls == 0
    assert "execute_query_to_destination" not in backend.calls
    receipt = json.loads(
        Path(outcome.receipt_path).read_text(encoding="utf-8")
    )
    assert receipt["status"] == "preparation_failed"
    assert receipt["automatic_retries"] == 0


def test_submission_failure_is_reconciled_without_resubmission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt_root = _write_exact_population_receipts(tmp_path, monkeypatch)
    backend = FakeMaterializationBackend(execution_error=True)
    executor = StrictV2SMaterializationOneShotExecutor(
        backend=backend,
        dataset=DATASET,
        receipt_root=receipt_root,
        max_source_age=timedelta(hours=48),
        now=NOW,
    )

    failed = executor.run(_request())
    assert failed.status == "submission_unknown"
    assert backend.execution_calls == 1

    backend.execution_error = False
    recovered = executor.reconcile_existing_job(
        _request(),
        timeout_seconds=30,
    )

    assert recovered.status == "completed"
    assert recovered.reconciled_existing_job is True
    assert backend.execution_calls == 1
    assert backend.fetch_calls == 1
