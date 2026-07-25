"""One-shot cloud materialization for the frozen BTC Strict V2-S policy."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, Protocol

from pydantic import Field, field_validator, model_validator

from crypto_address_identity.universe.bigquery import (
    BigQueryBoundaryError,
    GoogleBigQueryBackend,
)
from crypto_address_identity.universe.candidate_materialization_v2_s import (
    EXPECTED_STRICT_V2_S_COARSE_COUNT,
    PINNED_STRICT_V2_S_QUERY_SHA256,
    STRICT_V2_S_CANDIDATE_SCHEMA,
    STRICT_V2_S_CANDIDATE_SCHEMA_SHA256,
    STRICT_V2_S_CUTOFF_HEIGHT,
    STRICT_V2_S_CUTOFF_TIME,
    STRICT_V2_S_MAXIMUM_BYTES_BILLED,
    STRICT_V2_S_RESULT_SCHEMA_VERSION,
    BigQueryStrictV2SMaterializationCostProbe,
    CandidateSchemaField,
    StrictV2SMaterializationQueryPlan,
)
from crypto_address_identity.universe.models import UniverseModel


STRICT_V2_S_AUTHORIZATION_ID = "btc-v2s-bootstrap-959187-one-shot"
STRICT_V2_S_DESTINATION_TABLE_ID = (
    "cai-btc-universe-20260724.cai_private."
    "btc_strict_v2_s_candidates_959187"
)
STRICT_V2_S_EXPECTED_DRY_RUN_BYTES = 638_112_721_818
STRICT_V2_S_DESTINATION_EXPIRATION_HOURS = 168
STRICT_V2_S_EXECUTION_RECEIPT_VERSION = (
    "btc_strict_v2_s_materialization_execution_receipt_v1"
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_TABLE_ID_RE = re.compile(
    r"^[a-z][a-z0-9-]{4,62}\.[A-Za-z_][A-Za-z0-9_]{0,1023}\."
    r"[A-Za-z_][A-Za-z0-9_]{0,1023}$"
)
_BIGQUERY_TYPE_ALIASES = {
    "BOOLEAN": "BOOL",
    "FLOAT": "FLOAT64",
    "INTEGER": "INT64",
    "RECORD": "STRUCT",
}


class StrictV2SMaterializationAlreadyAttempted(RuntimeError):
    """Raised before network use when the one-shot receipt already exists."""


class StrictV2SMaterializationReceiptInvalid(RuntimeError):
    """Raised when an existing receipt cannot safely reconcile its job."""


class CandidateDestinationMetadata(UniverseModel):
    table_id: str
    result_schema_sha256: str
    row_count: int = Field(ge=0)
    expires_at: datetime

    @field_validator("result_schema_sha256")
    @classmethod
    def validate_hash(cls, value: str) -> str:
        if not _SHA256_RE.fullmatch(value):
            raise ValueError("destination schema hash must be SHA-256")
        return value

    @field_validator("expires_at")
    @classmethod
    def validate_expiry(cls, value: datetime) -> datetime:
        return _as_utc(value)


class StrictV2SCloudExecution(UniverseModel):
    job_id: str
    destination_table_id: str
    result_schema_sha256: str
    row_count: int = Field(ge=0)
    total_bytes_processed: int = Field(ge=0)
    total_bytes_billed: int = Field(ge=0)

    @field_validator("result_schema_sha256")
    @classmethod
    def validate_hash(cls, value: str) -> str:
        if not _SHA256_RE.fullmatch(value):
            raise ValueError("execution schema hash must be SHA-256")
        return value


class StrictV2SMaterializationBackend(Protocol):
    def table_metadata(self, table_id: str): ...

    def monthly_successful_query_usage(
        self,
        *,
        month_start: datetime,
        month_end: datetime,
    ): ...

    def dry_run(
        self,
        sql: str,
        parameters: dict[str, object],
        maximum_bytes_billed: int,
    ): ...

    def prepare_destination_table_no_retry(
        self,
        *,
        table_id: str,
        fields: tuple[CandidateSchemaField, ...],
        expires_at: datetime,
    ) -> CandidateDestinationMetadata: ...

    def execute_query_to_destination_no_retry(
        self,
        sql: str,
        parameters: dict[str, object],
        *,
        maximum_bytes_billed: int,
        job_id: str,
        destination_table_id: str,
    ) -> StrictV2SCloudExecution: ...

    def fetch_existing_destination_job_no_retry(
        self,
        *,
        job_id: str,
        destination_table_id: str,
        timeout_seconds: float,
    ) -> StrictV2SCloudExecution: ...


class StrictV2SMaterializationExecutionRequest(UniverseModel):
    authorization_id: str
    billing_acknowledged: Literal[True]
    destination_table_id: str
    expected_query_sha256: str
    expected_result_schema_sha256: str
    expected_source_schema_sha256: str
    expected_dry_run_bytes: int = Field(gt=0)
    expected_successful_query_jobs: int = Field(ge=0)
    expected_month_to_date_billed_bytes: int = Field(ge=0)
    maximum_bytes_billed: int = Field(gt=0)
    monthly_processing_budget_bytes: int = Field(gt=0)
    reserve_bytes: int = Field(ge=0)
    expected_candidate_rows: int = Field(gt=0)
    destination_expiration_hours: int = Field(gt=0)

    @field_validator(
        "expected_query_sha256",
        "expected_result_schema_sha256",
        "expected_source_schema_sha256",
    )
    @classmethod
    def validate_hash(cls, value: str) -> str:
        if not _SHA256_RE.fullmatch(value):
            raise ValueError("execution checksum must be lower-case SHA-256")
        return value

    @field_validator("destination_table_id")
    @classmethod
    def validate_table_id(cls, value: str) -> str:
        if not _TABLE_ID_RE.fullmatch(value):
            raise ValueError("destination table id is invalid")
        return value

    @model_validator(mode="after")
    def validate_frozen_contract(
        self,
    ) -> "StrictV2SMaterializationExecutionRequest":
        exact_values = (
            (
                self.authorization_id,
                STRICT_V2_S_AUTHORIZATION_ID,
                "authorization id",
            ),
            (
                self.destination_table_id,
                STRICT_V2_S_DESTINATION_TABLE_ID,
                "destination table",
            ),
            (
                self.expected_query_sha256,
                PINNED_STRICT_V2_S_QUERY_SHA256,
                "query checksum",
            ),
            (
                self.expected_result_schema_sha256,
                STRICT_V2_S_CANDIDATE_SCHEMA_SHA256,
                "result schema checksum",
            ),
            (
                self.expected_dry_run_bytes,
                STRICT_V2_S_EXPECTED_DRY_RUN_BYTES,
                "dry-run bytes",
            ),
            (
                self.maximum_bytes_billed,
                STRICT_V2_S_MAXIMUM_BYTES_BILLED,
                "billing cap",
            ),
            (
                self.expected_candidate_rows,
                EXPECTED_STRICT_V2_S_COARSE_COUNT,
                "candidate rows",
            ),
            (
                self.destination_expiration_hours,
                STRICT_V2_S_DESTINATION_EXPIRATION_HOURS,
                "destination expiration",
            ),
        )
        for actual, expected, label in exact_values:
            if actual != expected:
                raise ValueError(
                    f"Strict V2-S execution {label} is not authorized"
                )
        if self.expected_dry_run_bytes > self.maximum_bytes_billed:
            raise ValueError("Strict V2-S dry run exceeds the billing cap")
        projected = (
            self.expected_month_to_date_billed_bytes
            + self.expected_dry_run_bytes
        )
        if (
            self.reserve_bytes >= self.monthly_processing_budget_bytes
            or projected
            > self.monthly_processing_budget_bytes - self.reserve_bytes
        ):
            raise ValueError(
                "Strict V2-S execution exceeds the monthly byte budget"
            )
        return self


class StrictV2SMaterializationExecutionOutcome(UniverseModel):
    status: Literal[
        "dry_run",
        "preflight_blocked",
        "preparation_failed",
        "completed",
        "quality_blocked",
        "submission_unknown",
    ]
    authorization_id: str
    destination_table_id: str
    query_sha256: str
    result_schema_sha256: str
    source_schema_sha256: str
    cutoff_height: Literal[959_187] = STRICT_V2_S_CUTOFF_HEIGHT
    cutoff_time: datetime = STRICT_V2_S_CUTOFF_TIME
    expected_candidate_rows: Literal[1_090_411] = (
        EXPECTED_STRICT_V2_S_COARSE_COUNT
    )
    candidate_rows: int = Field(default=0, ge=0)
    job_id: str
    receipt_path: str
    receipt_created: bool
    execution_calls: int = Field(default=0, ge=0, le=1)
    network_requests: int = Field(default=0, ge=0)
    automatic_retries: Literal[0] = 0
    candidate_materialized: bool = False
    reconciled_existing_job: bool = False
    total_bytes_processed: int | None = Field(default=None, ge=0)
    total_bytes_billed: int | None = Field(default=None, ge=0)
    blocking_reasons: tuple[str, ...] = ()
    provider_requests: Literal[0] = 0
    provider_points: Literal[0] = 0
    written_paths: tuple[str, ...] = ()

    @field_validator("cutoff_time")
    @classmethod
    def validate_time(cls, value: datetime) -> datetime:
        return _as_utc(value)

    @field_validator(
        "query_sha256",
        "result_schema_sha256",
        "source_schema_sha256",
    )
    @classmethod
    def validate_hash(cls, value: str) -> str:
        if not _SHA256_RE.fullmatch(value):
            raise ValueError("outcome checksum must be SHA-256")
        return value

    @field_validator("blocking_reasons")
    @classmethod
    def canonicalize_reasons(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted(set(value)))

    @model_validator(mode="after")
    def validate_state(self) -> "StrictV2SMaterializationExecutionOutcome":
        if self.status in {"dry_run", "preflight_blocked"}:
            if self.receipt_created or self.execution_calls:
                raise ValueError(
                    "non-executing materialization cannot create a receipt"
                )
        elif self.status == "preparation_failed":
            if (
                not self.receipt_created
                or self.execution_calls
                or self.written_paths != (self.receipt_path,)
            ):
                raise ValueError(
                    "failed preparation requires one sealed receipt"
                )
        else:
            if (
                not self.receipt_created
                or self.execution_calls != 1
                or self.written_paths != (self.receipt_path,)
            ):
                raise ValueError(
                    "executing materialization requires one sealed receipt"
                )
        if self.status == "completed" and (
            self.candidate_rows != self.expected_candidate_rows
            or not self.candidate_materialized
            or self.blocking_reasons
        ):
            raise ValueError(
                "completed materialization requires exact candidate rows"
            )
        if self.status != "completed" and self.candidate_materialized:
            raise ValueError(
                "non-completed materialization cannot claim candidates"
            )
        return self


def preview_strict_v2_s_materialization_execution(
    request: StrictV2SMaterializationExecutionRequest,
    *,
    dataset: str,
    receipt_root: Path,
) -> StrictV2SMaterializationExecutionOutcome:
    plan = StrictV2SMaterializationQueryPlan.load(dataset)
    _validate_plan(plan, request)
    receipt_path = _receipt_path(receipt_root, request.authorization_id)
    return _outcome(
        status="dry_run",
        request=request,
        plan=plan,
        receipt_path=receipt_path,
    )


class StrictV2SMaterializationOneShotExecutor:
    """Create one private destination and submit one no-retry source query."""

    def __init__(
        self,
        *,
        backend: StrictV2SMaterializationBackend,
        dataset: str,
        receipt_root: Path,
        max_source_age: timedelta,
        now: datetime | None = None,
    ) -> None:
        if max_source_age <= timedelta(0):
            raise ValueError("max_source_age must be positive")
        self._backend = backend
        self._plan = StrictV2SMaterializationQueryPlan.load(dataset)
        self._receipt_root = receipt_root
        self._max_source_age = max_source_age
        self._now = _as_utc(now or datetime.now(UTC))

    def run(
        self,
        request: StrictV2SMaterializationExecutionRequest,
    ) -> StrictV2SMaterializationExecutionOutcome:
        _validate_plan(self._plan, request)
        receipt_path = _receipt_path(
            self._receipt_root,
            request.authorization_id,
        )
        if receipt_path.exists():
            raise StrictV2SMaterializationAlreadyAttempted()

        cost = BigQueryStrictV2SMaterializationCostProbe(
            backend=self._backend,
            dataset=self._plan.dataset,
            receipt_root=self._receipt_root,
            max_source_age=self._max_source_age,
            now=self._now,
        ).run(
            expected_query_sha256=request.expected_query_sha256,
            expected_result_schema_sha256=(
                request.expected_result_schema_sha256
            ),
            monthly_processing_budget_bytes=(
                request.monthly_processing_budget_bytes
            ),
            reserve_bytes=request.reserve_bytes,
        )
        reasons = list(cost.blocking_reasons)
        if cost.status != "checkpoint_passed":
            reasons.append("strict_v2_s_execution_cost_gate_blocked")
            return _outcome(
                status="preflight_blocked",
                request=request,
                plan=self._plan,
                receipt_path=receipt_path,
                network_requests=cost.network_requests,
                blocking_reasons=tuple(reasons),
            )
        comparisons = (
            (
                cost.source_schema_sha256,
                request.expected_source_schema_sha256,
                "strict_v2_s_execution_source_schema_mismatch",
            ),
            (
                cost.dry_run_bytes,
                request.expected_dry_run_bytes,
                "strict_v2_s_execution_dry_run_bytes_mismatch",
            ),
            (
                cost.successful_query_jobs,
                request.expected_successful_query_jobs,
                "strict_v2_s_execution_job_count_mismatch",
            ),
            (
                cost.month_to_date_billed_bytes,
                request.expected_month_to_date_billed_bytes,
                "strict_v2_s_execution_monthly_bytes_mismatch",
            ),
        )
        for actual, expected, reason in comparisons:
            if actual != expected:
                reasons.append(reason)
        if reasons:
            return _outcome(
                status="preflight_blocked",
                request=request,
                plan=self._plan,
                receipt_path=receipt_path,
                network_requests=cost.network_requests,
                blocking_reasons=tuple(reasons),
            )

        job_id = _job_id(request)
        expires_at = self._now + timedelta(
            hours=request.destination_expiration_hours
        )
        _create_receipt_exclusive(
            receipt_path,
            _receipt_payload(
                status="started",
                request=request,
                plan=self._plan,
                job_id=job_id,
                created_at=self._now,
                expires_at=expires_at,
            ),
        )
        try:
            destination = self._backend.prepare_destination_table_no_retry(
                table_id=request.destination_table_id,
                fields=STRICT_V2_S_CANDIDATE_SCHEMA,
                expires_at=expires_at,
            )
            _validate_destination(destination, request, require_empty=True)
        except Exception:
            outcome = _outcome(
                status="preparation_failed",
                request=request,
                plan=self._plan,
                receipt_path=receipt_path,
                receipt_created=True,
                network_requests=cost.network_requests + 1,
                blocking_reasons=(
                    "strict_v2_s_destination_preparation_failed",
                ),
                written_paths=(str(receipt_path),),
            )
            _replace_receipt(
                receipt_path,
                _terminal_receipt_payload(
                    outcome=outcome,
                    request=request,
                    created_at=self._now,
                    expires_at=expires_at,
                ),
            )
            return outcome

        try:
            execution = (
                self._backend.execute_query_to_destination_no_retry(
                    self._plan.sql,
                    {
                        "cutoff_height": STRICT_V2_S_CUTOFF_HEIGHT,
                        "cutoff_time": STRICT_V2_S_CUTOFF_TIME,
                    },
                    maximum_bytes_billed=request.maximum_bytes_billed,
                    job_id=job_id,
                    destination_table_id=request.destination_table_id,
                )
            )
        except Exception:
            outcome = _outcome(
                status="submission_unknown",
                request=request,
                plan=self._plan,
                receipt_path=receipt_path,
                receipt_created=True,
                execution_calls=1,
                network_requests=cost.network_requests + 2,
                blocking_reasons=(
                    "strict_v2_s_materialization_submission_unknown",
                ),
                written_paths=(str(receipt_path),),
            )
            _replace_receipt(
                receipt_path,
                _terminal_receipt_payload(
                    outcome=outcome,
                    request=request,
                    created_at=self._now,
                    expires_at=expires_at,
                ),
            )
            return outcome

        outcome = self._execution_outcome(
            execution=execution,
            request=request,
            receipt_path=receipt_path,
            network_requests=cost.network_requests + 2,
        )
        _replace_receipt(
            receipt_path,
            _terminal_receipt_payload(
                outcome=outcome,
                request=request,
                created_at=self._now,
                expires_at=expires_at,
            ),
        )
        return outcome

    def reconcile_existing_job(
        self,
        request: StrictV2SMaterializationExecutionRequest,
        *,
        timeout_seconds: float,
    ) -> StrictV2SMaterializationExecutionOutcome:
        _validate_plan(self._plan, request)
        receipt_path = _receipt_path(
            self._receipt_root,
            request.authorization_id,
        )
        receipt = _read_reconcilable_receipt(receipt_path, request)
        execution = self._backend.fetch_existing_destination_job_no_retry(
            job_id=str(receipt["job_id"]),
            destination_table_id=request.destination_table_id,
            timeout_seconds=timeout_seconds,
        )
        outcome = self._execution_outcome(
            execution=execution,
            request=request,
            receipt_path=receipt_path,
            network_requests=1,
            reconciled_existing_job=True,
        )
        _replace_receipt(
            receipt_path,
            _terminal_receipt_payload(
                outcome=outcome,
                request=request,
                created_at=_as_utc(
                    datetime.fromisoformat(
                        str(receipt["created_at"]).replace("Z", "+00:00")
                    )
                ),
                expires_at=_as_utc(
                    datetime.fromisoformat(
                        str(receipt["expires_at"]).replace("Z", "+00:00")
                    )
                ),
            ),
        )
        return outcome

    def _execution_outcome(
        self,
        *,
        execution: StrictV2SCloudExecution,
        request: StrictV2SMaterializationExecutionRequest,
        receipt_path: Path,
        network_requests: int,
        reconciled_existing_job: bool = False,
    ) -> StrictV2SMaterializationExecutionOutcome:
        reasons: list[str] = []
        if execution.destination_table_id != request.destination_table_id:
            reasons.append("strict_v2_s_destination_table_mismatch")
        if (
            execution.result_schema_sha256
            != request.expected_result_schema_sha256
        ):
            reasons.append("strict_v2_s_destination_schema_mismatch")
        if execution.row_count != request.expected_candidate_rows:
            reasons.append("strict_v2_s_destination_row_count_mismatch")
        if execution.total_bytes_processed > request.maximum_bytes_billed:
            reasons.append("strict_v2_s_execution_bytes_exceeded")
        status = "quality_blocked" if reasons else "completed"
        return _outcome(
            status=status,
            request=request,
            plan=self._plan,
            receipt_path=receipt_path,
            receipt_created=True,
            execution_calls=1,
            network_requests=network_requests,
            candidate_rows=execution.row_count,
            candidate_materialized=not reasons,
            reconciled_existing_job=reconciled_existing_job,
            total_bytes_processed=execution.total_bytes_processed,
            total_bytes_billed=execution.total_bytes_billed,
            blocking_reasons=tuple(reasons),
            written_paths=(str(receipt_path),),
        )


class GoogleBigQueryStrictV2SMaterializationBackend(
    GoogleBigQueryBackend
):
    """Google SDK adapter for one private destination and later extraction."""

    def prepare_destination_table_no_retry(
        self,
        *,
        table_id: str,
        fields: tuple[CandidateSchemaField, ...],
        expires_at: datetime,
    ) -> CandidateDestinationMetadata:
        if table_id != STRICT_V2_S_DESTINATION_TABLE_ID:
            raise BigQueryBoundaryError(
                "BigQuery destination table is not authorized"
            )
        dataset_id = table_id.rsplit(".", 1)[0]
        try:
            dataset = self._client.get_dataset(dataset_id)
            for entry in getattr(dataset, "access_entries", ()):
                if (
                    getattr(entry, "entity_type", None) == "specialGroup"
                    and getattr(entry, "entity_id", None)
                    in {"allAuthenticatedUsers", "allUsers"}
                ):
                    raise BigQueryBoundaryError(
                        "BigQuery destination dataset is public"
                    )
            table = self._bigquery.Table(
                table_id,
                schema=[
                    self._bigquery.SchemaField(
                        field.name,
                        field.bigquery_type,
                        mode=field.mode,
                    )
                    for field in fields
                ],
            )
            table.expires = _as_utc(expires_at)
            created = self._client.create_table(
                table,
                exists_ok=False,
                retry=None,
            )
        except BigQueryBoundaryError:
            raise
        except Exception as exc:
            raise BigQueryBoundaryError(
                "BigQuery destination preparation failed"
            ) from exc
        return self._destination_metadata(created)

    def execute_query_to_destination_no_retry(
        self,
        sql: str,
        parameters: dict[str, object],
        *,
        maximum_bytes_billed: int,
        job_id: str,
        destination_table_id: str,
    ) -> StrictV2SCloudExecution:
        if (
            maximum_bytes_billed != STRICT_V2_S_MAXIMUM_BYTES_BILLED
            or destination_table_id != STRICT_V2_S_DESTINATION_TABLE_ID
        ):
            raise BigQueryBoundaryError(
                "BigQuery materialization contract is invalid"
            )
        job_config = self._query_job_config(
            parameters,
            maximum_bytes_billed=maximum_bytes_billed,
            dry_run=False,
        )
        job_config.destination = destination_table_id
        job_config.write_disposition = (
            self._bigquery.WriteDisposition.WRITE_EMPTY
        )
        job_config.create_disposition = (
            self._bigquery.CreateDisposition.CREATE_NEVER
        )
        job_config.labels = {
            "cai_workflow": "btc_v2s_bootstrap",
            "cai_policy": "v2_s",
        }
        try:
            job = self._client.query(
                sql,
                job_config=job_config,
                job_id=job_id,
                retry=None,
                job_retry=None,
            )
            job.result(retry=None, job_retry=None)
            table = self._client.get_table(
                destination_table_id,
                retry=None,
            )
        except Exception as exc:
            raise BigQueryBoundaryError(
                "BigQuery materialization submission is unresolved"
            ) from exc
        return self._cloud_execution(job=job, table=table)

    def fetch_existing_destination_job_no_retry(
        self,
        *,
        job_id: str,
        destination_table_id: str,
        timeout_seconds: float,
    ) -> StrictV2SCloudExecution:
        if (
            destination_table_id != STRICT_V2_S_DESTINATION_TABLE_ID
            or timeout_seconds <= 0
            or timeout_seconds > 300
        ):
            raise BigQueryBoundaryError(
                "BigQuery reconciliation contract is invalid"
            )
        try:
            job = self._client.get_job(job_id)
            if (
                str(getattr(job, "state", "")).upper() != "DONE"
                or getattr(job, "error_result", None) is not None
                or str(getattr(job, "destination", ""))
                != destination_table_id
            ):
                raise BigQueryBoundaryError(
                    "BigQuery materialization job is not reconcilable"
                )
            job.result(
                timeout=timeout_seconds,
                retry=None,
                job_retry=None,
            )
            table = self._client.get_table(
                destination_table_id,
                retry=None,
            )
        except BigQueryBoundaryError:
            raise
        except Exception as exc:
            raise BigQueryBoundaryError(
                "BigQuery materialization reconciliation failed"
            ) from exc
        return self._cloud_execution(job=job, table=table)

    def destination_metadata(self, table_id: str) -> dict[str, object]:
        try:
            table = self._client.get_table(table_id, retry=None)
        except Exception as exc:
            raise BigQueryBoundaryError(
                "BigQuery destination metadata is unavailable"
            ) from exc
        metadata = self._destination_metadata(table)
        return metadata.model_dump(mode="python")

    def stream_destination_arrow_batches(
        self,
        *,
        table_id: str,
        page_size: int,
    ):
        if table_id != STRICT_V2_S_DESTINATION_TABLE_ID or page_size <= 0:
            raise BigQueryBoundaryError(
                "BigQuery destination stream bounds are invalid"
            )
        try:
            table = self._client.get_table(table_id, retry=None)
            rows = self._client.list_rows(
                table,
                page_size=page_size,
                retry=None,
            )
            yield from rows.to_arrow_iterable()
        except Exception as exc:
            raise BigQueryBoundaryError(
                "BigQuery destination Arrow stream failed"
            ) from exc

    def _destination_metadata(
        self,
        table: object,
    ) -> CandidateDestinationMetadata:
        fields = tuple(
            CandidateSchemaField(
                name=str(field.name),
                bigquery_type=_canonical_bigquery_type(
                    str(field.field_type)
                ),
                mode=str(field.mode).upper(),
            )
            for field in getattr(table, "schema", ())
        )
        schema_hash = candidate_destination_schema_sha256(fields)
        expires = getattr(table, "expires", None)
        if expires is None:
            raise BigQueryBoundaryError(
                "BigQuery destination table must expire"
            )
        return CandidateDestinationMetadata(
            table_id=str(table.full_table_id).replace(":", ".", 1),
            result_schema_sha256=schema_hash,
            row_count=int(getattr(table, "num_rows", 0) or 0),
            expires_at=expires,
        )

    def _cloud_execution(
        self,
        *,
        job: object,
        table: object,
    ) -> StrictV2SCloudExecution:
        metadata = self._destination_metadata(table)
        return StrictV2SCloudExecution(
            job_id=str(getattr(job, "job_id", "")),
            destination_table_id=metadata.table_id,
            result_schema_sha256=metadata.result_schema_sha256,
            row_count=metadata.row_count,
            total_bytes_processed=int(
                getattr(job, "total_bytes_processed", 0) or 0
            ),
            total_bytes_billed=int(
                getattr(job, "total_bytes_billed", 0) or 0
            ),
        )


def candidate_destination_schema_sha256(
    fields: tuple[CandidateSchemaField, ...],
) -> str:
    normalized = tuple(
        CandidateSchemaField(
            name=field.name,
            bigquery_type=_canonical_bigquery_type(field.bigquery_type),
            mode=field.mode.upper(),
        )
        for field in fields
    )
    payload = {
        "schema_version": STRICT_V2_S_RESULT_SCHEMA_VERSION,
        "fields": [
            {
                "name": field.name,
                "bigquery_type": field.bigquery_type,
                "mode": field.mode,
            }
            for field in normalized
        ],
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
    ).hexdigest()


def _canonical_bigquery_type(value: str) -> str:
    normalized = value.upper()
    return _BIGQUERY_TYPE_ALIASES.get(normalized, normalized)


def _validate_plan(
    plan: StrictV2SMaterializationQueryPlan,
    request: StrictV2SMaterializationExecutionRequest,
) -> None:
    if (
        plan.query_sha256 != request.expected_query_sha256
        or plan.query_sha256 != PINNED_STRICT_V2_S_QUERY_SHA256
    ):
        raise ValueError("Strict V2-S execution query checksum drifted")
    if (
        request.expected_result_schema_sha256
        != STRICT_V2_S_CANDIDATE_SCHEMA_SHA256
    ):
        raise ValueError("Strict V2-S execution result schema drifted")


def _validate_destination(
    destination: CandidateDestinationMetadata,
    request: StrictV2SMaterializationExecutionRequest,
    *,
    require_empty: bool,
) -> None:
    if destination.table_id != request.destination_table_id:
        raise BigQueryBoundaryError("destination table id mismatch")
    if (
        destination.result_schema_sha256
        != request.expected_result_schema_sha256
    ):
        raise BigQueryBoundaryError("destination table schema mismatch")
    if require_empty and destination.row_count:
        raise BigQueryBoundaryError("destination table is not empty")


def _job_id(
    request: StrictV2SMaterializationExecutionRequest,
) -> str:
    payload = {
        "authorization_id": request.authorization_id,
        "destination_table_id": request.destination_table_id,
        "query_sha256": request.expected_query_sha256,
        "cutoff_height": STRICT_V2_S_CUTOFF_HEIGHT,
    }
    digest = hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    ).hexdigest()[:40]
    return f"cai_btc_v2s_{digest}"


def _receipt_path(receipt_root: Path, authorization_id: str) -> Path:
    return receipt_root / f"{authorization_id}.json"


def _receipt_payload(
    *,
    status: str,
    request: StrictV2SMaterializationExecutionRequest,
    plan: StrictV2SMaterializationQueryPlan,
    job_id: str,
    created_at: datetime,
    expires_at: datetime,
) -> dict[str, object]:
    return {
        "schema_version": STRICT_V2_S_EXECUTION_RECEIPT_VERSION,
        "status": status,
        "authorization_id": request.authorization_id,
        "job_id": job_id,
        "destination_table_id": request.destination_table_id,
        "created_at": _iso(created_at),
        "expires_at": _iso(expires_at),
        "billing_acknowledged": True,
        "query_sha256": plan.query_sha256,
        "result_schema_sha256": request.expected_result_schema_sha256,
        "source_schema_sha256": request.expected_source_schema_sha256,
        "expected_dry_run_bytes": request.expected_dry_run_bytes,
        "maximum_bytes_billed": request.maximum_bytes_billed,
        "expected_candidate_rows": request.expected_candidate_rows,
        "automatic_retries": 0,
        "candidate_materialized": False,
    }


def _terminal_receipt_payload(
    *,
    outcome: StrictV2SMaterializationExecutionOutcome,
    request: StrictV2SMaterializationExecutionRequest,
    created_at: datetime,
    expires_at: datetime,
) -> dict[str, object]:
    return {
        "schema_version": STRICT_V2_S_EXECUTION_RECEIPT_VERSION,
        "status": outcome.status,
        "authorization_id": outcome.authorization_id,
        "job_id": outcome.job_id,
        "destination_table_id": outcome.destination_table_id,
        "created_at": _iso(created_at),
        "expires_at": _iso(expires_at),
        "billing_acknowledged": True,
        "query_sha256": outcome.query_sha256,
        "result_schema_sha256": outcome.result_schema_sha256,
        "source_schema_sha256": outcome.source_schema_sha256,
        "expected_dry_run_bytes": request.expected_dry_run_bytes,
        "expected_successful_query_jobs": (
            request.expected_successful_query_jobs
        ),
        "expected_month_to_date_billed_bytes": (
            request.expected_month_to_date_billed_bytes
        ),
        "maximum_bytes_billed": request.maximum_bytes_billed,
        "monthly_processing_budget_bytes": (
            request.monthly_processing_budget_bytes
        ),
        "reserve_bytes": request.reserve_bytes,
        "destination_expiration_hours": (
            request.destination_expiration_hours
        ),
        "candidate_rows": outcome.candidate_rows,
        "expected_candidate_rows": outcome.expected_candidate_rows,
        "candidate_materialized": outcome.candidate_materialized,
        "reconciled_existing_job": outcome.reconciled_existing_job,
        "total_bytes_processed": outcome.total_bytes_processed,
        "total_bytes_billed": outcome.total_bytes_billed,
        "blocking_reasons": list(outcome.blocking_reasons),
        "automatic_retries": 0,
    }


def _create_receipt_exclusive(
    path: Path,
    payload: dict[str, object],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    encoded = _encode_receipt(payload)
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
    except Exception:
        try:
            path.unlink()
        except OSError:
            pass
        raise


def _replace_receipt(
    path: Path,
    payload: dict[str, object],
) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(_encode_receipt(payload))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _read_reconcilable_receipt(
    path: Path,
    request: StrictV2SMaterializationExecutionRequest,
) -> dict[str, object]:
    try:
        metadata = path.stat()
        if stat.S_IMODE(metadata.st_mode) != 0o600:
            raise StrictV2SMaterializationReceiptInvalid()
        payload = json.loads(path.read_text(encoding="utf-8"))
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version")
            != STRICT_V2_S_EXECUTION_RECEIPT_VERSION
            or payload.get("status") != "submission_unknown"
            or payload.get("authorization_id")
            != request.authorization_id
            or payload.get("destination_table_id")
            != request.destination_table_id
            or payload.get("query_sha256")
            != request.expected_query_sha256
        ):
            raise StrictV2SMaterializationReceiptInvalid()
        return payload
    except StrictV2SMaterializationReceiptInvalid:
        raise
    except Exception as exc:
        raise StrictV2SMaterializationReceiptInvalid() from exc


def _encode_receipt(payload: dict[str, object]) -> bytes:
    return (
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        + "\n"
    ).encode("ascii")


def _outcome(
    *,
    status: Literal[
        "dry_run",
        "preflight_blocked",
        "preparation_failed",
        "completed",
        "quality_blocked",
        "submission_unknown",
    ],
    request: StrictV2SMaterializationExecutionRequest,
    plan: StrictV2SMaterializationQueryPlan,
    receipt_path: Path,
    receipt_created: bool = False,
    execution_calls: int = 0,
    network_requests: int = 0,
    candidate_rows: int = 0,
    candidate_materialized: bool = False,
    reconciled_existing_job: bool = False,
    total_bytes_processed: int | None = None,
    total_bytes_billed: int | None = None,
    blocking_reasons: tuple[str, ...] = (),
    written_paths: tuple[str, ...] = (),
) -> StrictV2SMaterializationExecutionOutcome:
    return StrictV2SMaterializationExecutionOutcome(
        status=status,
        authorization_id=request.authorization_id,
        destination_table_id=request.destination_table_id,
        query_sha256=plan.query_sha256,
        result_schema_sha256=request.expected_result_schema_sha256,
        source_schema_sha256=request.expected_source_schema_sha256,
        candidate_rows=candidate_rows,
        job_id=_job_id(request),
        receipt_path=str(receipt_path),
        receipt_created=receipt_created,
        execution_calls=execution_calls,
        network_requests=network_requests,
        candidate_materialized=candidate_materialized,
        reconciled_existing_job=reconciled_existing_job,
        total_bytes_processed=total_bytes_processed,
        total_bytes_billed=total_bytes_billed,
        blocking_reasons=blocking_reasons,
        written_paths=written_paths,
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return _as_utc(value).isoformat().replace("+00:00", "Z")
