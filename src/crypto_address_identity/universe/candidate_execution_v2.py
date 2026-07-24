"""One-shot billed execution boundary for aggregate-only BTC importance v2."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator, model_validator

from crypto_address_identity.universe.bigquery import BigQueryBackend
from crypto_address_identity.universe.candidate_statistics_v2 import (
    PINNED_V2_CUTOFF_HEIGHT,
    PINNED_V2_SOURCE_INPUT_ONLY_ADDRESS_COUNT,
    PINNED_V2_SOURCE_STANDARD_ADDRESS_COUNT,
    BigQueryCandidateStatisticsV2Probe,
    CandidateStatisticsV2CostEstimate,
    CandidateStatisticsV2QualityReport,
    CandidateStatisticsV2Result,
    parse_candidate_statistics_v2_rows,
)
from crypto_address_identity.universe.models import UniverseModel
from crypto_address_identity.universe.query_plan import BigQueryQueryPlan


CANDIDATE_STATISTICS_V2_AUTHORIZATION_ID = (
    "btc-importance-v2-20260724-one-shot"
)
CANDIDATE_STATISTICS_V2_RECOVERY_AUTHORIZATION_ID = (
    "btc-importance-v2-20260724-quota-recovery-one-shot"
)
CANDIDATE_STATISTICS_V2_MAXIMUM_BYTES_BILLED = 650_000_000_000
PINNED_V2_MONTHLY_PROCESSING_BUDGET_BYTES = 2_000_000_000_000
PINNED_V2_MONTHLY_RESERVE_BYTES = 250_000_000_000
PINNED_V2_CUTOFF_DATE = date(2026, 7, 24)
PINNED_V2_DRY_RUN_BYTES = 637_999_682_243
PINNED_V2_CANDIDATE_QUERY_SHA256 = (
    "47b0b8977cc1443578bc3daf3f90a2cf5e0e48ae758a4b7a133d3caa7d301e74"
)
PINNED_V2_CANDIDATE_SCHEMA_SHA256 = (
    "7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7"
)
PINNED_V2_FAILED_RECEIPT_SHA256 = (
    "80fe04a3ca6be426f4fbb1c2c5705674b54059589d49e91e731449afd771b661"
)
PINNED_V2_FAILED_JOB_ID = (
    "cai_btc_importance_v2_5bf66cb53c91d059f860e2c44865303383ba694d"
)
PINNED_V2_FAILED_JOB_ERROR_REASON = "quotaExceeded"

_AUTHORIZATION_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{2,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class CandidateStatisticsV2ExecutionAlreadyAttempted(RuntimeError):
    """Raised before network use when the one-shot receipt already exists."""


class CandidateStatisticsV2RecoveryEvidenceInvalid(RuntimeError):
    """Raised before network use when quota-recovery evidence is invalid."""


class CandidateStatisticsV2ExecutionRequest(UniverseModel):
    authorization_id: str
    billing_acknowledged: Literal[True]
    as_of_date: date
    cutoff_height: int = Field(ge=0)
    expected_query_sha256: str
    expected_schema_sha256: str
    expected_source_standard_address_count: int = Field(ge=0)
    expected_source_input_only_address_count: int = Field(ge=0)
    expected_dry_run_bytes: int = Field(gt=0)
    expected_successful_query_jobs: int = Field(ge=0)
    expected_month_to_date_billed_bytes: int = Field(ge=0)
    maximum_bytes_billed: int = Field(gt=0)
    monthly_processing_budget_bytes: int = Field(gt=0)
    reserve_bytes: int = Field(ge=0)
    recovery_from_authorization_id: str | None = None
    expected_previous_receipt_sha256: str | None = None
    expected_previous_job_id: str | None = None
    expected_previous_job_error_reason: str | None = None
    expected_previous_job_total_bytes_processed: int | None = Field(
        default=None,
        ge=0,
    )
    expected_previous_job_total_bytes_billed: int | None = Field(
        default=None,
        ge=0,
    )

    @field_validator("authorization_id")
    @classmethod
    def validate_authorization_id(cls, value: str) -> str:
        if not _AUTHORIZATION_ID_RE.fullmatch(value):
            raise ValueError("authorization_id contains unsafe characters")
        return value

    @field_validator("expected_query_sha256", "expected_schema_sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        if not _SHA256_RE.fullmatch(value):
            raise ValueError("expected checksum must be a lower-case SHA-256")
        return value

    @field_validator("expected_previous_receipt_sha256")
    @classmethod
    def validate_optional_sha256(cls, value: str | None) -> str | None:
        if value is not None and not _SHA256_RE.fullmatch(value):
            raise ValueError("expected checksum must be a lower-case SHA-256")
        return value

    @model_validator(mode="after")
    def validate_pinned_contract(
        self,
    ) -> "CandidateStatisticsV2ExecutionRequest":
        if self.authorization_id not in {
            CANDIDATE_STATISTICS_V2_AUTHORIZATION_ID,
            CANDIDATE_STATISTICS_V2_RECOVERY_AUTHORIZATION_ID,
        }:
            raise ValueError(
                "candidate v2 execution authorization id is not authorized"
            )
        exact_values = (
            (
                self.as_of_date,
                PINNED_V2_CUTOFF_DATE,
                "cutoff date",
            ),
            (
                self.cutoff_height,
                PINNED_V2_CUTOFF_HEIGHT,
                "cutoff height",
            ),
            (
                self.expected_query_sha256,
                PINNED_V2_CANDIDATE_QUERY_SHA256,
                "query checksum",
            ),
            (
                self.expected_schema_sha256,
                PINNED_V2_CANDIDATE_SCHEMA_SHA256,
                "schema checksum",
            ),
            (
                self.expected_source_standard_address_count,
                PINNED_V2_SOURCE_STANDARD_ADDRESS_COUNT,
                "source baseline",
            ),
            (
                self.expected_source_input_only_address_count,
                PINNED_V2_SOURCE_INPUT_ONLY_ADDRESS_COUNT,
                "input-only baseline",
            ),
            (
                self.expected_dry_run_bytes,
                PINNED_V2_DRY_RUN_BYTES,
                "dry-run bytes",
            ),
            (
                self.maximum_bytes_billed,
                CANDIDATE_STATISTICS_V2_MAXIMUM_BYTES_BILLED,
                "billing cap",
            ),
            (
                self.monthly_processing_budget_bytes,
                PINNED_V2_MONTHLY_PROCESSING_BUDGET_BYTES,
                "monthly processing budget",
            ),
            (
                self.reserve_bytes,
                PINNED_V2_MONTHLY_RESERVE_BYTES,
                "monthly reserve",
            ),
        )
        for actual, expected, label in exact_values:
            if actual != expected:
                raise ValueError(
                    f"candidate v2 execution {label} is not authorized"
                )
        if self.expected_dry_run_bytes > self.maximum_bytes_billed:
            raise ValueError("candidate v2 dry run exceeds the billing cap")
        projected = (
            self.expected_month_to_date_billed_bytes
            + self.expected_dry_run_bytes
        )
        if (
            projected
            > self.monthly_processing_budget_bytes - self.reserve_bytes
        ):
            raise ValueError(
                "candidate v2 execution exceeds the monthly byte budget"
            )
        recovery_values = (
            self.recovery_from_authorization_id,
            self.expected_previous_receipt_sha256,
            self.expected_previous_job_id,
            self.expected_previous_job_error_reason,
            self.expected_previous_job_total_bytes_processed,
            self.expected_previous_job_total_bytes_billed,
        )
        if (
            self.authorization_id
            == CANDIDATE_STATISTICS_V2_AUTHORIZATION_ID
        ):
            if any(value is not None for value in recovery_values):
                raise ValueError(
                    "initial candidate v2 authorization cannot recover"
                )
        else:
            exact_recovery_values = (
                (
                    self.recovery_from_authorization_id,
                    CANDIDATE_STATISTICS_V2_AUTHORIZATION_ID,
                    "prior authorization id",
                ),
                (
                    self.expected_previous_receipt_sha256,
                    PINNED_V2_FAILED_RECEIPT_SHA256,
                    "prior receipt checksum",
                ),
                (
                    self.expected_previous_job_id,
                    PINNED_V2_FAILED_JOB_ID,
                    "prior job id",
                ),
                (
                    self.expected_previous_job_error_reason,
                    PINNED_V2_FAILED_JOB_ERROR_REASON,
                    "prior job error reason",
                ),
                (
                    self.expected_previous_job_total_bytes_processed,
                    0,
                    "prior processed bytes",
                ),
                (
                    self.expected_previous_job_total_bytes_billed,
                    0,
                    "prior billed bytes",
                ),
            )
            for actual, expected, label in exact_recovery_values:
                if actual != expected:
                    raise ValueError(
                        f"candidate v2 recovery {label} is not authorized"
                    )
        return self

    @property
    def cutoff_time(self) -> datetime:
        return datetime.combine(self.as_of_date, time.max, tzinfo=UTC)


class CandidateStatisticsV2ExecutionOutcome(UniverseModel):
    status: Literal[
        "dry_run",
        "preflight_blocked",
        "completed",
        "quality_blocked",
        "failed",
    ]
    authorization_id: str
    billing_acknowledged: Literal[True]
    job_id: str
    query_sha256: str
    expected_schema_sha256: str
    expected_source_standard_address_count: int = Field(ge=0)
    expected_source_input_only_address_count: int = Field(ge=0)
    expected_dry_run_bytes: int = Field(gt=0)
    expected_successful_query_jobs: int = Field(ge=0)
    expected_month_to_date_billed_bytes: int = Field(ge=0)
    cutoff_height: int = Field(ge=0)
    cutoff_time: datetime
    maximum_bytes_billed: int = Field(gt=0)
    monthly_processing_budget_bytes: int = Field(gt=0)
    reserve_bytes: int = Field(ge=0)
    receipt_path: str
    receipt_created: bool
    cost_estimate: CandidateStatisticsV2CostEstimate | None = None
    row_count: int = Field(default=0, ge=0, le=2)
    total_bytes_processed: int | None = Field(default=None, ge=0)
    total_bytes_billed: int | None = Field(default=None, ge=0)
    statistics: CandidateStatisticsV2Result | None = None
    quality: CandidateStatisticsV2QualityReport | None = None
    blocking_reasons: tuple[str, ...] = ()
    network_requests: int = Field(default=0, ge=0, le=4)
    execution_calls: int = Field(default=0, ge=0, le=1)
    automatic_retries: Literal[0] = 0
    recovery_from_authorization_id: str | None = None
    expected_previous_receipt_sha256: str | None = None
    expected_previous_job_id: str | None = None
    expected_previous_job_error_reason: str | None = None
    expected_previous_job_total_bytes_processed: int | None = Field(
        default=None,
        ge=0,
    )
    expected_previous_job_total_bytes_billed: int | None = Field(
        default=None,
        ge=0,
    )
    recovery_evidence_validated: bool = False
    provider_requests: Literal[0] = 0
    provider_points: Literal[0] = 0
    candidate_materialized: Literal[False] = False
    written_paths: tuple[str, ...] = ()

    @field_validator("cutoff_time")
    @classmethod
    def validate_cutoff_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("cutoff_time must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator("blocking_reasons")
    @classmethod
    def canonicalize_reasons(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted(set(value)))

    @model_validator(mode="after")
    def validate_execution_state(
        self,
    ) -> "CandidateStatisticsV2ExecutionOutcome":
        if self.status in {"dry_run", "preflight_blocked"}:
            if (
                self.receipt_created
                or self.execution_calls
                or self.written_paths
            ):
                raise ValueError(
                    "non-executing outcomes cannot write a receipt"
                )
        elif (
            not self.receipt_created
            or self.execution_calls != 1
            or self.written_paths != (self.receipt_path,)
        ):
            raise ValueError(
                "executing outcomes require one receipt and call"
            )
        if self.status == "completed" and (
            self.row_count != 1
            or self.statistics is None
            or self.quality is None
            or not self.quality.allow_interpretation
        ):
            raise ValueError(
                "completed outcome requires one accepted aggregate row"
            )
        if self.status == "quality_blocked" and (
            self.quality is None or self.quality.allow_interpretation
        ):
            raise ValueError(
                "quality-blocked outcome requires blocking quality"
            )
        is_recovery = (
            self.authorization_id
            == CANDIDATE_STATISTICS_V2_RECOVERY_AUTHORIZATION_ID
        )
        if not is_recovery and (
            self.recovery_from_authorization_id is not None
            or self.expected_previous_receipt_sha256 is not None
            or self.expected_previous_job_id is not None
            or self.expected_previous_job_error_reason is not None
            or self.expected_previous_job_total_bytes_processed is not None
            or self.expected_previous_job_total_bytes_billed is not None
            or self.recovery_evidence_validated
        ):
            raise ValueError(
                "initial candidate v2 outcome cannot contain recovery evidence"
            )
        if (
            is_recovery
            and self.status
            in {"completed", "quality_blocked", "failed"}
            and not self.recovery_evidence_validated
        ):
            raise ValueError(
                "executed recovery requires validated prior evidence"
            )
        return self


def preview_candidate_statistics_v2_execution(
    request: CandidateStatisticsV2ExecutionRequest,
    *,
    dataset: str,
    receipt_root: Path,
) -> CandidateStatisticsV2ExecutionOutcome:
    plan = BigQueryQueryPlan.load(dataset)
    _validate_plan(plan, request)
    return _outcome(
        status="dry_run",
        request=request,
        plan=plan,
        receipt_path=_receipt_path(
            receipt_root,
            request.authorization_id,
        ),
    )


class CandidateStatisticsV2OneShotExecutor:
    """Billed, cost-gated execution with one durable authorization."""

    def __init__(
        self,
        *,
        backend: BigQueryBackend,
        dataset: str,
        receipt_root: Path,
        max_source_age: timedelta,
        now: datetime | None = None,
    ) -> None:
        if max_source_age <= timedelta(0):
            raise ValueError("max_source_age must be positive")
        self._backend = backend
        self._plan = BigQueryQueryPlan.load(dataset)
        self._receipt_root = receipt_root
        self._max_source_age = max_source_age
        self._now = _as_utc(now or datetime.now(UTC))

    def run(
        self,
        request: CandidateStatisticsV2ExecutionRequest,
    ) -> CandidateStatisticsV2ExecutionOutcome:
        _validate_plan(self._plan, request)
        receipt_path = _receipt_path(
            self._receipt_root,
            request.authorization_id,
        )
        if receipt_path.exists():
            raise CandidateStatisticsV2ExecutionAlreadyAttempted()
        recovery_evidence_validated = _validate_recovery_evidence(
            self._receipt_root,
            request,
        )

        cost = BigQueryCandidateStatisticsV2Probe(
            backend=self._backend,
            dataset=self._plan.dataset,
            max_source_age=self._max_source_age,
            now=self._now,
        ).run(
            cutoff_height=request.cutoff_height,
            cutoff_time=request.cutoff_time,
            expected_query_sha256=request.expected_query_sha256,
            sandbox_budget_bytes=request.monthly_processing_budget_bytes,
            reserve_bytes=request.reserve_bytes,
        )
        preflight_reasons = list(cost.blocking_reasons)
        if cost.status != "within_budget" or cost.within_budget is not True:
            if not preflight_reasons:
                preflight_reasons.append(
                    "candidate_statistics_v2_execution_cost_gate_blocked"
                )
        if cost.schema_sha256 != request.expected_schema_sha256:
            preflight_reasons.append(
                "candidate_statistics_v2_execution_schema_hash_mismatch"
            )
        if cost.dry_run_bytes != request.expected_dry_run_bytes:
            preflight_reasons.append(
                "candidate_statistics_v2_execution_dry_run_bytes_mismatch"
            )
        if (
            cost.successful_query_jobs
            != request.expected_successful_query_jobs
        ):
            preflight_reasons.append(
                "candidate_statistics_v2_execution_job_count_mismatch"
            )
        if (
            cost.month_to_date_billed_bytes
            != request.expected_month_to_date_billed_bytes
        ):
            preflight_reasons.append(
                "candidate_statistics_v2_execution_monthly_bytes_mismatch"
            )
        if preflight_reasons:
            return _outcome(
                status="preflight_blocked",
                request=request,
                plan=self._plan,
                receipt_path=receipt_path,
                cost_estimate=cost,
                blocking_reasons=tuple(preflight_reasons),
                network_requests=cost.network_requests,
                recovery_evidence_validated=recovery_evidence_validated,
            )

        job_id = _job_id(request)
        _create_receipt_exclusive(
            receipt_path,
            {
                "schema_version": (
                    "btc_candidate_statistics_v2_execution_receipt_v1"
                ),
                "status": "started",
                "authorization_id": request.authorization_id,
                "job_id": job_id,
                "created_at": self._now.isoformat().replace("+00:00", "Z"),
                "billing_acknowledged": True,
                "query_sha256": self._plan.candidate_statistics_v2_sha256,
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
                "cost_estimate": cost.model_dump(mode="json"),
                "automatic_retries": 0,
                "candidate_materialized": False,
                **_recovery_receipt_fields(
                    request,
                    recovery_evidence_validated,
                ),
            },
        )

        try:
            execution = self._backend.execute_aggregate_at_most_two_no_retry(
                self._plan.candidate_statistics_v2_sql,
                {
                    "cutoff_height": request.cutoff_height,
                    "cutoff_time": request.cutoff_time,
                    "query_sha256": (
                        self._plan.candidate_statistics_v2_sha256
                    ),
                    "schema_sha256": request.expected_schema_sha256,
                },
                maximum_bytes_billed=request.maximum_bytes_billed,
                job_id=job_id,
            )
        except Exception:
            outcome = _outcome(
                status="failed",
                request=request,
                plan=self._plan,
                receipt_path=receipt_path,
                receipt_created=True,
                cost_estimate=cost,
                blocking_reasons=(
                    "candidate_statistics_v2_execution_failed",
                ),
                network_requests=cost.network_requests + 1,
                execution_calls=1,
                written_paths=(str(receipt_path),),
                recovery_evidence_validated=recovery_evidence_validated,
            )
            _finish_receipt(receipt_path, outcome)
            return outcome

        statistics, quality = parse_candidate_statistics_v2_rows(
            execution.rows,
            expected_query_sha256=request.expected_query_sha256,
            expected_schema_sha256=request.expected_schema_sha256,
            expected_source_standard_address_count=(
                request.expected_source_standard_address_count
            ),
            expected_source_input_only_address_count=(
                request.expected_source_input_only_address_count
            ),
            expected_cutoff_height=request.cutoff_height,
            expected_cutoff_time=request.cutoff_time,
            now=self._now,
            max_source_age=self._max_source_age,
        )
        post_execution_reasons = list(quality.blocking_reasons)
        if execution.total_bytes_processed != request.expected_dry_run_bytes:
            post_execution_reasons.append(
                "candidate_statistics_v2_execution_processed_bytes_mismatch"
            )
        if execution.total_bytes_billed > request.maximum_bytes_billed:
            post_execution_reasons.append(
                "candidate_statistics_v2_execution_billing_cap_exceeded"
            )
        if post_execution_reasons:
            quality = CandidateStatisticsV2QualityReport(
                status="blocked",
                allow_interpretation=False,
                blocking_reasons=tuple(post_execution_reasons),
                warnings=quality.warnings,
            )
        outcome = _outcome(
            status=(
                "completed"
                if quality.allow_interpretation
                else "quality_blocked"
            ),
            request=request,
            plan=self._plan,
            receipt_path=receipt_path,
            receipt_created=True,
            cost_estimate=cost,
            row_count=len(execution.rows),
            total_bytes_processed=execution.total_bytes_processed,
            total_bytes_billed=execution.total_bytes_billed,
            statistics=statistics,
            quality=quality,
            blocking_reasons=quality.blocking_reasons,
            network_requests=cost.network_requests + 1,
            execution_calls=1,
            written_paths=(str(receipt_path),),
            recovery_evidence_validated=recovery_evidence_validated,
        )
        _finish_receipt(receipt_path, outcome)
        return outcome


def _validate_plan(
    plan: BigQueryQueryPlan,
    request: CandidateStatisticsV2ExecutionRequest,
) -> None:
    if (
        plan.candidate_statistics_v2_sha256
        != PINNED_V2_CANDIDATE_QUERY_SHA256
        or plan.candidate_statistics_v2_sha256
        != request.expected_query_sha256
    ):
        raise ValueError("candidate v2 query plan checksum drifted")


def _receipt_path(receipt_root: Path, authorization_id: str) -> Path:
    if authorization_id not in {
        CANDIDATE_STATISTICS_V2_AUTHORIZATION_ID,
        CANDIDATE_STATISTICS_V2_RECOVERY_AUTHORIZATION_ID,
    }:
        raise ValueError("candidate v2 authorization id is not allowed")
    return receipt_root / f"{authorization_id}.json"


def _validate_recovery_evidence(
    receipt_root: Path,
    request: CandidateStatisticsV2ExecutionRequest,
) -> bool:
    if (
        request.authorization_id
        != CANDIDATE_STATISTICS_V2_RECOVERY_AUTHORIZATION_ID
    ):
        return False
    previous_path = _receipt_path(
        receipt_root,
        CANDIDATE_STATISTICS_V2_AUTHORIZATION_ID,
    )
    try:
        metadata = previous_path.stat()
        if not stat.S_ISREG(metadata.st_mode):
            raise CandidateStatisticsV2RecoveryEvidenceInvalid()
        if stat.S_IMODE(metadata.st_mode) != 0o600:
            raise CandidateStatisticsV2RecoveryEvidenceInvalid()
        encoded = previous_path.read_bytes()
        if not encoded or len(encoded) > 1_000_000:
            raise CandidateStatisticsV2RecoveryEvidenceInvalid()
        digest = hashlib.sha256(encoded).hexdigest()
        if (
            digest != PINNED_V2_FAILED_RECEIPT_SHA256
            or digest != request.expected_previous_receipt_sha256
        ):
            raise CandidateStatisticsV2RecoveryEvidenceInvalid()
        payload = json.loads(encoded)
    except (
        CandidateStatisticsV2RecoveryEvidenceInvalid,
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise CandidateStatisticsV2RecoveryEvidenceInvalid() from exc
    expected_fields = {
        "schema_version": (
            "btc_candidate_statistics_v2_execution_receipt_v1"
        ),
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
    if (
        not isinstance(payload, dict)
        or any(
            payload.get(field) != expected
            for field, expected in expected_fields.items()
        )
    ):
        raise CandidateStatisticsV2RecoveryEvidenceInvalid()
    return True


def _recovery_receipt_fields(
    request: CandidateStatisticsV2ExecutionRequest,
    validated: bool,
) -> dict[str, object]:
    if (
        request.authorization_id
        != CANDIDATE_STATISTICS_V2_RECOVERY_AUTHORIZATION_ID
    ):
        return {}
    return {
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
        "expected_previous_job_total_bytes_processed": (
            request.expected_previous_job_total_bytes_processed
        ),
        "expected_previous_job_total_bytes_billed": (
            request.expected_previous_job_total_bytes_billed
        ),
        "recovery_evidence_validated": validated,
    }


def _job_id(request: CandidateStatisticsV2ExecutionRequest) -> str:
    digest = hashlib.sha256(
        (
            request.authorization_id
            + "\0"
            + request.expected_query_sha256
            + "\0"
            + request.expected_schema_sha256
        ).encode("ascii")
    ).hexdigest()
    return f"cai_btc_importance_v2_{digest[:40]}"


def _create_receipt_exclusive(
    path: Path,
    payload: dict[str, object],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
    except FileExistsError as exc:
        raise CandidateStatisticsV2ExecutionAlreadyAttempted() from exc
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(_encode_json(payload))
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        path.unlink(missing_ok=True)
        raise
    _fsync_directory(path.parent)


def _finish_receipt(
    path: Path,
    outcome: CandidateStatisticsV2ExecutionOutcome,
) -> None:
    payload = {
        "schema_version": (
            "btc_candidate_statistics_v2_execution_receipt_v1"
        ),
        **outcome.model_dump(mode="json"),
        "completed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(_encode_json(payload))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _encode_json(payload: dict[str, object]) -> bytes:
    return (
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        + "\n"
    ).encode("ascii")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    return value.astimezone(UTC)


def _outcome(
    *,
    status: Literal[
        "dry_run",
        "preflight_blocked",
        "completed",
        "quality_blocked",
        "failed",
    ],
    request: CandidateStatisticsV2ExecutionRequest,
    plan: BigQueryQueryPlan,
    receipt_path: Path,
    receipt_created: bool = False,
    cost_estimate: CandidateStatisticsV2CostEstimate | None = None,
    row_count: int = 0,
    total_bytes_processed: int | None = None,
    total_bytes_billed: int | None = None,
    statistics: CandidateStatisticsV2Result | None = None,
    quality: CandidateStatisticsV2QualityReport | None = None,
    blocking_reasons: tuple[str, ...] = (),
    network_requests: int = 0,
    execution_calls: int = 0,
    written_paths: tuple[str, ...] = (),
    recovery_evidence_validated: bool = False,
) -> CandidateStatisticsV2ExecutionOutcome:
    return CandidateStatisticsV2ExecutionOutcome(
        status=status,
        authorization_id=request.authorization_id,
        billing_acknowledged=request.billing_acknowledged,
        job_id=_job_id(request),
        query_sha256=plan.candidate_statistics_v2_sha256,
        expected_schema_sha256=request.expected_schema_sha256,
        expected_source_standard_address_count=(
            request.expected_source_standard_address_count
        ),
        expected_source_input_only_address_count=(
            request.expected_source_input_only_address_count
        ),
        expected_dry_run_bytes=request.expected_dry_run_bytes,
        expected_successful_query_jobs=(
            request.expected_successful_query_jobs
        ),
        expected_month_to_date_billed_bytes=(
            request.expected_month_to_date_billed_bytes
        ),
        cutoff_height=request.cutoff_height,
        cutoff_time=request.cutoff_time,
        maximum_bytes_billed=request.maximum_bytes_billed,
        monthly_processing_budget_bytes=(
            request.monthly_processing_budget_bytes
        ),
        reserve_bytes=request.reserve_bytes,
        receipt_path=str(receipt_path),
        receipt_created=receipt_created,
        cost_estimate=cost_estimate,
        row_count=row_count,
        total_bytes_processed=total_bytes_processed,
        total_bytes_billed=total_bytes_billed,
        statistics=statistics,
        quality=quality,
        blocking_reasons=blocking_reasons,
        network_requests=network_requests,
        execution_calls=execution_calls,
        recovery_from_authorization_id=(
            request.recovery_from_authorization_id
        ),
        expected_previous_receipt_sha256=(
            request.expected_previous_receipt_sha256
        ),
        expected_previous_job_id=request.expected_previous_job_id,
        expected_previous_job_error_reason=(
            request.expected_previous_job_error_reason
        ),
        expected_previous_job_total_bytes_processed=(
            request.expected_previous_job_total_bytes_processed
        ),
        expected_previous_job_total_bytes_billed=(
            request.expected_previous_job_total_bytes_billed
        ),
        recovery_evidence_validated=recovery_evidence_validated,
        written_paths=written_paths,
    )
