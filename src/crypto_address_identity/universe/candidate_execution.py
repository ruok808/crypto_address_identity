"""One-shot, aggregate-only execution boundary for BTC candidate statistics."""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator, model_validator

from crypto_address_identity.universe.bigquery import BigQueryBackend
from crypto_address_identity.universe.candidate_statistics import (
    BigQueryCandidateStatisticsProbe,
    CandidateStatisticsCostEstimate,
    CandidateStatisticsQualityReport,
    CandidateStatisticsResult,
    parse_candidate_statistics_rows,
)
from crypto_address_identity.universe.models import UniverseModel
from crypto_address_identity.universe.query_plan import BigQueryQueryPlan


CANDIDATE_STATISTICS_MAXIMUM_BYTES_BILLED = 650_000_000_000
MINIMUM_SANDBOX_RESERVE_BYTES = 250_000_000_000
PINNED_SOURCE_STANDARD_ADDRESS_COUNT = 1_557_951_354
PINNED_CUTOFF_HEIGHT = 959_187
PINNED_CUTOFF_DATE = date(2026, 7, 24)
PINNED_CANDIDATE_QUERY_SHA256 = (
    "5dbb2c914448837ac43b20e4943abb33130cf2ce9c1b7c2a72eb5ce4d285012c"
)
PINNED_CANDIDATE_SCHEMA_SHA256 = (
    "7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7"
)

_AUTHORIZATION_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{2,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class CandidateStatisticsExecutionAlreadyAttempted(RuntimeError):
    """Raised before network use when an authorization receipt already exists."""


class CandidateStatisticsExecutionRequest(UniverseModel):
    authorization_id: str
    as_of_date: date
    cutoff_height: int = Field(ge=0)
    expected_query_sha256: str
    expected_schema_sha256: str
    expected_source_standard_address_count: int = Field(ge=0)
    maximum_bytes_billed: int = Field(gt=0)
    sandbox_budget_bytes: int = Field(gt=0)
    reserve_bytes: int = Field(ge=0)

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

    @model_validator(mode="after")
    def validate_pinned_contract(self) -> "CandidateStatisticsExecutionRequest":
        if self.as_of_date != PINNED_CUTOFF_DATE:
            raise ValueError("candidate execution cutoff date is not authorized")
        if self.cutoff_height != PINNED_CUTOFF_HEIGHT:
            raise ValueError("candidate execution cutoff height is not authorized")
        if self.expected_query_sha256 != PINNED_CANDIDATE_QUERY_SHA256:
            raise ValueError("candidate execution query checksum is not authorized")
        if self.expected_schema_sha256 != PINNED_CANDIDATE_SCHEMA_SHA256:
            raise ValueError("candidate execution schema checksum is not authorized")
        if (
            self.expected_source_standard_address_count
            != PINNED_SOURCE_STANDARD_ADDRESS_COUNT
        ):
            raise ValueError("candidate execution source baseline is not authorized")
        if (
            self.maximum_bytes_billed
            != CANDIDATE_STATISTICS_MAXIMUM_BYTES_BILLED
        ):
            raise ValueError("candidate execution billing cap is not authorized")
        if self.reserve_bytes < MINIMUM_SANDBOX_RESERVE_BYTES:
            raise ValueError("candidate execution reserve is below the minimum")
        if self.reserve_bytes >= self.sandbox_budget_bytes:
            raise ValueError("candidate execution reserve must be below budget")
        if (
            self.sandbox_budget_bytes - self.reserve_bytes
            < self.maximum_bytes_billed
        ):
            raise ValueError("candidate execution budget cannot cover the query cap")
        return self

    @property
    def cutoff_time(self) -> datetime:
        return datetime.combine(self.as_of_date, time.max, tzinfo=UTC)


class CandidateStatisticsExecutionOutcome(UniverseModel):
    status: Literal[
        "dry_run",
        "preflight_blocked",
        "completed",
        "quality_blocked",
        "failed",
    ]
    authorization_id: str
    query_sha256: str
    expected_schema_sha256: str
    expected_source_standard_address_count: int = Field(ge=0)
    cutoff_height: int = Field(ge=0)
    cutoff_time: datetime
    maximum_bytes_billed: int = Field(gt=0)
    receipt_path: str
    receipt_created: bool
    cost_estimate: CandidateStatisticsCostEstimate | None = None
    row_count: int = Field(default=0, ge=0, le=2)
    total_bytes_processed: int | None = Field(default=None, ge=0)
    total_bytes_billed: int | None = Field(default=None, ge=0)
    statistics: CandidateStatisticsResult | None = None
    quality: CandidateStatisticsQualityReport | None = None
    blocking_reasons: tuple[str, ...] = ()
    network_requests: int = Field(default=0, ge=0, le=4)
    execution_calls: int = Field(default=0, ge=0, le=1)
    automatic_retries: Literal[0] = 0
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
    ) -> "CandidateStatisticsExecutionOutcome":
        if self.status in {"dry_run", "preflight_blocked"}:
            if (
                self.receipt_created
                or self.execution_calls
                or self.written_paths
            ):
                raise ValueError("non-executing outcomes cannot write a receipt")
        else:
            if (
                not self.receipt_created
                or self.execution_calls != 1
                or self.written_paths != (self.receipt_path,)
            ):
                raise ValueError("executing outcomes require one receipt and call")
        if self.status == "completed":
            if (
                self.row_count != 1
                or self.statistics is None
                or self.quality is None
                or not self.quality.allow_interpretation
            ):
                raise ValueError("completed outcome requires one accepted row")
        if self.status == "quality_blocked":
            if self.quality is None or self.quality.allow_interpretation:
                raise ValueError("quality-blocked outcome requires blocking quality")
        return self


def preview_candidate_statistics_execution(
    request: CandidateStatisticsExecutionRequest,
    *,
    dataset: str,
    receipt_root: Path,
) -> CandidateStatisticsExecutionOutcome:
    plan = BigQueryQueryPlan.load(dataset)
    _validate_plan(plan, request)
    receipt_path = _receipt_path(receipt_root, request.authorization_id)
    return _outcome(
        status="dry_run",
        request=request,
        plan=plan,
        receipt_path=receipt_path,
    )


class CandidateStatisticsOneShotExecutor:
    """Cost-gated one-shot execution with an exclusive durable receipt."""

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
        request: CandidateStatisticsExecutionRequest,
    ) -> CandidateStatisticsExecutionOutcome:
        _validate_plan(self._plan, request)
        receipt_path = _receipt_path(
            self._receipt_root,
            request.authorization_id,
        )
        if receipt_path.exists():
            raise CandidateStatisticsExecutionAlreadyAttempted()

        cost = BigQueryCandidateStatisticsProbe(
            backend=self._backend,
            dataset=self._plan.dataset,
            max_source_age=self._max_source_age,
            now=self._now,
        ).run(
            cutoff_height=request.cutoff_height,
            cutoff_time=request.cutoff_time,
            expected_query_sha256=request.expected_query_sha256,
            sandbox_budget_bytes=request.sandbox_budget_bytes,
            reserve_bytes=request.reserve_bytes,
        )
        preflight_reasons = list(cost.blocking_reasons)
        if cost.status != "within_budget" or cost.within_budget is not True:
            if not preflight_reasons:
                preflight_reasons.append(
                    "candidate_statistics_execution_cost_gate_blocked"
                )
        if cost.schema_sha256 != request.expected_schema_sha256:
            preflight_reasons.append(
                "candidate_statistics_execution_schema_hash_mismatch"
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
            )

        job_id = _job_id(request)
        _create_receipt_exclusive(
            receipt_path,
            {
                "schema_version": "btc_candidate_statistics_execution_receipt_v1",
                "status": "started",
                "authorization_id": request.authorization_id,
                "job_id": job_id,
                "created_at": self._now.isoformat().replace("+00:00", "Z"),
                "query_sha256": self._plan.candidate_statistics_sha256,
                "expected_schema_sha256": request.expected_schema_sha256,
                "expected_source_standard_address_count": (
                    request.expected_source_standard_address_count
                ),
                "cutoff_height": request.cutoff_height,
                "cutoff_time": request.cutoff_time.isoformat().replace(
                    "+00:00", "Z"
                ),
                "maximum_bytes_billed": request.maximum_bytes_billed,
                "cost_estimate": cost.model_dump(mode="json"),
                "automatic_retries": 0,
                "candidate_materialized": False,
            },
        )

        try:
            execution = self._backend.execute_aggregate_at_most_two_no_retry(
                self._plan.candidate_statistics_sql,
                {
                    "cutoff_height": request.cutoff_height,
                    "cutoff_time": request.cutoff_time,
                    "query_sha256": self._plan.candidate_statistics_sha256,
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
                    "candidate_statistics_execution_failed",
                ),
                network_requests=cost.network_requests + 1,
                execution_calls=1,
                written_paths=(str(receipt_path),),
            )
            _finish_receipt(receipt_path, outcome)
            return outcome

        statistics, quality = parse_candidate_statistics_rows(
            execution.rows,
            expected_query_sha256=request.expected_query_sha256,
            expected_schema_sha256=request.expected_schema_sha256,
            expected_source_standard_address_count=(
                request.expected_source_standard_address_count
            ),
            expected_cutoff_height=request.cutoff_height,
            expected_cutoff_time=request.cutoff_time,
            now=self._now,
            max_source_age=self._max_source_age,
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
        )
        _finish_receipt(receipt_path, outcome)
        return outcome


def _validate_plan(
    plan: BigQueryQueryPlan,
    request: CandidateStatisticsExecutionRequest,
) -> None:
    if (
        plan.candidate_statistics_sha256
        != PINNED_CANDIDATE_QUERY_SHA256
        or plan.candidate_statistics_sha256
        != request.expected_query_sha256
    ):
        raise ValueError("candidate statistics query plan checksum drifted")


def _receipt_path(receipt_root: Path, authorization_id: str) -> Path:
    if not _AUTHORIZATION_ID_RE.fullmatch(authorization_id):
        raise ValueError("authorization_id contains unsafe characters")
    return receipt_root / f"{authorization_id}.json"


def _job_id(request: CandidateStatisticsExecutionRequest) -> str:
    digest = hashlib.sha256(
        (
            request.authorization_id
            + "\0"
            + request.expected_query_sha256
            + "\0"
            + request.expected_schema_sha256
        ).encode("ascii")
    ).hexdigest()
    return f"cai_btc_candidate_statistics_{digest[:40]}"


def _create_receipt_exclusive(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = _encode_json(payload)
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
    except FileExistsError as exc:
        raise CandidateStatisticsExecutionAlreadyAttempted() from exc
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        path.unlink(missing_ok=True)
        raise
    _fsync_directory(path.parent)


def _finish_receipt(
    path: Path,
    outcome: CandidateStatisticsExecutionOutcome,
) -> None:
    payload = {
        "schema_version": "btc_candidate_statistics_execution_receipt_v1",
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
    request: CandidateStatisticsExecutionRequest,
    plan: BigQueryQueryPlan,
    receipt_path: Path,
    receipt_created: bool = False,
    cost_estimate: CandidateStatisticsCostEstimate | None = None,
    row_count: int = 0,
    total_bytes_processed: int | None = None,
    total_bytes_billed: int | None = None,
    statistics: CandidateStatisticsResult | None = None,
    quality: CandidateStatisticsQualityReport | None = None,
    blocking_reasons: tuple[str, ...] = (),
    network_requests: int = 0,
    execution_calls: int = 0,
    written_paths: tuple[str, ...] = (),
) -> CandidateStatisticsExecutionOutcome:
    return CandidateStatisticsExecutionOutcome(
        status=status,
        authorization_id=request.authorization_id,
        query_sha256=plan.candidate_statistics_sha256,
        expected_schema_sha256=request.expected_schema_sha256,
        expected_source_standard_address_count=(
            request.expected_source_standard_address_count
        ),
        cutoff_height=request.cutoff_height,
        cutoff_time=request.cutoff_time,
        maximum_bytes_billed=request.maximum_bytes_billed,
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
        written_paths=written_paths,
    )
