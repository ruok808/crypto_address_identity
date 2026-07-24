"""BigQuery metadata, cost, checkpoint, and optional execution boundary."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Literal, Protocol

from pydantic import Field, computed_field, field_validator

from crypto_address_identity.universe.models import SourceProbeResult, UniverseModel
from crypto_address_identity.universe.query_plan import BigQueryQueryPlan


class BigQueryDependencyMissing(RuntimeError):
    """Raised when the optional Google SDK is unavailable."""


class BigQueryCredentialsUnavailable(RuntimeError):
    """Raised when Google credentials cannot be established."""


class BigQueryBoundaryError(RuntimeError):
    """Safe query boundary error without an upstream payload."""


class TableField(UniverseModel):
    name: str
    field_type: str
    mode: str
    fields: tuple["TableField", ...] = ()

    @field_validator("field_type", "mode")
    @classmethod
    def canonicalize_upper(cls, value: str) -> str:
        return value.upper()


class TableMetadata(UniverseModel):
    table_id: str
    fields: tuple[TableField, ...]
    partition_field: str | None
    partition_type: str | None
    modified_at: datetime

    @field_validator("modified_at")
    @classmethod
    def validate_modified_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("modified_at must be timezone-aware")
        return value.astimezone(UTC)

    @computed_field
    @property
    def schema_sha256(self) -> str:
        payload = [
            field.model_dump(mode="json")
            for field in sorted(self.fields, key=lambda item: item.name)
        ]
        return hashlib.sha256(
            json.dumps(
                payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
            ).encode("ascii")
        ).hexdigest()


class QueryEstimate(UniverseModel):
    total_bytes_processed: int = Field(ge=0)
    cache_hit: bool


class BigQueryBackend(Protocol):
    last_query_total_bytes_processed: int | None

    def table_metadata(self, table_id: str) -> TableMetadata: ...

    def dry_run(
        self,
        sql: str,
        parameters: dict[str, object],
        maximum_bytes_billed: int,
    ) -> QueryEstimate: ...

    def query_one(
        self,
        sql: str,
        parameters: dict[str, object],
        *,
        maximum_bytes_billed: int,
    ) -> Mapping[str, object]: ...

    def stream_arrow_batches(
        self,
        sql: str,
        parameters: dict[str, object],
        *,
        maximum_bytes_billed: int,
        page_size: int,
    ) -> Iterator[object]: ...


_TRANSACTION_FIELDS = {
    "hash": ("STRING", "REQUIRED"),
    "block_hash": ("STRING", "REQUIRED"),
    "block_number": ("INTEGER", "REQUIRED"),
    "block_timestamp": ("TIMESTAMP", "REQUIRED"),
    "block_timestamp_month": ("DATE", "REQUIRED"),
    "inputs": ("RECORD", "REPEATED"),
    "outputs": ("RECORD", "REPEATED"),
}
_TRANSACTION_INPUT_FIELDS = {
    "index": ("INTEGER", "REQUIRED"),
    "spent_transaction_hash": ("STRING", "NULLABLE"),
    "spent_output_index": ("INTEGER", "NULLABLE"),
    "script_hex": ("STRING", "NULLABLE"),
    "type": ("STRING", "NULLABLE"),
    "addresses": ("STRING", "REPEATED"),
    "value": ("NUMERIC", "NULLABLE"),
}
_TRANSACTION_OUTPUT_FIELDS = {
    "index": ("INTEGER", "REQUIRED"),
    "script_hex": ("STRING", "NULLABLE"),
    "type": ("STRING", "NULLABLE"),
    "addresses": ("STRING", "REPEATED"),
    "value": ("NUMERIC", "NULLABLE"),
}
_BLOCK_FIELDS = {
    "hash": ("STRING", "REQUIRED"),
    "number": ("INTEGER", "REQUIRED"),
    "timestamp": ("TIMESTAMP", "REQUIRED"),
    "timestamp_month": ("DATE", "REQUIRED"),
}
_ADDRESS_SCALE_TRANSACTION_FIELDS = {
    "block_number": ("INTEGER", "REQUIRED"),
    "block_timestamp": ("TIMESTAMP", "REQUIRED"),
    "block_timestamp_month": ("DATE", "REQUIRED"),
    "outputs": ("RECORD", "REPEATED"),
}
_ADDRESS_SCALE_OUTPUT_FIELDS = {
    "addresses": ("STRING", "REPEATED"),
}


def _field_contracts(
    fields: tuple[TableField, ...],
) -> dict[str, tuple[str, str]]:
    return {
        field.name: (field.field_type, field.mode)
        for field in fields
    }


def _contains_contract(
    actual: Mapping[str, tuple[str, str]],
    required: Mapping[str, tuple[str, str]],
) -> bool:
    return all(
        actual.get(field_name) == contract
        for field_name, contract in required.items()
    )


def _transaction_metadata_reasons(
    *,
    transactions: TableMetadata,
    now: datetime,
    max_source_age: timedelta,
    required_fields: Mapping[str, tuple[str, str]],
    required_nested_fields: Mapping[str, Mapping[str, tuple[str, str]]],
) -> tuple[str, ...]:
    reasons: list[str] = []
    schema_matches = _contains_contract(
        _field_contracts(transactions.fields),
        required_fields,
    )
    for container_name, required in required_nested_fields.items():
        container = next(
            (
                field
                for field in transactions.fields
                if field.name == container_name
            ),
            None,
        )
        if container is None or not _contains_contract(
            _field_contracts(container.fields),
            required,
        ):
            schema_matches = False
    if not schema_matches:
        reasons.append("bigquery_transactions_schema_mismatch")
    if (
        transactions.partition_field != "block_timestamp_month"
        or transactions.partition_type != "DAY"
    ):
        reasons.append("bigquery_transactions_not_time_partitioned")
    if now - transactions.modified_at > max_source_age:
        reasons.append("bigquery_transactions_stale")
    return tuple(sorted(set(reasons)))


class BigQueryAddressScaleEstimate(UniverseModel):
    """Cost-only result for the exact aggregate address-scale query."""

    source_kind: Literal["bigquery"] = "bigquery"
    query_kind: Literal["btc_address_scale"] = "btc_address_scale"
    status: Literal["within_budget", "over_budget", "blocked"]
    read_only: Literal[True] = True
    schema_sha256: str | None = None
    query_sha256: str
    dry_run_bytes: int | None = Field(default=None, ge=0)
    sandbox_budget_bytes: int = Field(gt=0)
    within_budget: bool | None = None
    exact_distinct: Literal[True] = True
    blocking_reasons: tuple[str, ...] = ()


class BigQueryAddressScaleProbe:
    """Estimate one exact address-only aggregate without executing it."""

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
        timestamp = now or datetime.now(UTC)
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("probe now must be timezone-aware")
        self._now = timestamp.astimezone(UTC)

    def run(
        self,
        *,
        cutoff_height: int,
        cutoff_time: datetime,
        sandbox_budget_bytes: int,
    ) -> BigQueryAddressScaleEstimate:
        if cutoff_height < 0:
            raise ValueError("cutoff height must be non-negative")
        if cutoff_time.tzinfo is None or cutoff_time.utcoffset() is None:
            raise ValueError("cutoff time must be timezone-aware")
        if sandbox_budget_bytes <= 0:
            raise ValueError("sandbox budget must be positive")

        try:
            transactions = self._backend.table_metadata(
                self._plan.transactions_table_id
            )
        except Exception:
            return self._blocked(
                sandbox_budget_bytes=sandbox_budget_bytes,
                reasons=("bigquery_metadata_unavailable",),
            )

        schema_sha256 = transactions.schema_sha256
        reasons = _transaction_metadata_reasons(
            transactions=transactions,
            now=self._now,
            max_source_age=self._max_source_age,
            required_fields=_ADDRESS_SCALE_TRANSACTION_FIELDS,
            required_nested_fields={
                "outputs": _ADDRESS_SCALE_OUTPUT_FIELDS,
            },
        )
        if reasons:
            return self._blocked(
                sandbox_budget_bytes=sandbox_budget_bytes,
                reasons=reasons,
                schema_sha256=schema_sha256,
            )

        try:
            estimate = self._backend.dry_run(
                self._plan.address_scale_sql,
                {
                    "cutoff_height": cutoff_height,
                    "cutoff_time": cutoff_time.astimezone(UTC),
                },
                0,
            )
        except Exception:
            return self._blocked(
                sandbox_budget_bytes=sandbox_budget_bytes,
                reasons=("bigquery_address_scale_dry_run_failed",),
                schema_sha256=schema_sha256,
            )

        within_budget = (
            estimate.total_bytes_processed <= sandbox_budget_bytes
        )
        return BigQueryAddressScaleEstimate(
            status="within_budget" if within_budget else "over_budget",
            schema_sha256=schema_sha256,
            query_sha256=self._plan.address_scale_sha256,
            dry_run_bytes=estimate.total_bytes_processed,
            sandbox_budget_bytes=sandbox_budget_bytes,
            within_budget=within_budget,
        )

    def _blocked(
        self,
        *,
        sandbox_budget_bytes: int,
        reasons: tuple[str, ...],
        schema_sha256: str | None = None,
    ) -> BigQueryAddressScaleEstimate:
        return BigQueryAddressScaleEstimate(
            status="blocked",
            schema_sha256=schema_sha256,
            query_sha256=self._plan.address_scale_sha256,
            sandbox_budget_bytes=sandbox_budget_bytes,
            within_budget=None,
            blocking_reasons=reasons,
        )


class BigQueryProbe:
    """Fail-closed schema, freshness, checkpoint, and cost probe."""

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
        timestamp = now or datetime.now(UTC)
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("probe now must be timezone-aware")
        self._now = timestamp.astimezone(UTC)

    def run(
        self,
        *,
        as_of_date: date,
        cutoff_height: int | None,
        cutoff_time: datetime,
        maximum_bytes_billed: int,
        execute_checkpoint: bool,
        checkpoint_maximum_bytes_billed: int,
    ) -> SourceProbeResult:
        try:
            transactions = self._backend.table_metadata(
                self._plan.transactions_table_id
            )
            blocks = self._backend.table_metadata(self._plan.blocks_table_id)
        except Exception:
            return self._blocked("bigquery_metadata_unavailable")

        reasons = self._metadata_reasons(transactions, blocks)
        schema_sha256 = self._combined_schema_hash(transactions, blocks)
        if reasons:
            return self._blocked(*reasons, schema_sha256=schema_sha256)

        parameters = {
            "cutoff_height": (
                cutoff_height
                if cutoff_height is not None
                else 9_223_372_036_854_775_807
            ),
            "cutoff_time": cutoff_time,
            "window_30d_start": cutoff_time - timedelta(days=30),
            "window_90d_start": cutoff_time - timedelta(days=90),
            "window_365d_start": cutoff_time - timedelta(days=365),
        }
        try:
            estimate = self._backend.dry_run(
                self._plan.address_features_sql,
                parameters,
                maximum_bytes_billed,
            )
        except Exception:
            return self._blocked(
                "bigquery_dry_run_failed", schema_sha256=schema_sha256
            )
        if (
            maximum_bytes_billed > 0
            and estimate.total_bytes_processed > maximum_bytes_billed
        ):
            return self._blocked(
                "bigquery_budget_exceeded",
                schema_sha256=schema_sha256,
                dry_run_bytes=estimate.total_bytes_processed,
            )

        if not execute_checkpoint:
            return SourceProbeResult(
                source_kind="bigquery",
                status="partial",
                schema_sha256=schema_sha256,
                dry_run_bytes=estimate.total_bytes_processed,
                script_completeness=True,
                capabilities=("address_rows", "script_hex", "source_accounting"),
                warnings=("bigquery_checkpoint_not_executed",),
            )
        if checkpoint_maximum_bytes_billed <= 0:
            return self._blocked(
                "bigquery_checkpoint_budget_missing",
                schema_sha256=schema_sha256,
                dry_run_bytes=estimate.total_bytes_processed,
            )
        try:
            checkpoint = self._backend.query_one(
                self._plan.source_checkpoint_sql,
                {
                    "as_of_date": as_of_date,
                    "finality_depth": 6,
                },
                maximum_bytes_billed=checkpoint_maximum_bytes_billed,
            )
            latest_height = self._non_negative_int(checkpoint, "latest_height")
            latest_hash = self._hash(checkpoint, "latest_hash")
            latest_time = self._timestamp(checkpoint, "latest_time")
            finalized_height = self._non_negative_int(
                checkpoint, "finalized_height"
            )
            finalized_hash = self._hash(checkpoint, "finalized_hash")
            taproot_count = self._non_negative_int(
                checkpoint, "taproot_address_count"
            )
        except Exception:
            return self._blocked(
                "bigquery_checkpoint_invalid",
                schema_sha256=schema_sha256,
                dry_run_bytes=estimate.total_bytes_processed,
            )
        if cutoff_height is not None and finalized_height != cutoff_height:
            return self._blocked(
                "bigquery_cutoff_mismatch",
                schema_sha256=schema_sha256,
                dry_run_bytes=estimate.total_bytes_processed,
            )

        warnings = (
            ("bigquery_recent_taproot_zero",) if taproot_count == 0 else ()
        )
        return SourceProbeResult(
            source_kind="bigquery",
            status="accepted",
            schema_sha256=schema_sha256,
            latest_height=latest_height,
            latest_hash=latest_hash,
            latest_time=latest_time,
            finalized_height=finalized_height,
            finalized_hash=finalized_hash,
            dry_run_bytes=estimate.total_bytes_processed,
            script_completeness=True,
            capabilities=("address_rows", "script_hex", "source_accounting"),
            warnings=warnings,
        )

    def _metadata_reasons(
        self, transactions: TableMetadata, blocks: TableMetadata
    ) -> tuple[str, ...]:
        reasons = list(
            _transaction_metadata_reasons(
                transactions=transactions,
                now=self._now,
                max_source_age=self._max_source_age,
                required_fields=_TRANSACTION_FIELDS,
                required_nested_fields={
                    "inputs": _TRANSACTION_INPUT_FIELDS,
                    "outputs": _TRANSACTION_OUTPUT_FIELDS,
                },
            )
        )

        if not _contains_contract(_field_contracts(blocks.fields), _BLOCK_FIELDS):
            reasons.append("bigquery_blocks_schema_mismatch")
        if (
            blocks.partition_field != "timestamp_month"
            or blocks.partition_type != "DAY"
        ):
            reasons.append("bigquery_blocks_not_time_partitioned")
        if self._now - blocks.modified_at > self._max_source_age:
            reasons.append("bigquery_blocks_stale")
        return tuple(sorted(set(reasons)))

    @staticmethod
    def _combined_schema_hash(
        transactions: TableMetadata, blocks: TableMetadata
    ) -> str:
        return hashlib.sha256(
            json.dumps(
                {
                    "transactions": transactions.schema_sha256,
                    "blocks": blocks.schema_sha256,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("ascii")
        ).hexdigest()

    @staticmethod
    def _non_negative_int(value: Mapping[str, object], key: str) -> int:
        candidate = value[key]
        if isinstance(candidate, bool) or not isinstance(candidate, int) or candidate < 0:
            raise BigQueryBoundaryError("BigQuery checkpoint is invalid")
        return candidate

    @staticmethod
    def _hash(value: Mapping[str, object], key: str) -> str:
        candidate = value[key]
        if not isinstance(candidate, str) or len(candidate) != 64 or candidate != candidate.lower():
            raise BigQueryBoundaryError("BigQuery checkpoint is invalid")
        try:
            bytes.fromhex(candidate)
        except ValueError as exc:
            raise BigQueryBoundaryError("BigQuery checkpoint is invalid") from exc
        return candidate

    @staticmethod
    def _timestamp(value: Mapping[str, object], key: str) -> datetime:
        candidate = value[key]
        if not isinstance(candidate, datetime) or candidate.tzinfo is None:
            raise BigQueryBoundaryError("BigQuery checkpoint is invalid")
        return candidate.astimezone(UTC)

    @staticmethod
    def _blocked(
        *reasons: str,
        schema_sha256: str | None = None,
        dry_run_bytes: int | None = None,
    ) -> SourceProbeResult:
        return SourceProbeResult(
            source_kind="bigquery",
            status="blocked",
            schema_sha256=schema_sha256,
            dry_run_bytes=dry_run_bytes,
            script_completeness=False,
            capabilities=(),
            blocking_reasons=tuple(reasons),
        )


class GoogleBigQueryBackend:
    """Lazy optional SDK adapter with explicit billing caps."""

    def __init__(self, *, billing_project: str, location: str) -> None:
        try:
            from google.cloud import bigquery
        except ImportError as exc:
            raise BigQueryDependencyMissing(
                "google-cloud-bigquery optional dependency is missing"
            ) from exc
        self._bigquery = bigquery
        self.last_query_total_bytes_processed: int | None = None
        try:
            self._client = bigquery.Client(project=billing_project, location=location)
        except Exception as exc:
            raise BigQueryCredentialsUnavailable(
                "BigQuery credentials are unavailable"
            ) from exc

    def table_metadata(self, table_id: str) -> TableMetadata:
        try:
            table = self._client.get_table(table_id)
        except Exception as exc:
            raise BigQueryBoundaryError("BigQuery metadata is unavailable") from exc
        partitioning = getattr(table, "time_partitioning", None)

        def convert_field(field: object) -> TableField:
            return TableField(
                name=field.name,
                field_type=field.field_type,
                mode=field.mode,
                fields=tuple(
                    convert_field(child)
                    for child in getattr(field, "fields", ())
                ),
            )

        return TableMetadata(
            table_id=table_id,
            fields=tuple(convert_field(field) for field in table.schema),
            partition_field=(
                getattr(partitioning, "field", None) if partitioning else None
            ),
            partition_type=(
                str(getattr(partitioning, "type_", "")).upper()
                if partitioning
                else None
            ),
            modified_at=table.modified,
        )

    def dry_run(
        self,
        sql: str,
        parameters: dict[str, object],
        maximum_bytes_billed: int,
    ) -> QueryEstimate:
        job_config = self._query_job_config(
            parameters,
            maximum_bytes_billed=maximum_bytes_billed,
            dry_run=True,
        )
        try:
            job = self._client.query(sql, job_config=job_config)
        except Exception as exc:
            raise BigQueryBoundaryError("BigQuery dry run failed") from exc
        return QueryEstimate(
            total_bytes_processed=int(job.total_bytes_processed or 0),
            cache_hit=bool(job.cache_hit),
        )

    def query_one(
        self,
        sql: str,
        parameters: dict[str, object],
        *,
        maximum_bytes_billed: int,
    ) -> Mapping[str, object]:
        if maximum_bytes_billed <= 0:
            raise BigQueryBoundaryError("BigQuery execution budget must be positive")
        job_config = self._query_job_config(
            parameters,
            maximum_bytes_billed=maximum_bytes_billed,
            dry_run=False,
        )
        try:
            rows = self._client.query(sql, job_config=job_config).result(
                page_size=1, max_results=1
            )
            row = next(iter(rows))
        except Exception as exc:
            raise BigQueryBoundaryError("BigQuery checkpoint query failed") from exc
        return dict(row.items())

    def stream_arrow_batches(
        self,
        sql: str,
        parameters: dict[str, object],
        *,
        maximum_bytes_billed: int,
        page_size: int,
    ) -> Iterator[object]:
        if maximum_bytes_billed <= 0 or page_size <= 0:
            raise BigQueryBoundaryError("BigQuery stream bounds must be positive")
        job_config = self._query_job_config(
            parameters,
            maximum_bytes_billed=maximum_bytes_billed,
            dry_run=False,
        )
        try:
            job = self._client.query(sql, job_config=job_config)
            iterator = job.result(page_size=page_size)
            for batch in iterator.to_arrow_iterable():
                yield batch
            self.last_query_total_bytes_processed = int(
                job.total_bytes_processed or 0
            )
        except Exception as exc:
            raise BigQueryBoundaryError("BigQuery Arrow stream failed") from exc

    def _query_job_config(
        self,
        parameters: dict[str, object],
        *,
        maximum_bytes_billed: int,
        dry_run: bool,
    ) -> object:
        bigquery = self._bigquery
        job_config = bigquery.QueryJobConfig(
            dry_run=dry_run,
            use_query_cache=False,
            use_legacy_sql=False,
            query_parameters=[
                self._scalar_parameter(name, value)
                for name, value in sorted(parameters.items())
            ],
        )
        if maximum_bytes_billed > 0 and not dry_run:
            job_config.maximum_bytes_billed = maximum_bytes_billed
        return job_config

    def _scalar_parameter(self, name: str, value: object) -> object:
        bigquery = self._bigquery
        if isinstance(value, datetime):
            parameter_type = "TIMESTAMP"
        elif isinstance(value, date):
            parameter_type = "DATE"
        elif isinstance(value, bool):
            parameter_type = "BOOL"
        elif isinstance(value, int):
            parameter_type = "INT64"
        elif isinstance(value, str):
            parameter_type = "STRING"
        else:
            raise BigQueryBoundaryError("Unsupported BigQuery parameter type")
        return bigquery.ScalarQueryParameter(name, parameter_type, value)
