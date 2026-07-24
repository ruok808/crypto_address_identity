"""BigQuery metadata, cost, checkpoint, and optional execution boundary."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Protocol

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


_OUTPUT_FIELDS = {
    "block_number": ("INTEGER", "NULLABLE"),
    "block_hash": ("STRING", "NULLABLE"),
    "block_timestamp": ("TIMESTAMP", "NULLABLE"),
    "transaction_hash": ("STRING", "NULLABLE"),
    "transaction_index": ("INTEGER", "NULLABLE"),
    "index": ("INTEGER", "NULLABLE"),
    "script_hex": ("STRING", "NULLABLE"),
    "type": ("STRING", "NULLABLE"),
    "addresses": ("STRING", "REPEATED"),
    "value": ("INTEGER", "NULLABLE"),
}
_INPUT_FIELDS = {
    "block_number": ("INTEGER", "NULLABLE"),
    "block_hash": ("STRING", "NULLABLE"),
    "block_timestamp": ("TIMESTAMP", "NULLABLE"),
    "transaction_hash": ("STRING", "NULLABLE"),
    "transaction_index": ("INTEGER", "NULLABLE"),
    "index": ("INTEGER", "NULLABLE"),
    "spent_transaction_hash": ("STRING", "NULLABLE"),
    "spent_output_index": ("INTEGER", "NULLABLE"),
    "type": ("STRING", "NULLABLE"),
    "addresses": ("STRING", "REPEATED"),
    "value": ("INTEGER", "NULLABLE"),
}


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
        cutoff_height: int,
        cutoff_time: datetime,
        maximum_bytes_billed: int,
        execute_checkpoint: bool,
        checkpoint_maximum_bytes_billed: int,
    ) -> SourceProbeResult:
        try:
            outputs = self._backend.table_metadata(self._plan.outputs_table_id)
            inputs = self._backend.table_metadata(self._plan.inputs_table_id)
        except Exception:
            return self._blocked("bigquery_metadata_unavailable")

        reasons = self._metadata_reasons(outputs, inputs)
        schema_sha256 = self._combined_schema_hash(outputs, inputs)
        if reasons:
            return self._blocked(*reasons, schema_sha256=schema_sha256)

        parameters = {
            "cutoff_height": cutoff_height,
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
        if finalized_height != cutoff_height:
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
        self, outputs: TableMetadata, inputs: TableMetadata
    ) -> tuple[str, ...]:
        reasons: list[str] = []
        for name, metadata, required in (
            ("outputs", outputs, _OUTPUT_FIELDS),
            ("inputs", inputs, _INPUT_FIELDS),
        ):
            fields = {
                field.name: (field.field_type, field.mode)
                for field in metadata.fields
            }
            if any(fields.get(field_name) != contract for field_name, contract in required.items()):
                reasons.append(f"bigquery_{name}_schema_mismatch")
            if (
                metadata.partition_field != "block_timestamp"
                or metadata.partition_type != "DAY"
            ):
                reasons.append(f"bigquery_{name}_not_time_partitioned")
            if self._now - metadata.modified_at > self._max_source_age:
                reasons.append(f"bigquery_{name}_stale")
        return tuple(sorted(set(reasons)))

    @staticmethod
    def _combined_schema_hash(
        outputs: TableMetadata, inputs: TableMetadata
    ) -> str:
        return hashlib.sha256(
            json.dumps(
                {
                    "outputs": outputs.schema_sha256,
                    "inputs": inputs.schema_sha256,
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
        return TableMetadata(
            table_id=table_id,
            fields=tuple(
                TableField(name=field.name, field_type=field.field_type, mode=field.mode)
                for field in table.schema
            ),
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
        if maximum_bytes_billed > 0:
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
