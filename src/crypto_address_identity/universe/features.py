"""Bounded BigQuery Arrow materialization into an immutable universe campaign."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta
from typing import Literal

import pyarrow as pa
from pydantic import Field, field_validator, model_validator

from crypto_address_identity.chains.bitcoin import normalize_bitcoin_address
from crypto_address_identity.universe.anchors import CalibrationAnchorSnapshot
from crypto_address_identity.universe.bigquery import (
    BigQueryBackend,
    BigQueryBoundaryError,
)
from crypto_address_identity.universe.models import (
    AddressFeatureRow,
    ScriptSubjectRow,
    SourceManifest,
    UniverseCoverageCounters,
    UniverseModel,
)
from crypto_address_identity.universe.query_plan import BigQueryQueryPlan
from crypto_address_identity.universe.storage import (
    UniverseIntegrityError,
    UniverseStore,
)


class FeatureMaterializationError(RuntimeError):
    """Safe materialization failure with no source row or upstream payload."""


class BigQueryMaterializationRequest(UniverseModel):
    source_manifest: SourceManifest
    dataset: str
    maximum_bytes_billed: int = Field(ge=1)
    page_size: int = Field(default=10_000, ge=1, le=100_000)

    @field_validator("dataset")
    @classmethod
    def validate_dataset(cls, value: str) -> str:
        BigQueryQueryPlan.load(value)
        return value

    @model_validator(mode="after")
    def validate_source_contract(self) -> "BigQueryMaterializationRequest":
        plan = BigQueryQueryPlan.load(self.dataset)
        if self.source_manifest.source_kind != "bigquery":
            raise ValueError("materialization requires a BigQuery source manifest")
        if self.source_manifest.query_sha256 != plan.address_features_sha256:
            raise ValueError("source manifest query hash does not match the query plan")
        if not self.source_manifest.script_completeness:
            raise ValueError("materialization requires complete script evidence")
        return self


class FeatureMaterializationResult(UniverseModel):
    status: Literal["published"]
    campaign_id: str
    address_feature_rows: int = Field(ge=0)
    script_subject_rows: int = Field(ge=0)
    source_accounting_rows: int = Field(ge=0)
    dry_run_bytes: int = Field(ge=0)
    total_bytes_processed: int = Field(ge=0)
    provider_requests: Literal[0] = 0
    provider_points: Literal[0] = 0
    written_paths: tuple[str, ...]


class BigQueryFeatureMaterializer:
    """Consume one bounded Arrow stream and publish only after full validation."""

    def __init__(
        self,
        *,
        backend: BigQueryBackend,
        store: UniverseStore,
    ) -> None:
        self._backend = backend
        self._store = store

    def run(
        self,
        *,
        request: BigQueryMaterializationRequest,
        calibration_snapshot: CalibrationAnchorSnapshot | None = None,
    ) -> FeatureMaterializationResult:
        plan = BigQueryQueryPlan.load(request.dataset)
        parameters = {
            "cutoff_height": request.source_manifest.cutoff_height,
            "cutoff_time": request.source_manifest.cutoff_time,
            "window_30d_start": request.source_manifest.cutoff_time
            - timedelta(days=30),
            "window_90d_start": request.source_manifest.cutoff_time
            - timedelta(days=90),
            "window_365d_start": request.source_manifest.cutoff_time
            - timedelta(days=365),
        }
        try:
            estimate = self._backend.dry_run(
                plan.address_features_sql,
                parameters,
                request.maximum_bytes_billed,
            )
        except Exception as exc:
            raise FeatureMaterializationError("BigQuery dry run failed") from exc
        if estimate.total_bytes_processed > request.maximum_bytes_billed:
            raise FeatureMaterializationError(
                "BigQuery dry-run estimate exceeds the execution cap"
            )

        writer = self._store.begin_campaign(request.source_manifest)
        try:
            stream = self._backend.stream_arrow_batches(
                plan.address_features_sql,
                parameters,
                maximum_bytes_billed=request.maximum_bytes_billed,
                page_size=request.page_size,
            )
            accounting_rows = 0
            for batch in stream:
                if not isinstance(batch, pa.RecordBatch):
                    raise FeatureMaterializationError(
                        "BigQuery stream yielded a non-Arrow batch"
                    )
                if batch.num_rows > request.page_size:
                    raise FeatureMaterializationError(
                        "BigQuery stream exceeded the bounded batch size"
                    )
                scripts, features, accounting = self._validate_batch(
                    batch, request.source_manifest
                )
                if scripts:
                    writer.write_script_subjects(scripts)
                if features:
                    writer.write_address_features(features)
                for counters in accounting:
                    writer.write_source_accounting(counters)
                    accounting_rows += 1

            actual_bytes = getattr(
                self._backend, "last_query_total_bytes_processed", None
            )
            if actual_bytes is None:
                actual_bytes = estimate.total_bytes_processed
            if (
                isinstance(actual_bytes, bool)
                or not isinstance(actual_bytes, int)
                or actual_bytes < 0
                or actual_bytes > request.maximum_bytes_billed
            ):
                raise FeatureMaterializationError(
                    "BigQuery execution bytes violate the approved cap"
                )
            if calibration_snapshot is not None:
                writer.write_calibration_anchor_snapshot(calibration_snapshot)
            published = writer.publish()
        except Exception as exc:
            writer.abort()
            if isinstance(exc, FeatureMaterializationError):
                raise
            if isinstance(exc, (UniverseIntegrityError, BigQueryBoundaryError)):
                raise FeatureMaterializationError(
                    "BTC universe materialization was blocked"
                ) from exc
            raise FeatureMaterializationError(
                "BTC universe materialization failed"
            ) from exc

        return FeatureMaterializationResult(
            status="published",
            campaign_id=published.campaign_id,
            address_feature_rows=published.address_feature_rows,
            script_subject_rows=published.script_subject_rows,
            source_accounting_rows=accounting_rows,
            dry_run_bytes=estimate.total_bytes_processed,
            total_bytes_processed=actual_bytes,
            written_paths=(str(published.root),),
        )

    @staticmethod
    def _validate_batch(
        batch: pa.RecordBatch, source_manifest: SourceManifest
    ) -> tuple[
        list[ScriptSubjectRow],
        list[AddressFeatureRow],
        list[UniverseCoverageCounters],
    ]:
        scripts: list[ScriptSubjectRow] = []
        features: list[AddressFeatureRow] = []
        accounting: list[UniverseCoverageCounters] = []
        for raw_row in batch.to_pylist():
            if not isinstance(raw_row, Mapping):
                raise FeatureMaterializationError("BigQuery row is malformed")
            row = dict(raw_row)
            row_kind = row.get("row_kind")
            if row_kind == "script_subject":
                scripts.append(
                    ScriptSubjectRow.model_validate(
                        _select_model_fields(row, ScriptSubjectRow)
                    )
                )
            elif row_kind == "address_feature":
                values = _select_model_fields(row, AddressFeatureRow)
                subject = normalize_bitcoin_address(
                    str(values["normalized_address"])
                )
                values["normalized_address"] = subject.normalized_address
                values["address_id"] = subject.address_id
                values["address_type"] = subject.address_type
                feature = AddressFeatureRow.model_validate(values)
                if feature.last_seen_height > source_manifest.cutoff_height:
                    raise FeatureMaterializationError(
                        "address feature exceeds campaign cutoff height"
                    )
                if feature.last_seen_time > source_manifest.cutoff_time:
                    raise FeatureMaterializationError(
                        "address feature exceeds campaign cutoff time"
                    )
                features.append(feature)
            elif row_kind == "source_accounting":
                counters = UniverseCoverageCounters.model_validate(
                    _select_model_fields(row, UniverseCoverageCounters)
                )
                if (
                    source_manifest.script_completeness
                    and counters.missing_script_hex_rows
                ):
                    raise FeatureMaterializationError(
                        "source accounting reports missing raw scripts"
                    )
                accounting.append(counters)
            else:
                raise FeatureMaterializationError("BigQuery row_kind is invalid")
        return scripts, features, accounting


def _select_model_fields(
    row: Mapping[str, object], model: type[UniverseModel]
) -> dict[str, object]:
    return {name: row.get(name) for name in model.model_fields}
