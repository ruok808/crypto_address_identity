"""Strict V2-S candidate schema and free BigQuery cost checkpoint."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from importlib import resources
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator

from crypto_address_identity.universe.bigquery import (
    BigQueryBackend,
    MonthlyQueryUsage,
    _transaction_metadata_reasons,
)
from crypto_address_identity.universe.candidate_population_contract_v2 import (
    PINNED_SCHEMA_SHA256,
    validate_candidate_population_contract_v2,
)
from crypto_address_identity.universe.models import UniverseModel
from crypto_address_identity.universe.query_plan import BigQueryQueryPlan


STRICT_V2_S_QUERY_KIND = "btc_strict_v2_s_candidate_materialization"
STRICT_V2_S_POLICY_VERSION = "btc_importance_v2"
STRICT_V2_S_VARIANT = "V2-S"
STRICT_V2_S_RESULT_SCHEMA_VERSION = "btc_strict_v2_s_candidate_schema_v1"
STRICT_V2_S_CUTOFF_HEIGHT = 959_187
STRICT_V2_S_CUTOFF_TIME = datetime(
    2026,
    7,
    24,
    23,
    59,
    59,
    999999,
    tzinfo=UTC,
)
STRICT_V2_S_MAXIMUM_BYTES_BILLED = 650_000_000_000
EXPECTED_OUTPUT_DEFINED_ADDRESS_COUNT = 1_557_941_780
EXPECTED_POSITIVE_VALUE_ADDRESS_COUNT = 1_531_420_608
EXPECTED_STRICT_V2_S_COARSE_COUNT = 1_090_411
EXPECTED_STRICT_V2_S_P0_COUNT = 21_736
EXPECTED_STRICT_V2_S_P1_COUNT = 2_143
EXPECTED_STRICT_V2_S_EDGE_COUNT = 133_730
EXPECTED_STRICT_V2_S_COARSE_OTHER_COUNT = 932_802

PINNED_STRICT_V2_S_QUERY_SHA256 = (
    "5cb4990e01b4983910d0d813b67e148b985111108e6a26a251fadf95b18506d3"
)
STRICT_V2_S_CANDIDATE_SCHEMA_SHA256 = (
    "ae5e08ff63b55f9bce3f5bbd17f858f2a29ec3da85223fd2f3c6675043883683"
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_POLICY_START = "WITH\n"
_POLICY_END = "strict_p0_overlap AS (\n"
_POLICY_TEMPLATE_MARKER = "{{STRICT_V2_S_POLICY_CTES}}"
_TRANSACTION_FIELDS = {
    "block_number": ("INTEGER", "REQUIRED"),
    "block_hash": ("STRING", "REQUIRED"),
    "hash": ("STRING", "REQUIRED"),
    "block_timestamp": ("TIMESTAMP", "REQUIRED"),
    "block_timestamp_month": ("DATE", "REQUIRED"),
    "inputs": ("RECORD", "REPEATED"),
    "outputs": ("RECORD", "REPEATED"),
}
_IO_FIELDS = {
    "addresses": ("STRING", "REPEATED"),
    "value": ("NUMERIC", "NULLABLE"),
}


@dataclass(frozen=True)
class CandidateSchemaField:
    name: str
    bigquery_type: str
    mode: Literal["REQUIRED"] = "REQUIRED"


STRICT_V2_S_CANDIDATE_SCHEMA = (
    CandidateSchemaField("normalized_address", "STRING"),
    CandidateSchemaField("candidate_tier", "STRING"),
    CandidateSchemaField("tier_rank", "INT64"),
    CandidateSchemaField("address_bucket", "INT64"),
    CandidateSchemaField("v2_chain_score", "INT64"),
    CandidateSchemaField("strict_p0_mask", "INT64"),
    CandidateSchemaField("receipt_support_mask", "INT64"),
    CandidateSchemaField("current_utxo_sats", "BIGNUMERIC"),
    CandidateSchemaField("lifetime_received_sats", "BIGNUMERIC"),
    CandidateSchemaField("residual_gross_90d_sats", "BIGNUMERIC"),
    CandidateSchemaField(
        "max_same_tx_received_lifetime_sats",
        "BIGNUMERIC",
    ),
    CandidateSchemaField("max_same_tx_received_365d_sats", "BIGNUMERIC"),
    CandidateSchemaField("max_same_tx_received_90d_sats", "BIGNUMERIC"),
    CandidateSchemaField(
        "same_tx_receive_ge_500_btc_90d_count",
        "INT64",
    ),
    CandidateSchemaField(
        "same_tx_receive_ge_500_btc_365d_count",
        "INT64",
    ),
    CandidateSchemaField("active_tx_90d_count", "INT64"),
    CandidateSchemaField("active_day_90d_count", "INT64"),
    CandidateSchemaField("active_tx_365d_count", "INT64"),
    CandidateSchemaField("active_day_365d_count", "INT64"),
    CandidateSchemaField("last_seen_time", "TIMESTAMP"),
    CandidateSchemaField("candidate_row_sha256", "STRING"),
)


def candidate_schema_sha256() -> str:
    payload = {
        "schema_version": STRICT_V2_S_RESULT_SCHEMA_VERSION,
        "fields": [asdict(field) for field in STRICT_V2_S_CANDIDATE_SCHEMA],
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class StrictV2SMaterializationQueryPlan:
    dataset: str
    transactions_table_id: str
    sql: str
    query_sha256: str

    @classmethod
    def load(cls, dataset: str) -> "StrictV2SMaterializationQueryPlan":
        base = BigQueryQueryPlan.load(dataset)
        source_sql = base.candidate_statistics_v2_sql
        if (
            source_sql.count(_POLICY_START) != 1
            or source_sql.count(_POLICY_END) != 1
        ):
            raise ValueError("Strict V2-S policy SQL boundary is invalid")
        policy_ctes = source_sql.split(_POLICY_START, 1)[1].split(
            _POLICY_END,
            1,
        )[0]
        template = (
            resources.files("crypto_address_identity.universe")
            .joinpath(
                "sql/bigquery/candidate_materialization_v2_s.sql"
            )
            .read_text(encoding="utf-8")
        )
        if template.count(_POLICY_TEMPLATE_MARKER) != 1:
            raise ValueError("Strict V2-S materialization marker is invalid")
        sql = template.replace(_POLICY_TEMPLATE_MARKER, policy_ctes)
        if "{{" in sql or "}}" in sql:
            raise ValueError("Strict V2-S SQL contains an unresolved marker")
        return cls(
            dataset=dataset,
            transactions_table_id=base.transactions_table_id,
            sql=sql,
            query_sha256=BigQueryQueryPlan.hash_sql(sql),
        )


class StrictV2SMaterializationCheckpoint(UniverseModel):
    status: Literal["dry_run", "checkpoint_passed", "blocked"]
    source_kind: Literal["bigquery"] = "bigquery"
    query_kind: Literal["btc_strict_v2_s_candidate_materialization"] = (
        "btc_strict_v2_s_candidate_materialization"
    )
    policy_version: Literal["btc_importance_v2"] = "btc_importance_v2"
    variant: Literal["V2-S"] = "V2-S"
    read_only: Literal[True] = True
    execution_authorized: Literal[False] = False
    candidate_materialization_allowed: Literal[False] = False
    candidate_rows_returned: Literal[0] = 0
    cutoff_height: Literal[959_187] = STRICT_V2_S_CUTOFF_HEIGHT
    cutoff_time: datetime = STRICT_V2_S_CUTOFF_TIME
    query_sha256: str
    result_schema_version: Literal[
        "btc_strict_v2_s_candidate_schema_v1"
    ] = STRICT_V2_S_RESULT_SCHEMA_VERSION
    result_schema_sha256: str
    source_schema_sha256: str | None = None
    population_contract_status: Literal[
        "not_checked",
        "accepted",
        "blocked",
    ]
    receipt_reads: int = Field(ge=0, le=2)
    network_requests: int = Field(ge=0, le=3)
    dry_run_bytes: int | None = Field(default=None, ge=0)
    future_maximum_bytes_billed: Literal[650_000_000_000] = (
        STRICT_V2_S_MAXIMUM_BYTES_BILLED
    )
    monthly_processing_budget_bytes: int | None = Field(default=None, gt=0)
    reserve_bytes: int | None = Field(default=None, ge=0)
    successful_query_jobs: int | None = Field(default=None, ge=0)
    month_to_date_billed_bytes: int | None = Field(default=None, ge=0)
    projected_month_to_date_bytes: int | None = Field(default=None, ge=0)
    projected_reserve_bytes: int | None = None
    within_budget: bool | None = None
    expected_output_defined_address_count: Literal[1_557_941_780] = (
        EXPECTED_OUTPUT_DEFINED_ADDRESS_COUNT
    )
    expected_positive_value_address_count: Literal[1_531_420_608] = (
        EXPECTED_POSITIVE_VALUE_ADDRESS_COUNT
    )
    expected_coarse_candidate_count: Literal[1_090_411] = (
        EXPECTED_STRICT_V2_S_COARSE_COUNT
    )
    expected_p0_count: Literal[21_736] = EXPECTED_STRICT_V2_S_P0_COUNT
    expected_p1_count: Literal[2_143] = EXPECTED_STRICT_V2_S_P1_COUNT
    expected_edge_count: Literal[133_730] = EXPECTED_STRICT_V2_S_EDGE_COUNT
    expected_coarse_other_count: Literal[932_802] = (
        EXPECTED_STRICT_V2_S_COARSE_OTHER_COUNT
    )
    blocking_reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    provider_requests: Literal[0] = 0
    provider_points: Literal[0] = 0
    written_paths: tuple[()] = ()

    @field_validator(
        "cutoff_time",
    )
    @classmethod
    def validate_cutoff_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("cutoff time must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator(
        "query_sha256",
        "result_schema_sha256",
        "source_schema_sha256",
    )
    @classmethod
    def validate_hash(cls, value: str | None) -> str | None:
        if value is not None and not _SHA256_RE.fullmatch(value):
            raise ValueError("checkpoint hashes must be lower-case SHA-256")
        return value

    @field_validator("blocking_reasons", "warnings")
    @classmethod
    def canonicalize_reasons(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted(set(value)))


def preview_strict_v2_s_materialization_checkpoint(
    *,
    dataset: str,
) -> StrictV2SMaterializationCheckpoint:
    plan = StrictV2SMaterializationQueryPlan.load(dataset)
    local_reasons = _local_contract_reasons(plan)
    return StrictV2SMaterializationCheckpoint(
        status="blocked" if local_reasons else "dry_run",
        query_sha256=plan.query_sha256,
        result_schema_sha256=candidate_schema_sha256(),
        population_contract_status="not_checked",
        receipt_reads=0,
        network_requests=0,
        blocking_reasons=local_reasons,
        warnings=("free_cost_checkpoint_not_executed",),
    )


class BigQueryStrictV2SMaterializationCostProbe:
    """Validate local contracts, then perform one free BigQuery dry run."""

    def __init__(
        self,
        *,
        backend: BigQueryBackend,
        dataset: str,
        receipt_root: Path,
        max_source_age: timedelta,
        now: datetime | None = None,
    ) -> None:
        self._backend = backend
        self._plan = StrictV2SMaterializationQueryPlan.load(dataset)
        self._receipt_root = receipt_root
        if max_source_age <= timedelta(0):
            raise ValueError("max source age must be positive")
        self._max_source_age = max_source_age
        timestamp = now or datetime.now(UTC)
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("probe now must be timezone-aware")
        self._now = timestamp.astimezone(UTC)

    def run(
        self,
        *,
        expected_query_sha256: str,
        expected_result_schema_sha256: str,
        monthly_processing_budget_bytes: int,
        reserve_bytes: int,
    ) -> StrictV2SMaterializationCheckpoint:
        expected_query_sha256 = _validated_sha256(
            expected_query_sha256,
            field_name="expected query SHA-256",
        )
        expected_result_schema_sha256 = _validated_sha256(
            expected_result_schema_sha256,
            field_name="expected result schema SHA-256",
        )
        if monthly_processing_budget_bytes <= 0:
            raise ValueError("monthly processing budget must be positive")
        if reserve_bytes < 0 or reserve_bytes >= monthly_processing_budget_bytes:
            raise ValueError("reserve must be non-negative and below budget")
        local_reasons = _local_contract_reasons(self._plan)
        if local_reasons:
            return self._blocked(
                budget=monthly_processing_budget_bytes,
                reserve=reserve_bytes,
                reasons=local_reasons,
            )
        if expected_query_sha256 != self._plan.query_sha256:
            return self._blocked(
                budget=monthly_processing_budget_bytes,
                reserve=reserve_bytes,
                reasons=(
                    "strict_v2_s_materialization_query_hash_mismatch",
                ),
            )
        if expected_result_schema_sha256 != candidate_schema_sha256():
            return self._blocked(
                budget=monthly_processing_budget_bytes,
                reserve=reserve_bytes,
                reasons=(
                    "strict_v2_s_materialization_result_schema_hash_mismatch",
                ),
            )

        population = validate_candidate_population_contract_v2(
            receipt_root=self._receipt_root,
        )
        population_reasons = _population_contract_reasons(population)
        if population_reasons:
            return self._blocked(
                budget=monthly_processing_budget_bytes,
                reserve=reserve_bytes,
                reasons=population_reasons,
                population_status="blocked",
                receipt_reads=population.receipt_reads,
            )

        try:
            metadata = self._backend.table_metadata(
                self._plan.transactions_table_id
            )
        except Exception:
            return self._blocked(
                budget=monthly_processing_budget_bytes,
                reserve=reserve_bytes,
                reasons=("strict_v2_s_bigquery_metadata_unavailable",),
                population_status="accepted",
                receipt_reads=population.receipt_reads,
                network_requests=1,
            )

        metadata_reasons = list(
            _transaction_metadata_reasons(
                transactions=metadata,
                now=self._now,
                max_source_age=self._max_source_age,
                required_fields=_TRANSACTION_FIELDS,
                required_nested_fields={
                    "inputs": _IO_FIELDS,
                    "outputs": _IO_FIELDS,
                },
            )
        )
        warnings: list[str] = []
        if "bigquery_transactions_stale" in metadata_reasons:
            metadata_reasons.remove("bigquery_transactions_stale")
            warnings.append(
                "strict_v2_s_source_metadata_stale_for_cost_checkpoint"
            )
        if metadata.schema_sha256 != PINNED_SCHEMA_SHA256:
            metadata_reasons.append(
                "strict_v2_s_source_schema_hash_mismatch"
            )
        if metadata_reasons:
            return self._blocked(
                budget=monthly_processing_budget_bytes,
                reserve=reserve_bytes,
                reasons=tuple(metadata_reasons),
                warnings=tuple(warnings),
                source_schema_sha256=metadata.schema_sha256,
                population_status="accepted",
                receipt_reads=population.receipt_reads,
                network_requests=1,
            )
        if self._now - metadata.modified_at > timedelta(hours=24):
            warnings.append(
                "strict_v2_s_source_metadata_older_than_24h"
            )

        month_start, month_end = _month_bounds(self._now)
        try:
            usage = self._backend.monthly_successful_query_usage(
                month_start=month_start,
                month_end=month_end,
            )
        except Exception:
            return self._blocked(
                budget=monthly_processing_budget_bytes,
                reserve=reserve_bytes,
                reasons=("strict_v2_s_monthly_usage_unavailable",),
                warnings=tuple(warnings),
                source_schema_sha256=metadata.schema_sha256,
                population_status="accepted",
                receipt_reads=population.receipt_reads,
                network_requests=2,
            )

        try:
            estimate = self._backend.dry_run(
                self._plan.sql,
                {
                    "cutoff_height": STRICT_V2_S_CUTOFF_HEIGHT,
                    "cutoff_time": STRICT_V2_S_CUTOFF_TIME,
                },
                0,
            )
        except Exception:
            return self._blocked(
                budget=monthly_processing_budget_bytes,
                reserve=reserve_bytes,
                reasons=("strict_v2_s_bigquery_dry_run_failed",),
                warnings=tuple(warnings),
                source_schema_sha256=metadata.schema_sha256,
                population_status="accepted",
                receipt_reads=population.receipt_reads,
                network_requests=3,
                usage=usage,
            )

        projected = usage.total_bytes_billed + estimate.total_bytes_processed
        projected_reserve = monthly_processing_budget_bytes - projected
        blocking: list[str] = []
        if estimate.total_bytes_processed > STRICT_V2_S_MAXIMUM_BYTES_BILLED:
            blocking.append(
                "strict_v2_s_materialization_dry_run_limit_exceeded"
            )
        if projected > monthly_processing_budget_bytes:
            blocking.append(
                "strict_v2_s_materialization_monthly_budget_exceeded"
            )
        if projected_reserve < reserve_bytes:
            blocking.append(
                "strict_v2_s_materialization_monthly_reserve_insufficient"
            )
        return StrictV2SMaterializationCheckpoint(
            status="blocked" if blocking else "checkpoint_passed",
            query_sha256=self._plan.query_sha256,
            result_schema_sha256=candidate_schema_sha256(),
            source_schema_sha256=metadata.schema_sha256,
            population_contract_status="accepted",
            receipt_reads=population.receipt_reads,
            network_requests=3,
            dry_run_bytes=estimate.total_bytes_processed,
            monthly_processing_budget_bytes=monthly_processing_budget_bytes,
            reserve_bytes=reserve_bytes,
            successful_query_jobs=usage.successful_query_jobs,
            month_to_date_billed_bytes=usage.total_bytes_billed,
            projected_month_to_date_bytes=projected,
            projected_reserve_bytes=projected_reserve,
            within_budget=not blocking,
            blocking_reasons=tuple(blocking),
            warnings=tuple(warnings),
        )

    def _blocked(
        self,
        *,
        budget: int,
        reserve: int,
        reasons: tuple[str, ...],
        population_status: Literal["not_checked", "accepted", "blocked"] = (
            "not_checked"
        ),
        receipt_reads: int = 0,
        network_requests: int = 0,
        warnings: tuple[str, ...] = (),
        source_schema_sha256: str | None = None,
        usage: MonthlyQueryUsage | None = None,
    ) -> StrictV2SMaterializationCheckpoint:
        return StrictV2SMaterializationCheckpoint(
            status="blocked",
            query_sha256=self._plan.query_sha256,
            result_schema_sha256=candidate_schema_sha256(),
            source_schema_sha256=source_schema_sha256,
            population_contract_status=population_status,
            receipt_reads=receipt_reads,
            network_requests=network_requests,
            monthly_processing_budget_bytes=budget,
            reserve_bytes=reserve,
            successful_query_jobs=getattr(
                usage,
                "successful_query_jobs",
                None,
            ),
            month_to_date_billed_bytes=getattr(
                usage,
                "total_bytes_billed",
                None,
            ),
            within_budget=None,
            blocking_reasons=reasons,
            warnings=warnings,
        )


def _population_contract_reasons(population: object) -> tuple[str, ...]:
    strict = getattr(population, "strict_capacity", None)
    valid = (
        getattr(population, "status", None) == "accepted"
        and getattr(population, "allow_materialization_design", False)
        and getattr(population, "policy_denominator", None) == "positive_value"
        and getattr(population, "output_defined_standard_address_count", None)
        == EXPECTED_OUTPUT_DEFINED_ADDRESS_COUNT
        and getattr(population, "positive_value_standard_address_count", None)
        == EXPECTED_POSITIVE_VALUE_ADDRESS_COUNT
        and strict is not None
        and strict.chain_p0_union_count == EXPECTED_STRICT_V2_S_P0_COUNT
        and strict.chain_p1_count == EXPECTED_STRICT_V2_S_P1_COUNT
        and strict.edge_upgrade_frontier_count
        == EXPECTED_STRICT_V2_S_EDGE_COUNT
        and strict.coarse_candidate_union_count
        == EXPECTED_STRICT_V2_S_COARSE_COUNT
    )
    if valid:
        return ()
    return ("strict_v2_s_population_contract_not_accepted",)


def _local_contract_reasons(
    plan: StrictV2SMaterializationQueryPlan,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if plan.query_sha256 != PINNED_STRICT_V2_S_QUERY_SHA256:
        reasons.append("strict_v2_s_pinned_query_drift")
    if candidate_schema_sha256() != STRICT_V2_S_CANDIDATE_SCHEMA_SHA256:
        reasons.append("strict_v2_s_pinned_result_schema_drift")
    return tuple(sorted(reasons))


def _validated_sha256(value: str, *, field_name: str) -> str:
    if not _SHA256_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be a lower-case SHA-256")
    return value


def _month_bounds(value: datetime) -> tuple[datetime, datetime]:
    start = datetime(value.year, value.month, 1, tzinfo=UTC)
    if value.month == 12:
        end = datetime(value.year + 1, 1, 1, tzinfo=UTC)
    else:
        end = datetime(value.year, value.month + 1, 1, tzinfo=UTC)
    return start, end
