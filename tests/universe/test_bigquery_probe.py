from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from crypto_address_identity.universe.bigquery import (
    BigQueryProbe,
    GoogleBigQueryBackend,
    QueryEstimate,
    TableField,
    TableMetadata,
)
from crypto_address_identity.universe.query_plan import (
    BigQueryQueryPlan,
    InvalidBigQueryDataset,
)


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "bigquery_schema.json"
NOW = datetime(2026, 7, 24, 1, tzinfo=UTC)


def table_metadata() -> dict[str, TableMetadata]:
    decoded = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    return {
        name: TableMetadata(
            table_id=value["table_id"],
            fields=tuple(TableField.model_validate(field) for field in value["fields"]),
            partition_field=value["partition_field"],
            partition_type=value["partition_type"],
            modified_at=datetime.fromisoformat(
                value["modified_at"].replace("Z", "+00:00")
            ),
        )
        for name, value in decoded.items()
    }


class FakeBigQueryBackend:
    def __init__(
        self,
        *,
        tables: dict[str, TableMetadata],
        dry_run_bytes: int,
        checkpoint: dict[str, object] | None = None,
    ) -> None:
        self.tables = tables
        self.dry_run_bytes = dry_run_bytes
        self.checkpoint = checkpoint or {
            "latest_height": 900_010,
            "latest_hash": "10" * 32,
            "latest_time": datetime(2026, 7, 23, 23, tzinfo=UTC),
            "finalized_height": 900_004,
            "finalized_hash": "11" * 32,
            "taproot_address_count": 2,
        }
        self.dry_run_queries: list[str] = []
        self.query_one_queries: list[str] = []

    def table_metadata(self, table_id: str) -> TableMetadata:
        name = table_id.rsplit(".", 1)[-1]
        return self.tables[name]

    def dry_run(
        self, sql: str, parameters: dict[str, object], maximum_bytes_billed: int
    ) -> QueryEstimate:
        self.dry_run_queries.append(sql)
        assert maximum_bytes_billed >= 0
        assert parameters["cutoff_height"] == 900_004
        return QueryEstimate(total_bytes_processed=self.dry_run_bytes, cache_hit=False)

    def query_one(
        self,
        sql: str,
        parameters: dict[str, object],
        *,
        maximum_bytes_billed: int,
    ) -> dict[str, object]:
        self.query_one_queries.append(sql)
        assert maximum_bytes_billed > 0
        assert parameters["as_of_date"] == date(2026, 7, 24)
        return self.checkpoint


def test_bigquery_query_plan_is_partition_bounded_and_deterministic() -> None:
    plan = BigQueryQueryPlan.load("bigquery-public-data.crypto_bitcoin")
    repeated = BigQueryQueryPlan.load("bigquery-public-data.crypto_bitcoin")

    assert plan.address_features_sha256 == repeated.address_features_sha256
    assert "ARRAY_LENGTH(addresses) = 1" in plan.address_features_sql
    assert "(block_hash, transaction_index)" in plan.address_features_sql
    assert "@cutoff_height" in plan.address_features_sql
    assert "@window_365d_start" in plan.address_features_sql
    assert "bigquery-public-data.crypto_bitcoin.outputs" in plan.address_features_sql
    assert "LOWER(script_hex) AS script_hex" in plan.address_features_sql
    assert "LOWER(TO_HEX(" in plan.address_features_sql
    assert "missing_script_hex_rows" in plan.address_features_sql
    assert "DATE_SUB(@as_of_date, INTERVAL 7 DAY)" in plan.source_checkpoint_sql
    assert "LIMIT 1" in plan.source_checkpoint_sql
    assert "fixture-project" not in plan.address_features_sql


def test_bigquery_query_hash_changes_when_sql_changes() -> None:
    plan = BigQueryQueryPlan.load("bigquery-public-data.crypto_bitcoin")

    assert plan.hash_sql(plan.address_features_sql + "\n-- semantic change") != (
        plan.address_features_sha256
    )


@pytest.mark.parametrize(
    "dataset",
    [
        "crypto_bitcoin",
        "BigQuery-Public-Data.crypto_bitcoin",
        "project.dataset.table",
        "project.dataset;DROP TABLE outputs",
    ],
)
def test_bigquery_query_plan_rejects_unsafe_dataset(dataset: str) -> None:
    with pytest.raises(InvalidBigQueryDataset):
        BigQueryQueryPlan.load(dataset)


def test_bigquery_probe_accepts_schema_freshness_and_bounded_estimate() -> None:
    backend = FakeBigQueryBackend(tables=table_metadata(), dry_run_bytes=900)
    result = BigQueryProbe(
        backend=backend,
        dataset="bigquery-public-data.crypto_bitcoin",
        max_source_age=timedelta(hours=48),
        now=NOW,
    ).run(
        as_of_date=date(2026, 7, 24),
        cutoff_height=900_004,
        cutoff_time=datetime(2026, 7, 23, 22, tzinfo=UTC),
        maximum_bytes_billed=1_000,
        execute_checkpoint=True,
        checkpoint_maximum_bytes_billed=10_000,
    )

    assert result.status == "accepted"
    assert result.dry_run_bytes == 900
    assert result.finalized_height == 900_004
    assert result.finalized_hash == "11" * 32
    assert result.script_completeness is True
    assert len(backend.dry_run_queries) == 1
    assert len(backend.query_one_queries) == 1


def test_bigquery_probe_warns_when_recent_taproot_count_is_zero() -> None:
    backend = FakeBigQueryBackend(
        tables=table_metadata(),
        dry_run_bytes=900,
        checkpoint={
            "latest_height": 900_010,
            "latest_hash": "10" * 32,
            "latest_time": datetime(2026, 7, 23, 23, tzinfo=UTC),
            "finalized_height": 900_004,
            "finalized_hash": "11" * 32,
            "taproot_address_count": 0,
        },
    )

    result = BigQueryProbe(
        backend=backend,
        dataset="bigquery-public-data.crypto_bitcoin",
        max_source_age=timedelta(hours=48),
        now=NOW,
    ).run(
        as_of_date=date(2026, 7, 24),
        cutoff_height=900_004,
        cutoff_time=datetime(2026, 7, 23, 22, tzinfo=UTC),
        maximum_bytes_billed=1_000,
        execute_checkpoint=True,
        checkpoint_maximum_bytes_billed=10_000,
    )

    assert result.status == "accepted"
    assert "bigquery_recent_taproot_zero" in result.warnings


@pytest.mark.parametrize(
    ("mutator", "reason"),
    [
        (
            lambda tables: tables["outputs"].model_copy(
                update={
                    "fields": tuple(
                        field
                        for field in tables["outputs"].fields
                        if field.name != "script_hex"
                    )
                }
            ),
            "bigquery_outputs_schema_mismatch",
        ),
        (
            lambda tables: tables["inputs"].model_copy(
                update={"partition_field": None}
            ),
            "bigquery_inputs_not_time_partitioned",
        ),
        (
            lambda tables: tables["outputs"].model_copy(
                update={"modified_at": NOW - timedelta(days=3)}
            ),
            "bigquery_outputs_stale",
        ),
    ],
)
def test_bigquery_probe_blocks_schema_partition_or_freshness_drift(
    mutator: object, reason: str
) -> None:
    tables = table_metadata()
    changed = mutator(tables)
    tables[changed.table_id.rsplit(".", 1)[-1]] = changed
    backend = FakeBigQueryBackend(tables=tables, dry_run_bytes=900)

    result = BigQueryProbe(
        backend=backend,
        dataset="bigquery-public-data.crypto_bitcoin",
        max_source_age=timedelta(hours=48),
        now=NOW,
    ).run(
        as_of_date=date(2026, 7, 24),
        cutoff_height=900_004,
        cutoff_time=datetime(2026, 7, 23, 22, tzinfo=UTC),
        maximum_bytes_billed=1_000,
        execute_checkpoint=False,
        checkpoint_maximum_bytes_billed=0,
    )

    assert result.status == "blocked"
    assert reason in result.blocking_reasons


def test_bigquery_probe_blocks_estimate_above_cap() -> None:
    backend = FakeBigQueryBackend(tables=table_metadata(), dry_run_bytes=1_001)

    result = BigQueryProbe(
        backend=backend,
        dataset="bigquery-public-data.crypto_bitcoin",
        max_source_age=timedelta(hours=48),
        now=NOW,
    ).run(
        as_of_date=date(2026, 7, 24),
        cutoff_height=900_004,
        cutoff_time=datetime(2026, 7, 23, 22, tzinfo=UTC),
        maximum_bytes_billed=1_000,
        execute_checkpoint=False,
        checkpoint_maximum_bytes_billed=0,
    )

    assert result.status == "blocked"
    assert result.blocking_reasons == ("bigquery_budget_exceeded",)
    assert result.dry_run_bytes == 1_001


def test_google_backend_applies_byte_cap_only_to_executing_queries() -> None:
    class FakeQueryJobConfig:
        def __init__(self, **values: object) -> None:
            self.maximum_bytes_billed: int | None = None
            self.__dict__.update(values)

    class FakeBigQueryModule:
        QueryJobConfig = FakeQueryJobConfig

    backend = object.__new__(GoogleBigQueryBackend)
    backend._bigquery = FakeBigQueryModule()

    dry_run_config = backend._query_job_config(
        {},
        maximum_bytes_billed=1_000,
        dry_run=True,
    )
    execute_config = backend._query_job_config(
        {},
        maximum_bytes_billed=1_000,
        dry_run=False,
    )

    assert dry_run_config.maximum_bytes_billed is None
    assert execute_config.maximum_bytes_billed == 1_000
