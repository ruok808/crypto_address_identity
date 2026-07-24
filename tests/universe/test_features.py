from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from pydantic import ValidationError

from crypto_address_identity.universe.bigquery import QueryEstimate
from crypto_address_identity.universe.features import (
    BigQueryFeatureMaterializer,
    BigQueryMaterializationRequest,
    FeatureMaterializationError,
)
from crypto_address_identity.universe.models import SourceManifest
from crypto_address_identity.universe.query_plan import BigQueryQueryPlan
from crypto_address_identity.universe.storage import UniverseStore
from tests.universe.conftest import (
    BTC_ADDRESSES,
    make_accounting,
    make_feature,
    make_script,
)


ALL_COLUMNS = (
    "row_kind",
    "script_id",
    "script_hex",
    "script_type",
    "normalized_address",
    "address_id",
    "provider_enrichable",
    "feature_version",
    "address_type",
    "first_seen_height",
    "last_seen_height",
    "first_seen_time",
    "last_seen_time",
    "output_count",
    "spent_output_count",
    "transaction_count",
    "current_utxo_sats",
    "lifetime_received_sats",
    "lifetime_spent_sats",
    "max_single_output_sats",
    "max_same_tx_received_sats",
    "inflow_30d_sats",
    "outflow_30d_sats",
    "gross_flow_30d_sats",
    "inflow_90d_sats",
    "outflow_90d_sats",
    "gross_flow_90d_sats",
    "gross_flow_365d_sats",
    "direct_large_counterparty_count",
    "total_output_rows",
    "total_input_rows",
    "distinct_script_subjects",
    "standard_single_address_rows",
    "empty_address_rows",
    "multi_address_rows",
    "nonstandard_rows",
    "unmatched_input_rows",
)


def stream_row(row_kind: str, values: dict[str, object]) -> dict[str, object]:
    return {column: (row_kind if column == "row_kind" else values.get(column)) for column in ALL_COLUMNS}


def record_batch(rows: list[dict[str, object]]) -> pa.RecordBatch:
    return pa.Table.from_pylist(rows).to_batches(max_chunksize=len(rows))[0]


def feature_row(address: str, **updates: object) -> dict[str, object]:
    return stream_row(
        "address_feature",
        make_feature(address, **updates).model_dump(mode="python"),
    )


def raw_feature_row(address: str, **updates: object) -> dict[str, object]:
    values = make_feature(address).model_dump(mode="python")
    values.update(updates)
    return stream_row("address_feature", values)


def script_row(
    address: str | None, *, index: int, script_type: str = "p2pkh"
) -> dict[str, object]:
    script_hex = f"{index + 1:02x}" * 24
    return stream_row(
        "script_subject",
        make_script(
            address,
            script_hex=script_hex,
            script_type=script_type,
        ).model_dump(mode="python"),
    )


def accounting_row() -> dict[str, object]:
    return stream_row(
        "source_accounting", make_accounting(distinct_script_subjects=6).model_dump()
    )


class FakeBigQueryBackend:
    def __init__(
        self,
        *,
        dry_run_bytes: int,
        result_batches: list[pa.RecordBatch],
        total_bytes_processed: int,
        fail_after_batches: int | None = None,
    ) -> None:
        self.dry_run_bytes = dry_run_bytes
        self.result_batches = result_batches
        self.last_query_total_bytes_processed = total_bytes_processed
        self.fail_after_batches = fail_after_batches
        self.stream_calls = 0
        self.page_sizes: list[int] = []

    def dry_run(
        self, sql: str, parameters: dict[str, object], maximum_bytes_billed: int
    ) -> QueryEstimate:
        assert maximum_bytes_billed > 0
        return QueryEstimate(total_bytes_processed=self.dry_run_bytes, cache_hit=False)

    def stream_arrow_batches(
        self,
        sql: str,
        parameters: dict[str, object],
        *,
        maximum_bytes_billed: int,
        page_size: int,
    ) -> Iterator[pa.RecordBatch]:
        self.stream_calls += 1
        self.page_sizes.append(page_size)
        for index, batch in enumerate(self.result_batches):
            if self.fail_after_batches is not None and index >= self.fail_after_batches:
                raise RuntimeError("fixture stream failure")
            yield batch


def materialization_request(
    *, maximum_bytes_billed: int = 1_000
) -> BigQueryMaterializationRequest:
    plan = BigQueryQueryPlan.load("bigquery-public-data.crypto_bitcoin")
    return BigQueryMaterializationRequest(
        source_manifest=SourceManifest(
            campaign_id="btc-20260724",
            source_kind="bigquery",
            source_revision="fixture-v1",
            cutoff_height=900_000,
            cutoff_hash="01" * 32,
            cutoff_time=datetime(2026, 7, 24, tzinfo=UTC),
            schema_sha256="02" * 32,
            query_sha256=plan.address_features_sha256,
            source_capabilities=("address_rows", "script_hex", "source_accounting"),
            script_completeness=True,
        ),
        dataset="bigquery-public-data.crypto_bitcoin",
        maximum_bytes_billed=maximum_bytes_billed,
        page_size=100,
    )


def feature_batches() -> list[pa.RecordBatch]:
    return [
        record_batch(
            [
                feature_row(BTC_ADDRESSES[0]),
                feature_row(BTC_ADDRESSES[1]),
                script_row(BTC_ADDRESSES[0], index=0),
                script_row(BTC_ADDRESSES[1], index=1),
                script_row(None, index=2, script_type="op_return"),
                accounting_row(),
            ]
        ),
        record_batch(
            [
                feature_row(BTC_ADDRESSES[2]),
                feature_row(BTC_ADDRESSES[3]),
                script_row(BTC_ADDRESSES[2], index=3),
                script_row(BTC_ADDRESSES[3], index=4),
                script_row(None, index=5, script_type="bare_multisig"),
            ]
        ),
    ]


def test_feature_materializer_streams_batches_and_accounts_every_source_row(
    tmp_path: Path,
) -> None:
    backend = FakeBigQueryBackend(
        dry_run_bytes=900,
        result_batches=feature_batches(),
        total_bytes_processed=900,
    )
    result = BigQueryFeatureMaterializer(
        backend=backend,
        store=UniverseStore(tmp_path / "universe"),
    ).run(request=materialization_request(maximum_bytes_billed=1_000))

    assert result.status == "published"
    assert result.address_feature_rows == 4
    assert result.script_subject_rows == 6
    assert result.total_bytes_processed == 900
    assert result.provider_requests == 0
    assert result.provider_points == 0
    assert backend.stream_calls == 1
    assert backend.page_sizes == [100]


def test_materialization_request_rejects_zero_execution_budget() -> None:
    with pytest.raises(ValidationError):
        materialization_request(maximum_bytes_billed=0)


def test_materializer_reports_actual_bytes_above_estimate_but_below_cap(
    tmp_path: Path,
) -> None:
    backend = FakeBigQueryBackend(
        dry_run_bytes=800,
        result_batches=feature_batches(),
        total_bytes_processed=950,
    )

    result = BigQueryFeatureMaterializer(
        backend=backend,
        store=UniverseStore(tmp_path / "universe"),
    ).run(request=materialization_request(maximum_bytes_billed=1_000))

    assert result.dry_run_bytes == 800
    assert result.total_bytes_processed == 950


@pytest.mark.parametrize(
    "bad_row",
    [
        lambda: raw_feature_row(BTC_ADDRESSES[0], current_utxo_sats=-1),
        lambda: raw_feature_row(BTC_ADDRESSES[0], last_seen_height=900_001),
        lambda: raw_feature_row(
            BTC_ADDRESSES[0],
            last_seen_time=datetime(2026, 7, 25, tzinfo=UTC),
        ),
    ],
)
def test_materializer_blocks_malformed_or_out_of_campaign_rows(
    tmp_path: Path, bad_row: object
) -> None:
    backend = FakeBigQueryBackend(
        dry_run_bytes=900,
        result_batches=[
            record_batch(
                [
                    bad_row(),
                    script_row(BTC_ADDRESSES[0], index=0),
                    accounting_row(),
                ]
            )
        ],
        total_bytes_processed=900,
    )

    with pytest.raises(FeatureMaterializationError):
        BigQueryFeatureMaterializer(
            backend=backend,
            store=UniverseStore(tmp_path / "universe"),
        ).run(request=materialization_request())

    assert not (tmp_path / "universe" / "campaigns" / "btc-20260724").exists()


@pytest.mark.parametrize("duplicate_kind", ["address", "script"])
def test_materializer_rejects_duplicate_ids_across_batches(
    tmp_path: Path, duplicate_kind: str
) -> None:
    repeated = (
        feature_row(BTC_ADDRESSES[0])
        if duplicate_kind == "address"
        else script_row(BTC_ADDRESSES[0], index=0)
    )
    other = (
        script_row(BTC_ADDRESSES[0], index=0)
        if duplicate_kind == "address"
        else feature_row(BTC_ADDRESSES[0])
    )
    backend = FakeBigQueryBackend(
        dry_run_bytes=900,
        result_batches=[
            record_batch([repeated, other, accounting_row()]),
            record_batch([repeated]),
        ],
        total_bytes_processed=900,
    )

    with pytest.raises(FeatureMaterializationError):
        BigQueryFeatureMaterializer(
            backend=backend,
            store=UniverseStore(tmp_path / "universe"),
        ).run(request=materialization_request())

    assert not any((tmp_path / "universe" / ".staging").iterdir())


def test_materializer_aborts_when_batch_iterator_fails(tmp_path: Path) -> None:
    backend = FakeBigQueryBackend(
        dry_run_bytes=900,
        result_batches=feature_batches(),
        total_bytes_processed=900,
        fail_after_batches=1,
    )

    with pytest.raises(FeatureMaterializationError):
        BigQueryFeatureMaterializer(
            backend=backend,
            store=UniverseStore(tmp_path / "universe"),
        ).run(request=materialization_request())

    assert not (tmp_path / "universe" / "campaigns" / "btc-20260724").exists()
    assert not any((tmp_path / "universe" / ".staging").iterdir())


def test_materializer_does_not_create_identity_or_provider_runtime_paths(
    tmp_path: Path,
) -> None:
    backend = FakeBigQueryBackend(
        dry_run_bytes=900,
        result_batches=feature_batches(),
        total_bytes_processed=900,
    )
    BigQueryFeatureMaterializer(
        backend=backend,
        store=UniverseStore(tmp_path / "universe"),
    ).run(request=materialization_request())

    assert not (tmp_path / "identity.sqlite3").exists()
    assert not (tmp_path / "raw").exists()
    assert not (tmp_path / "exports").exists()
