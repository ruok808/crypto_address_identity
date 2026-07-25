from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from crypto_address_identity.universe.bigquery import (
    MonthlyQueryUsage,
    QueryEstimate,
)
from crypto_address_identity.universe.candidate_materialization_v2_s import (
    EXPECTED_STRICT_V2_S_COARSE_COUNT,
    EXPECTED_STRICT_V2_S_EDGE_COUNT,
    EXPECTED_STRICT_V2_S_P0_COUNT,
    EXPECTED_STRICT_V2_S_P1_COUNT,
    PINNED_STRICT_V2_S_QUERY_SHA256,
    STRICT_V2_S_CANDIDATE_SCHEMA,
    STRICT_V2_S_CANDIDATE_SCHEMA_SHA256,
    BigQueryStrictV2SMaterializationCostProbe,
    StrictV2SMaterializationQueryPlan,
    candidate_schema_sha256,
    preview_strict_v2_s_materialization_checkpoint,
)
from tests.universe.test_candidate_execution_v2 import (
    PinnedV2TableMetadata,
)
from tests.universe.test_candidate_population_contract_v2 import (
    _v1_receipt,
    _v2_receipt,
    _write_pair,
)


NOW = datetime(2026, 7, 25, 1, tzinfo=UTC)
CUTOFF_TIME = datetime(2026, 7, 24, 23, 59, 59, 999999, tzinfo=UTC)
DATASET = "bigquery-public-data.crypto_bitcoin"


def _write_exact_receipts(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    return _write_pair(
        root,
        monkeypatch,
        v1=_v1_receipt(output_defined_count=1_557_941_780),
        v2=_v2_receipt(
            positive_value_count=1_531_420_608,
            p0_count=EXPECTED_STRICT_V2_S_P0_COUNT,
            p1_count=EXPECTED_STRICT_V2_S_P1_COUNT,
            edge_count=EXPECTED_STRICT_V2_S_EDGE_COUNT,
            coarse_count=EXPECTED_STRICT_V2_S_COARSE_COUNT,
        ),
    )


class FakeStrictV2SBackend:
    def __init__(
        self,
        *,
        dry_run_bytes: int = 637_999_682_243,
        successful_query_jobs: int = 5,
        billed_bytes: int = 838_768_525_312,
        metadata: object | None = None,
    ) -> None:
        self.dry_run_bytes = dry_run_bytes
        self.successful_query_jobs = successful_query_jobs
        self.billed_bytes = billed_bytes
        self.metadata = metadata or PinnedV2TableMetadata()
        self.calls: list[str] = []

    def table_metadata(self, table_id: str):
        self.calls.append("table_metadata")
        assert table_id == "bigquery-public-data.crypto_bitcoin.transactions"
        return self.metadata

    def monthly_successful_query_usage(
        self,
        *,
        month_start: datetime,
        month_end: datetime,
    ) -> MonthlyQueryUsage:
        self.calls.append("monthly_successful_query_usage")
        assert month_start == datetime(2026, 7, 1, tzinfo=UTC)
        assert month_end == datetime(2026, 8, 1, tzinfo=UTC)
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
        assert parameters == {
            "cutoff_height": 959_187,
            "cutoff_time": CUTOFF_TIME,
        }
        assert maximum_bytes_billed == 0
        return QueryEstimate(
            total_bytes_processed=self.dry_run_bytes,
            cache_hit=False,
        )


def _probe(
    *,
    backend: FakeStrictV2SBackend,
    receipt_root: Path,
) -> BigQueryStrictV2SMaterializationCostProbe:
    return BigQueryStrictV2SMaterializationCostProbe(
        backend=backend,
        dataset=DATASET,
        receipt_root=receipt_root,
        max_source_age=timedelta(hours=48),
        now=NOW,
    )


def _run_probe(
    probe: BigQueryStrictV2SMaterializationCostProbe,
    *,
    query_sha256: str = PINNED_STRICT_V2_S_QUERY_SHA256,
    schema_sha256: str = STRICT_V2_S_CANDIDATE_SCHEMA_SHA256,
    budget_bytes: int = 2_000_000_000_000,
    reserve_bytes: int = 250_000_000_000,
):
    return probe.run(
        expected_query_sha256=query_sha256,
        expected_result_schema_sha256=schema_sha256,
        monthly_processing_budget_bytes=budget_bytes,
        reserve_bytes=reserve_bytes,
    )


def test_strict_v2_s_sql_is_fixed_address_only_and_destination_free() -> None:
    plan = StrictV2SMaterializationQueryPlan.load(DATASET)
    repeated = StrictV2SMaterializationQueryPlan.load(DATASET)

    assert plan.query_sha256 == repeated.query_sha256
    assert plan.query_sha256 == PINNED_STRICT_V2_S_QUERY_SHA256
    assert plan.sql.count("bigquery-public-data.crypto_bitcoin.transactions") == 1
    assert "WHERE strict_is_coarse" in plan.sql
    assert "WHEN strict_is_p0 THEN 'p0'" in plan.sql
    assert "WHEN strict_is_p1 THEN 'p1'" in plan.sql
    assert "WHEN strict_is_edge THEN 'edge'" in plan.sql
    assert "ELSE 'coarse_other'" in plan.sql
    assert "FARM_FINGERPRINT(normalized_address)" in plan.sql
    assert "candidate_row_sha256" in plan.sql
    assert "ORDER BY tier_rank, address_bucket, normalized_address" in plan.sql
    upper = plan.sql.upper()
    for forbidden in (
        "CREATE TABLE",
        "CREATE OR REPLACE",
        "INSERT INTO",
        "MERGE INTO",
        "UPDATE ",
        "DELETE FROM",
        "EXPORT DATA",
        "DROP TABLE",
    ):
        assert forbidden not in upper


def test_strict_v2_s_result_schema_is_fixed_and_excludes_tx_identifiers() -> None:
    field_names = tuple(field.name for field in STRICT_V2_S_CANDIDATE_SCHEMA)

    assert field_names == (
        "normalized_address",
        "candidate_tier",
        "tier_rank",
        "address_bucket",
        "v2_chain_score",
        "strict_p0_mask",
        "receipt_support_mask",
        "current_utxo_sats",
        "lifetime_received_sats",
        "residual_gross_90d_sats",
        "max_same_tx_received_lifetime_sats",
        "max_same_tx_received_365d_sats",
        "max_same_tx_received_90d_sats",
        "same_tx_receive_ge_500_btc_90d_count",
        "same_tx_receive_ge_500_btc_365d_count",
        "active_tx_90d_count",
        "active_day_90d_count",
        "active_tx_365d_count",
        "active_day_365d_count",
        "last_seen_time",
        "candidate_row_sha256",
    )
    assert candidate_schema_sha256() == STRICT_V2_S_CANDIDATE_SCHEMA_SHA256
    assert len(STRICT_V2_S_CANDIDATE_SCHEMA_SHA256) == 64
    assert all(field.mode == "REQUIRED" for field in STRICT_V2_S_CANDIDATE_SCHEMA)
    assert all(
        field.bigquery_type == "BIGNUMERIC"
        for field in STRICT_V2_S_CANDIDATE_SCHEMA
        if field.name.endswith("_sats")
    )
    assert not {"transaction_hash", "block_hash", "txid"} & set(field_names)


def test_strict_v2_s_offline_preview_reads_nothing_and_writes_nothing(
    tmp_path: Path,
) -> None:
    outcome = preview_strict_v2_s_materialization_checkpoint(dataset=DATASET)

    assert outcome.status == "dry_run"
    assert outcome.population_contract_status == "not_checked"
    assert outcome.receipt_reads == 0
    assert outcome.network_requests == 0
    assert outcome.provider_requests == 0
    assert outcome.provider_points == 0
    assert outcome.written_paths == ()
    assert outcome.candidate_materialization_allowed is False
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    ("query_sha256", "schema_sha256", "reason"),
    [
        (
            "ff" * 32,
            STRICT_V2_S_CANDIDATE_SCHEMA_SHA256,
            "strict_v2_s_materialization_query_hash_mismatch",
        ),
        (
            PINNED_STRICT_V2_S_QUERY_SHA256,
            "ee" * 32,
            "strict_v2_s_materialization_result_schema_hash_mismatch",
        ),
    ],
)
def test_strict_v2_s_checksum_drift_blocks_before_receipts_or_network(
    tmp_path: Path,
    query_sha256: str,
    schema_sha256: str,
    reason: str,
) -> None:
    backend = FakeStrictV2SBackend()

    outcome = _run_probe(
        _probe(backend=backend, receipt_root=tmp_path / "missing"),
        query_sha256=query_sha256,
        schema_sha256=schema_sha256,
    )

    assert outcome.status == "blocked"
    assert outcome.receipt_reads == 0
    assert outcome.network_requests == 0
    assert outcome.blocking_reasons == (reason,)
    assert backend.calls == []


def test_strict_v2_s_internal_pin_drift_blocks_before_receipts_or_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = FakeStrictV2SBackend()
    monkeypatch.setattr(
        "crypto_address_identity.universe.candidate_materialization_v2_s."
        "PINNED_STRICT_V2_S_QUERY_SHA256",
        "aa" * 32,
    )

    outcome = _run_probe(
        _probe(backend=backend, receipt_root=tmp_path / "missing")
    )

    assert outcome.status == "blocked"
    assert outcome.blocking_reasons == ("strict_v2_s_pinned_query_drift",)
    assert outcome.receipt_reads == 0
    assert outcome.network_requests == 0
    assert backend.calls == []


def test_strict_v2_s_requires_accepted_dual_receipts_before_network(
    tmp_path: Path,
) -> None:
    backend = FakeStrictV2SBackend()

    outcome = _run_probe(
        _probe(backend=backend, receipt_root=tmp_path / "missing")
    )

    assert outcome.status == "blocked"
    assert outcome.population_contract_status == "blocked"
    assert outcome.network_requests == 0
    assert outcome.receipt_reads == 0
    assert "strict_v2_s_population_contract_not_accepted" in (
        outcome.blocking_reasons
    )
    assert backend.calls == []


def test_strict_v2_s_schema_drift_blocks_before_usage_and_dry_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt_root = _write_exact_receipts(tmp_path, monkeypatch)
    metadata = PinnedV2TableMetadata()
    metadata.schema_sha256 = "aa" * 32
    backend = FakeStrictV2SBackend(metadata=metadata)

    outcome = _run_probe(_probe(backend=backend, receipt_root=receipt_root))

    assert outcome.status == "blocked"
    assert outcome.network_requests == 1
    assert "strict_v2_s_source_schema_hash_mismatch" in (
        outcome.blocking_reasons
    )
    assert backend.calls == ["table_metadata"]


def test_strict_v2_s_live_checkpoint_is_cost_only_and_budgeted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt_root = _write_exact_receipts(tmp_path, monkeypatch)
    backend = FakeStrictV2SBackend()

    outcome = _run_probe(_probe(backend=backend, receipt_root=receipt_root))

    assert outcome.status == "checkpoint_passed"
    assert outcome.query_kind == "btc_strict_v2_s_candidate_materialization"
    assert outcome.variant == "V2-S"
    assert outcome.population_contract_status == "accepted"
    assert outcome.receipt_reads == 2
    assert outcome.network_requests == 3
    assert outcome.dry_run_bytes == 637_999_682_243
    assert outcome.projected_month_to_date_bytes == 1_476_768_207_555
    assert outcome.projected_reserve_bytes == 523_231_792_445
    assert outcome.within_budget is True
    assert outcome.expected_coarse_candidate_count == 1_090_411
    assert outcome.expected_p0_count == 21_736
    assert outcome.expected_p1_count == 2_143
    assert outcome.expected_edge_count == 133_730
    assert outcome.expected_coarse_other_count == 932_802
    assert outcome.candidate_rows_returned == 0
    assert outcome.candidate_materialization_allowed is False
    assert outcome.provider_requests == 0
    assert outcome.provider_points == 0
    assert outcome.written_paths == ()
    assert backend.calls == [
        "table_metadata",
        "monthly_successful_query_usage",
        "dry_run",
    ]
    assert not hasattr(backend, "execute_query")


@pytest.mark.parametrize(
    ("dry_run_bytes", "budget_bytes", "reserve_bytes", "reason"),
    [
        (
            650_000_000_001,
            2_000_000_000_000,
            250_000_000_000,
            "strict_v2_s_materialization_dry_run_limit_exceeded",
        ),
        (
            637_999_682_243,
            1_600_000_000_000,
            250_000_000_000,
            "strict_v2_s_materialization_monthly_reserve_insufficient",
        ),
    ],
)
def test_strict_v2_s_cost_or_reserve_failure_is_machine_readable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dry_run_bytes: int,
    budget_bytes: int,
    reserve_bytes: int,
    reason: str,
) -> None:
    receipt_root = _write_exact_receipts(tmp_path, monkeypatch)
    backend = FakeStrictV2SBackend(dry_run_bytes=dry_run_bytes)

    outcome = _run_probe(
        _probe(backend=backend, receipt_root=receipt_root),
        budget_bytes=budget_bytes,
        reserve_bytes=reserve_bytes,
    )

    assert outcome.status == "blocked"
    assert reason in outcome.blocking_reasons
    assert outcome.candidate_materialization_allowed is False
    assert outcome.network_requests == 3
