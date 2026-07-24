from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

from crypto_address_identity.cli import main
from crypto_address_identity.universe.bigquery import (
    AggregateQueryExecution,
    MonthlyQueryUsage,
    QueryEstimate,
)
from crypto_address_identity.universe.candidate_execution import (
    CANDIDATE_STATISTICS_MAXIMUM_BYTES_BILLED,
    PINNED_CANDIDATE_QUERY_SHA256,
    PINNED_CANDIDATE_SCHEMA_SHA256,
    PINNED_CUTOFF_HEIGHT,
    PINNED_SOURCE_STANDARD_ADDRESS_COUNT,
)
from crypto_address_identity.universe.candidate_execution_v2 import (
    CANDIDATE_STATISTICS_V2_AUTHORIZATION_ID,
    CANDIDATE_STATISTICS_V2_MAXIMUM_BYTES_BILLED,
    CANDIDATE_STATISTICS_V2_RECOVERY_AUTHORIZATION_ID,
    PINNED_V2_CANDIDATE_QUERY_SHA256,
    PINNED_V2_CANDIDATE_SCHEMA_SHA256,
    PINNED_V2_DRY_RUN_BYTES,
    PINNED_V2_FAILED_JOB_ERROR_REASON,
    PINNED_V2_FAILED_JOB_ID,
    PINNED_V2_FAILED_RECEIPT_SHA256,
    PINNED_V2_MONTHLY_PROCESSING_BUDGET_BYTES,
    PINNED_V2_MONTHLY_RESERVE_BYTES,
)
from crypto_address_identity.universe.candidate_statistics_v2 import (
    PINNED_V2_CUTOFF_HEIGHT,
    PINNED_V2_SOURCE_INPUT_ONLY_ADDRESS_COUNT,
    PINNED_V2_SOURCE_STANDARD_ADDRESS_COUNT,
)
from crypto_address_identity.universe.features import FeatureMaterializationResult
from crypto_address_identity.universe.models import SourceManifest, SourceProbeResult
from crypto_address_identity.universe.query_plan import BigQueryQueryPlan
from crypto_address_identity.universe.storage import UniverseStore
from tests.universe.conftest import (
    BTC_ADDRESSES,
    make_accounting,
    make_feature,
    make_script,
)
from tests.universe.test_bigquery_probe import table_metadata
from tests.universe.test_candidate_execution import (
    PinnedTableMetadata,
    execution_result_row,
)
from tests.universe.test_candidate_execution_v2 import (
    EXPECTED_MONTH_TO_DATE_BILLED_BYTES,
    EXPECTED_SUCCESSFUL_QUERY_JOBS,
    PinnedV2TableMetadata,
    execution_result_row as execution_result_row_v2,
    write_started_recovery_receipt,
)


class FakeBigQueryBackend:
    def __init__(self) -> None:
        self.tables = table_metadata()
        self.calls: list[str] = []
        self.dry_run_caps: list[int] = []
        self.query_caps: list[int] = []
        self.last_query_total_bytes_processed = 900

    def table_metadata(self, table_id: str):
        self.calls.append("table_metadata")
        return self.tables[table_id.rsplit(".", 1)[-1]]

    def dry_run(
        self,
        sql: str,
        parameters: dict[str, object],
        maximum_bytes_billed: int,
    ) -> QueryEstimate:
        self.calls.append("dry_run")
        self.dry_run_caps.append(maximum_bytes_billed)
        return QueryEstimate(total_bytes_processed=900, cache_hit=False)

    def query_one(
        self,
        sql: str,
        parameters: dict[str, object],
        *,
        maximum_bytes_billed: int,
    ) -> dict[str, object]:
        self.calls.append("query_one")
        self.query_caps.append(maximum_bytes_billed)
        return {
            "latest_height": 900_010,
            "latest_hash": "10" * 32,
            "latest_time": datetime(2026, 7, 23, 23, tzinfo=UTC),
            "finalized_height": 900_004,
            "finalized_hash": "11" * 32,
            "taproot_address_count": 2,
        }

    def monthly_successful_query_usage(
        self,
        *,
        month_start: datetime,
        month_end: datetime,
    ) -> MonthlyQueryUsage:
        self.calls.append("monthly_successful_query_usage")
        return MonthlyQueryUsage(
            successful_query_jobs=2,
            total_bytes_billed=100,
        )


class FakeBitcoinCoreProbe:
    def run(self) -> SourceProbeResult:
        return SourceProbeResult(
            source_kind="bitcoin_core",
            status="accepted",
            schema_sha256="12" * 32,
            latest_height=900_010,
            latest_hash="10" * 32,
            latest_time=datetime(2026, 7, 23, 23, tzinfo=UTC),
            finalized_height=900_004,
            finalized_hash="11" * 32,
            script_completeness=True,
            capabilities=("historical_block_scan", "utxo_probe"),
        )


class FakeCandidateExecutionBackend(FakeBigQueryBackend):
    def table_metadata(self, table_id: str) -> PinnedTableMetadata:
        self.calls.append("table_metadata")
        assert table_id.endswith(".transactions")
        return PinnedTableMetadata()

    def execute_aggregate_at_most_two_no_retry(
        self,
        sql: str,
        parameters: dict[str, object],
        *,
        maximum_bytes_billed: int,
        job_id: str,
    ) -> AggregateQueryExecution:
        self.calls.append("execute_aggregate_at_most_two_no_retry")
        assert maximum_bytes_billed == CANDIDATE_STATISTICS_MAXIMUM_BYTES_BILLED
        assert job_id.startswith("cai_btc_candidate_statistics_")
        return AggregateQueryExecution(
            rows=(execution_result_row(),),
            total_bytes_processed=900,
            total_bytes_billed=900,
        )


class FakeCandidateExecutionV2Backend(FakeBigQueryBackend):
    def table_metadata(self, table_id: str) -> PinnedV2TableMetadata:
        self.calls.append("table_metadata")
        assert table_id.endswith(".transactions")
        return PinnedV2TableMetadata()

    def monthly_successful_query_usage(
        self,
        *,
        month_start: datetime,
        month_end: datetime,
    ) -> MonthlyQueryUsage:
        self.calls.append("monthly_successful_query_usage")
        return MonthlyQueryUsage(
            successful_query_jobs=EXPECTED_SUCCESSFUL_QUERY_JOBS,
            total_bytes_billed=EXPECTED_MONTH_TO_DATE_BILLED_BYTES,
        )

    def dry_run(
        self,
        sql: str,
        parameters: dict[str, object],
        maximum_bytes_billed: int,
    ) -> QueryEstimate:
        self.calls.append("dry_run")
        return QueryEstimate(
            total_bytes_processed=PINNED_V2_DRY_RUN_BYTES,
            cache_hit=False,
        )

    def execute_aggregate_at_most_two_no_retry(
        self,
        sql: str,
        parameters: dict[str, object],
        *,
        maximum_bytes_billed: int,
        job_id: str,
    ) -> AggregateQueryExecution:
        self.calls.append("execute_aggregate_at_most_two_no_retry")
        assert maximum_bytes_billed == (
            CANDIDATE_STATISTICS_V2_MAXIMUM_BYTES_BILLED
        )
        assert job_id.startswith("cai_btc_importance_v2_")
        return AggregateQueryExecution(
            rows=(execution_result_row_v2(),),
            total_bytes_processed=PINNED_V2_DRY_RUN_BYTES,
            total_bytes_billed=PINNED_V2_DRY_RUN_BYTES,
        )

    def fetch_existing_aggregate_at_most_two_no_retry(
        self,
        *,
        job_id: str,
        timeout_seconds: float,
    ) -> AggregateQueryExecution:
        self.calls.append("fetch_existing_aggregate_at_most_two_no_retry")
        assert job_id.startswith("cai_btc_importance_v2_")
        assert timeout_seconds == 120.0
        return AggregateQueryExecution(
            rows=(execution_result_row_v2(),),
            total_bytes_processed=PINNED_V2_DRY_RUN_BYTES,
            total_bytes_billed=PINNED_V2_DRY_RUN_BYTES,
        )


class FakeBigQueryMaterializer:
    def __init__(self) -> None:
        self.requests = []

    def run(self, *, request, calibration_snapshot=None):
        self.requests.append(request)
        assert calibration_snapshot is None
        return FeatureMaterializationResult(
            status="published",
            campaign_id=request.source_manifest.campaign_id,
            address_feature_rows=12,
            script_subject_rows=15,
            source_accounting_rows=1,
            dry_run_bytes=900,
            total_bytes_processed=900,
            written_paths=("data/universe/campaigns/btc-cli-build",),
        )


def _configure(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CAI_DATABASE_PATH", str(tmp_path / "identity.sqlite3"))
    monkeypatch.setenv("CAI_RAW_PAYLOAD_ROOT", str(tmp_path / "raw"))
    monkeypatch.setenv("CAI_EXPORT_ROOT", str(tmp_path / "exports"))
    monkeypatch.setenv("CAI_UNIVERSE_ROOT", str(tmp_path / "universe"))
    monkeypatch.setenv(
        "CAI_UNIVERSE_DUCKDB_PATH", str(tmp_path / "universe.duckdb")
    )
    monkeypatch.setenv("CAI_BIGQUERY_BILLING_PROJECT", "fixture-project")
    monkeypatch.delenv("CAI_0XROUTER_TOKEN", raising=False)


def _publish_campaign(tmp_path: Path) -> None:
    manifest = SourceManifest(
        campaign_id="btc-cli-fixture",
        source_kind="fixture",
        source_revision="cli-fixture-v1",
        cutoff_height=900_000,
        cutoff_hash="01" * 32,
        cutoff_time=datetime(2026, 7, 24, tzinfo=UTC),
        schema_sha256="02" * 32,
        query_sha256="03" * 32,
        source_capabilities=("address_rows", "script_hex", "source_accounting"),
        script_completeness=True,
    )
    writer = UniverseStore(tmp_path / "universe").begin_campaign(manifest)
    writer.write_address_features(
        [
            make_feature(
                BTC_ADDRESSES[0],
                current_utxo_sats=100 * 100_000_000,
                lifetime_received_sats=100 * 100_000_000,
                lifetime_spent_sats=0,
            )
        ]
    )
    writer.write_script_subjects([make_script(BTC_ADDRESSES[0])])
    writer.write_source_accounting(make_accounting())
    writer.publish()


def _output(capsys) -> dict[str, object]:
    return json.loads(capsys.readouterr().out)


def _candidate_execution_arguments(mode: str) -> list[str]:
    return [
        "universe",
        "execute",
        "bigquery-candidate-statistics",
        mode,
        "--authorization-id",
        "btc-candidate-statistics-20260724-v1",
        "--as-of-date",
        "2026-07-24",
        "--cutoff-height",
        str(PINNED_CUTOFF_HEIGHT),
        "--expected-query-sha256",
        PINNED_CANDIDATE_QUERY_SHA256,
        "--expected-schema-sha256",
        PINNED_CANDIDATE_SCHEMA_SHA256,
        "--expected-source-address-count",
        str(PINNED_SOURCE_STANDARD_ADDRESS_COUNT),
        "--maximum-bytes-billed",
        str(CANDIDATE_STATISTICS_MAXIMUM_BYTES_BILLED),
        "--sandbox-budget-bytes",
        "1099511627776",
        "--reserve-bytes",
        "250000000000",
    ]


def _candidate_execution_v2_arguments(mode: str) -> list[str]:
    return [
        "universe",
        "execute",
        "bigquery-candidate-statistics-v2",
        mode,
        "--authorization-id",
        CANDIDATE_STATISTICS_V2_AUTHORIZATION_ID,
        "--acknowledge-billed-execution",
        "--as-of-date",
        "2026-07-24",
        "--cutoff-height",
        str(PINNED_V2_CUTOFF_HEIGHT),
        "--expected-query-sha256",
        PINNED_V2_CANDIDATE_QUERY_SHA256,
        "--expected-schema-sha256",
        PINNED_V2_CANDIDATE_SCHEMA_SHA256,
        "--expected-source-address-count",
        str(PINNED_V2_SOURCE_STANDARD_ADDRESS_COUNT),
        "--expected-input-only-address-count",
        str(PINNED_V2_SOURCE_INPUT_ONLY_ADDRESS_COUNT),
        "--expected-dry-run-bytes",
        str(PINNED_V2_DRY_RUN_BYTES),
        "--expected-successful-query-jobs",
        str(EXPECTED_SUCCESSFUL_QUERY_JOBS),
        "--expected-month-to-date-billed-bytes",
        str(EXPECTED_MONTH_TO_DATE_BILLED_BYTES),
        "--maximum-bytes-billed",
        str(CANDIDATE_STATISTICS_V2_MAXIMUM_BYTES_BILLED),
        "--monthly-processing-budget-bytes",
        str(PINNED_V2_MONTHLY_PROCESSING_BUDGET_BYTES),
        "--reserve-bytes",
        str(PINNED_V2_MONTHLY_RESERVE_BYTES),
    ]


def _candidate_execution_v2_recovery_arguments(mode: str) -> list[str]:
    arguments = _candidate_execution_v2_arguments(mode)
    authorization_index = arguments.index(
        CANDIDATE_STATISTICS_V2_AUTHORIZATION_ID
    )
    arguments[authorization_index] = (
        CANDIDATE_STATISTICS_V2_RECOVERY_AUTHORIZATION_ID
    )
    arguments.extend(
        [
            "--recovery-from-authorization-id",
            CANDIDATE_STATISTICS_V2_AUTHORIZATION_ID,
            "--expected-previous-receipt-sha256",
            PINNED_V2_FAILED_RECEIPT_SHA256,
            "--expected-previous-job-id",
            PINNED_V2_FAILED_JOB_ID,
            "--expected-previous-job-error-reason",
            PINNED_V2_FAILED_JOB_ERROR_REASON,
            "--expected-previous-job-total-bytes-processed",
            "0",
            "--expected-previous-job-total-bytes-billed",
            "0",
        ]
    )
    return arguments


def test_bigquery_offline_probe_does_not_construct_backend_or_write(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_bigquery_backend",
        lambda settings: (_ for _ in ()).throw(AssertionError("network boundary")),
    )

    exit_code = main(
        [
            "universe",
            "probe",
            "bigquery",
            "--dry-run",
            "--as-of-date",
            "2026-07-24",
        ]
    )
    output = _output(capsys)

    assert exit_code == 0
    assert output["status"] == "dry_run"
    assert output["network_requests"] == 0
    assert output["provider_requests"] == 0
    assert output["provider_points"] == 0
    assert output["written_paths"] == []
    assert output["query_sha256"] == BigQueryQueryPlan.load(
        "bigquery-public-data.crypto_bitcoin"
    ).address_features_sha256
    assert not (tmp_path / "universe").exists()


def test_bigquery_execute_readonly_uses_injected_backend_and_safe_json(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    backend = FakeBigQueryBackend()
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_bigquery_backend",
        lambda settings: backend,
    )

    exit_code = main(
        [
            "universe",
            "probe",
            "bigquery",
            "--execute-readonly",
            "--as-of-date",
            "2026-07-24",
            "--maximum-bytes-billed",
            "1000",
        ]
    )
    output = _output(capsys)

    assert exit_code == 0
    assert output["status"] == "accepted"
    assert output["finalized_height"] == 900_004
    assert output["dry_run_bytes"] == 900
    assert output["provider_requests"] == 0
    assert backend.calls == [
        "table_metadata",
        "table_metadata",
        "dry_run",
        "query_one",
    ]
    assert backend.dry_run_caps == [0]
    assert backend.query_caps == [1_000]
    assert "credential" not in json.dumps(output).lower()


def test_bigquery_execute_readonly_requires_positive_byte_cap(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)

    exit_code = main(
        [
            "universe",
            "probe",
            "bigquery",
            "--execute-readonly",
            "--as-of-date",
            "2026-07-24",
            "--maximum-bytes-billed",
            "0",
        ]
    )

    assert exit_code == 2
    assert _output(capsys) == {
        "error_code": "invalid_input",
        "status": "error",
    }


def test_bigquery_address_scale_offline_probe_is_network_free(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_bigquery_backend",
        lambda settings: (_ for _ in ()).throw(AssertionError("network boundary")),
    )

    exit_code = main(
        [
            "universe",
            "probe",
            "bigquery-address-scale",
            "--dry-run",
            "--as-of-date",
            "2026-07-24",
        ]
    )
    output = _output(capsys)

    assert exit_code == 0
    assert output["status"] == "dry_run"
    assert output["query_kind"] == "btc_address_scale"
    assert output["network_requests"] == 0
    assert output["provider_requests"] == 0
    assert output["written_paths"] == []
    assert not (tmp_path / "universe").exists()


def test_bigquery_address_scale_live_probe_only_estimates(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    backend = FakeBigQueryBackend()
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_bigquery_backend",
        lambda settings: backend,
    )

    exit_code = main(
        [
            "universe",
            "probe",
            "bigquery-address-scale",
            "--execute-readonly",
            "--as-of-date",
            "2026-07-24",
            "--sandbox-budget-bytes",
            "1000",
        ]
    )
    output = _output(capsys)

    assert exit_code == 0
    assert output["status"] == "within_budget"
    assert output["query_kind"] == "btc_address_scale"
    assert output["dry_run_bytes"] == 900
    assert output["sandbox_budget_bytes"] == 1_000
    assert output["within_budget"] is True
    assert output["exact_distinct"] is True
    assert output["provider_requests"] == 0
    assert output["written_paths"] == []
    assert backend.calls == ["table_metadata", "dry_run"]
    assert backend.query_caps == []
    assert not (tmp_path / "universe").exists()


def test_bigquery_address_scale_live_probe_requires_positive_budget(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)

    exit_code = main(
        [
            "universe",
            "probe",
            "bigquery-address-scale",
            "--execute-readonly",
            "--as-of-date",
            "2026-07-24",
            "--sandbox-budget-bytes",
            "0",
        ]
    )

    assert exit_code == 2
    assert _output(capsys) == {
        "error_code": "invalid_input",
        "status": "error",
    }


def test_bigquery_candidate_statistics_offline_preview_is_network_free(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_bigquery_backend",
        lambda settings: (_ for _ in ()).throw(AssertionError("network boundary")),
    )

    exit_code = main(
        [
            "universe",
            "probe",
            "bigquery-candidate-statistics",
            "--dry-run",
            "--as-of-date",
            "2026-07-24",
            "--cutoff-height",
            "959187",
        ]
    )
    output = _output(capsys)

    assert exit_code == 0
    assert output["status"] == "dry_run"
    assert output["query_kind"] == "btc_candidate_statistics"
    assert output["query_sha256"] == BigQueryQueryPlan.load(
        "bigquery-public-data.crypto_bitcoin"
    ).candidate_statistics_sha256
    assert output["network_requests"] == 0
    assert output["provider_requests"] == 0
    assert output["provider_points"] == 0
    assert output["written_paths"] == []
    assert not (tmp_path / "universe").exists()


def test_bigquery_candidate_statistics_live_probe_never_executes_or_writes(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    backend = FakeBigQueryBackend()
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_bigquery_backend",
        lambda settings: backend,
    )
    query_sha256 = BigQueryQueryPlan.load(
        "bigquery-public-data.crypto_bitcoin"
    ).candidate_statistics_sha256

    exit_code = main(
        [
            "universe",
            "probe",
            "bigquery-candidate-statistics",
            "--execute-readonly",
            "--as-of-date",
            "2026-07-24",
            "--cutoff-height",
            "959187",
            "--expected-query-sha256",
            query_sha256,
            "--sandbox-budget-bytes",
            "2000",
            "--reserve-bytes",
            "500",
        ]
    )
    output = _output(capsys)

    assert exit_code == 0
    assert output["status"] == "within_budget"
    assert output["dry_run_bytes"] == 900
    assert output["month_to_date_billed_bytes"] == 100
    assert output["projected_month_to_date_bytes"] == 1000
    assert output["projected_reserve_bytes"] == 1000
    assert output["within_budget"] is True
    assert output["network_requests"] == 3
    assert output["provider_requests"] == 0
    assert output["provider_points"] == 0
    assert output["written_paths"] == []
    assert backend.calls == [
        "table_metadata",
        "monthly_successful_query_usage",
        "dry_run",
    ]
    assert backend.query_caps == []
    assert not (tmp_path / "universe").exists()


def test_bigquery_candidate_statistics_live_probe_requires_pinned_hash(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)

    exit_code = main(
        [
            "universe",
            "probe",
            "bigquery-candidate-statistics",
            "--execute-readonly",
            "--as-of-date",
            "2026-07-24",
            "--cutoff-height",
            "959187",
            "--sandbox-budget-bytes",
            "2000",
            "--reserve-bytes",
            "500",
        ]
    )

    assert exit_code == 2
    assert _output(capsys) == {
        "error_code": "invalid_input",
        "status": "error",
    }


def test_bigquery_candidate_statistics_v2_offline_preview_is_network_free(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_bigquery_backend",
        lambda settings: (_ for _ in ()).throw(AssertionError("network boundary")),
    )

    exit_code = main(
        [
            "universe",
            "probe",
            "bigquery-candidate-statistics-v2",
            "--dry-run",
            "--as-of-date",
            "2026-07-24",
            "--cutoff-height",
            "959187",
        ]
    )
    output = _output(capsys)

    assert exit_code == 0
    assert output["status"] == "dry_run"
    assert output["query_kind"] == "btc_candidate_statistics_v2"
    assert output["policy_version"] == "btc_importance_v2"
    assert output["query_sha256"] == BigQueryQueryPlan.load(
        "bigquery-public-data.crypto_bitcoin"
    ).candidate_statistics_v2_sha256
    assert output["network_requests"] == 0
    assert output["provider_requests"] == 0
    assert output["provider_points"] == 0
    assert output["written_paths"] == []
    assert not (tmp_path / "universe").exists()


def test_bigquery_candidate_statistics_v2_live_dry_run_never_executes_or_writes(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    backend = FakeBigQueryBackend()
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_bigquery_backend",
        lambda settings: backend,
    )
    query_sha256 = BigQueryQueryPlan.load(
        "bigquery-public-data.crypto_bitcoin"
    ).candidate_statistics_v2_sha256

    exit_code = main(
        [
            "universe",
            "probe",
            "bigquery-candidate-statistics-v2",
            "--live-dry-run",
            "--as-of-date",
            "2026-07-24",
            "--cutoff-height",
            "959187",
            "--expected-query-sha256",
            query_sha256,
            "--sandbox-budget-bytes",
            "2000",
            "--reserve-bytes",
            "500",
        ]
    )
    output = _output(capsys)

    assert exit_code == 0
    assert output["status"] == "within_budget"
    assert output["query_kind"] == "btc_candidate_statistics_v2"
    assert output["policy_version"] == "btc_importance_v2"
    assert output["dry_run_bytes"] == 900
    assert output["within_budget"] is True
    assert output["network_requests"] == 3
    assert output["provider_requests"] == 0
    assert output["provider_points"] == 0
    assert output["written_paths"] == []
    assert backend.calls == [
        "table_metadata",
        "monthly_successful_query_usage",
        "dry_run",
    ]
    assert backend.query_caps == []
    assert not (tmp_path / "universe").exists()


def test_bigquery_candidate_statistics_execution_preview_is_offline(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_bigquery_backend",
        lambda settings: (_ for _ in ()).throw(AssertionError("network boundary")),
    )

    exit_code = main(_candidate_execution_arguments("--dry-run"))
    output = _output(capsys)

    assert exit_code == 0
    assert output["status"] == "dry_run"
    assert output["query_sha256"] == PINNED_CANDIDATE_QUERY_SHA256
    assert output["expected_schema_sha256"] == PINNED_CANDIDATE_SCHEMA_SHA256
    assert output["execution_calls"] == 0
    assert output["automatic_retries"] == 0
    assert output["candidate_materialized"] is False
    assert output["written_paths"] == []
    assert not (tmp_path / "universe").exists()


def test_bigquery_candidate_statistics_executes_once_via_explicit_cli(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    backend = FakeCandidateExecutionBackend()
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_bigquery_backend",
        lambda settings: backend,
    )

    exit_code = main(_candidate_execution_arguments("--execute-once"))
    output = _output(capsys)

    assert exit_code == 0
    assert output["status"] == "completed"
    assert output["row_count"] == 1
    assert output["execution_calls"] == 1
    assert output["automatic_retries"] == 0
    assert output["candidate_materialized"] is False
    assert output["provider_requests"] == 0
    assert output["provider_points"] == 0
    assert len(output["written_paths"]) == 1
    assert backend.calls == [
        "table_metadata",
        "monthly_successful_query_usage",
        "dry_run",
        "execute_aggregate_at_most_two_no_retry",
    ]

    second_exit_code = main(_candidate_execution_arguments("--execute-once"))
    second_output = _output(capsys)
    assert second_exit_code == 2
    assert second_output == {
        "error_code": "candidate_execution_already_attempted",
        "status": "error",
    }
    assert backend.calls.count("execute_aggregate_at_most_two_no_retry") == 1


def test_bigquery_candidate_statistics_v2_execution_preview_is_offline(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_bigquery_backend",
        lambda settings: (_ for _ in ()).throw(
            AssertionError("network boundary")
        ),
    )

    exit_code = main(_candidate_execution_v2_arguments("--dry-run"))
    output = _output(capsys)

    assert exit_code == 0
    assert output["status"] == "dry_run"
    assert output["billing_acknowledged"] is True
    assert output["query_sha256"] == PINNED_V2_CANDIDATE_QUERY_SHA256
    assert output["execution_calls"] == 0
    assert output["automatic_retries"] == 0
    assert output["candidate_materialized"] is False
    assert output["written_paths"] == []
    assert not (tmp_path / "universe").exists()


def test_bigquery_candidate_statistics_v2_recovery_preview_is_offline(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_bigquery_backend",
        lambda settings: (_ for _ in ()).throw(
            AssertionError("network boundary")
        ),
    )

    exit_code = main(
        _candidate_execution_v2_recovery_arguments("--dry-run")
    )
    output = _output(capsys)

    assert exit_code == 0
    assert output["status"] == "dry_run"
    assert output["authorization_id"] == (
        CANDIDATE_STATISTICS_V2_RECOVERY_AUTHORIZATION_ID
    )
    assert output["recovery_from_authorization_id"] == (
        CANDIDATE_STATISTICS_V2_AUTHORIZATION_ID
    )
    assert output["expected_previous_receipt_sha256"] == (
        PINNED_V2_FAILED_RECEIPT_SHA256
    )
    assert output["recovery_evidence_validated"] is False
    assert output["execution_calls"] == 0
    assert output["written_paths"] == []
    assert not (tmp_path / "universe").exists()


def test_bigquery_candidate_statistics_v2_executes_once_via_cli(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    backend = FakeCandidateExecutionV2Backend()
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_bigquery_backend",
        lambda settings: backend,
    )

    exit_code = main(_candidate_execution_v2_arguments("--execute-once"))
    output = _output(capsys)

    assert exit_code == 0
    assert output["status"] == "completed"
    assert output["row_count"] == 1
    assert output["execution_calls"] == 1
    assert output["automatic_retries"] == 0
    assert output["candidate_materialized"] is False
    assert len(output["written_paths"]) == 1
    assert backend.calls == [
        "table_metadata",
        "monthly_successful_query_usage",
        "dry_run",
        "execute_aggregate_at_most_two_no_retry",
    ]

    second_exit_code = main(
        _candidate_execution_v2_arguments("--execute-once")
    )
    second_output = _output(capsys)
    assert second_exit_code == 2
    assert second_output == {
        "error_code": "candidate_v2_execution_already_attempted",
        "status": "error",
    }
    assert backend.calls.count(
        "execute_aggregate_at_most_two_no_retry"
    ) == 1


def test_bigquery_candidate_statistics_v2_reconciles_existing_job_via_cli(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    write_started_recovery_receipt(
        tmp_path / "universe",
        changes={
            "created_at": datetime.now(UTC).isoformat().replace(
                "+00:00",
                "Z",
            )
        },
    )
    backend = FakeCandidateExecutionV2Backend()
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_bigquery_backend",
        lambda settings: backend,
    )

    exit_code = main(
        _candidate_execution_v2_recovery_arguments(
            "--reconcile-existing-job"
        )
    )
    output = _output(capsys)

    assert exit_code == 0
    assert output["status"] == "completed"
    assert output["row_count"] == 1
    assert output["execution_calls"] == 1
    assert output["automatic_retries"] == 0
    assert output["candidate_materialized"] is False
    assert backend.calls == [
        "fetch_existing_aggregate_at_most_two_no_retry",
    ]


def test_bitcoin_core_execute_readonly_uses_injected_probe(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_bitcoin_core_probe",
        lambda settings: FakeBitcoinCoreProbe(),
    )

    assert main(
        ["universe", "probe", "bitcoin-core", "--execute-readonly"]
    ) == 0
    output = _output(capsys)

    assert output["status"] == "accepted"
    assert output["script_completeness"] is True
    assert output["provider_requests"] == 0
    assert "cookie" not in json.dumps(output).lower()


def test_build_offline_dry_run_reports_query_hash_without_writes(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_bigquery_backend",
        lambda settings: (_ for _ in ()).throw(AssertionError("network boundary")),
    )

    assert main(
        [
            "universe",
            "build",
            "bigquery",
            "--dry-run",
            "--campaign-id",
            "btc-cli-build",
            "--cutoff-height",
            "900004",
            "--cutoff-time",
            "2026-07-23T22:00:00Z",
        ]
    ) == 0
    output = _output(capsys)

    assert output["status"] == "dry_run"
    assert output["dry_run_bytes"] is None
    assert len(output["query_sha256"]) == 64
    assert output["network_requests"] == 0
    assert output["written_paths"] == []
    assert not (tmp_path / "universe").exists()


def test_execute_chain_read_requires_positive_byte_cap(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)

    exit_code = main(
        [
            "universe",
            "build",
            "bigquery",
            "--execute-chain-read",
            "--campaign-id",
            "btc-cli-build",
            "--cutoff-height",
            "900004",
            "--cutoff-time",
            "2026-07-23T22:00:00Z",
            "--maximum-bytes-billed",
            "0",
        ]
    )

    assert exit_code == 2
    assert _output(capsys)["error_code"] == "invalid_input"


def test_execute_chain_read_delegates_only_after_accepted_checkpoint(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    backend = FakeBigQueryBackend()
    materializer = FakeBigQueryMaterializer()
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_bigquery_backend",
        lambda settings: backend,
    )
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_bigquery_materializer",
        lambda settings, selected_backend: materializer,
    )

    exit_code = main(
        [
            "universe",
            "build",
            "bigquery",
            "--execute-chain-read",
            "--campaign-id",
            "btc-cli-build",
            "--cutoff-height",
            "900004",
            "--cutoff-time",
            "2026-07-23T22:00:00Z",
            "--maximum-bytes-billed",
            "1000",
        ]
    )
    output = _output(capsys)

    assert exit_code == 0
    assert output["status"] == "published"
    assert output["provider_requests"] == 0
    assert output["provider_points"] == 0
    assert len(materializer.requests) == 1
    request = materializer.requests[0]
    assert request.maximum_bytes_billed == 1000
    assert request.source_manifest.cutoff_hash == "11" * 32


def test_candidate_dry_run_never_opens_identity_or_provider(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    _publish_campaign(tmp_path)
    monkeypatch.setattr(
        "crypto_address_identity.cli.IdentityDatabase.__init__",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("identity database boundary")
        ),
    )
    monkeypatch.setattr(
        "crypto_address_identity.cli.ZeroXRouterClient.__init__",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("provider boundary")
        ),
    )

    assert main(
        [
            "universe",
            "candidates",
            "--campaign-id",
            "btc-cli-fixture",
            "--dry-run",
            "--runtime-minutes",
            "480",
            "--requests-per-minute",
            "25",
        ]
    ) == 0
    output = _output(capsys)

    assert output["status"] == "dry_run"
    assert output["provider_requests"] == 0
    assert output["provider_points"] == 0
    assert output["written_paths"] == []
    assert "1Boat" not in json.dumps(output)


def test_conflicting_modes_return_structured_invalid_input(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)

    exit_code = main(
        [
            "universe",
            "probe",
            "bigquery",
            "--dry-run",
            "--execute-readonly",
            "--as-of-date",
            "2026-07-24",
            "--maximum-bytes-billed",
            "1000",
        ]
    )

    assert exit_code == 2
    assert _output(capsys) == {
        "error_code": "invalid_input",
        "status": "error",
    }


def test_universe_commands_leave_coverage_launchagent_files_byte_identical(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    root = Path(__file__).parents[2]
    paths = sorted((root / "ops" / "launchd").glob("*"))
    before = {
        path: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in paths
        if path.is_file()
    }

    assert main(
        [
            "universe",
            "probe",
            "bigquery",
            "--dry-run",
            "--as-of-date",
            "2026-07-24",
        ]
    ) == 0
    _output(capsys)
    after = {
        path: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in paths
        if path.is_file()
    }

    assert before == after
