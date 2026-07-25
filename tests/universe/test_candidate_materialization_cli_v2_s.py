from __future__ import annotations

import hashlib
import json
from pathlib import Path

from crypto_address_identity.cli import main
from crypto_address_identity.universe.candidate_materialization_execution_v2_s import (
    STRICT_V2_S_AUTHORIZATION_ID,
    STRICT_V2_S_DESTINATION_TABLE_ID,
    STRICT_V2_S_EXPECTED_DRY_RUN_BYTES,
)
from crypto_address_identity.universe.candidate_materialization_v2_s import (
    PINNED_STRICT_V2_S_QUERY_SHA256,
    STRICT_V2_S_CANDIDATE_SCHEMA_SHA256,
)
from crypto_address_identity.universe.candidate_publication_v2_s import (
    CandidateArtifactExpectedCounts,
    StrictV2SCandidateArtifactPublisher,
)
from tests.universe.test_candidate_materialization_execution_v2_s import (
    SOURCE_SCHEMA_SHA256,
    FakeMaterializationBackend,
    _write_exact_population_receipts,
)
from tests.universe.test_candidate_publication_v2_s import (
    FakeCandidateTableBackend,
    _rows,
)


def _configure(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CAI_DATABASE_PATH", str(tmp_path / "identity.sqlite3"))
    monkeypatch.setenv("CAI_RAW_PAYLOAD_ROOT", str(tmp_path / "raw"))
    monkeypatch.setenv("CAI_EXPORT_ROOT", str(tmp_path / "exports"))
    monkeypatch.setenv("CAI_UNIVERSE_ROOT", str(tmp_path / "universe"))
    monkeypatch.setenv(
        "CAI_UNIVERSE_DUCKDB_PATH",
        str(tmp_path / "universe.duckdb"),
    )
    monkeypatch.setenv("CAI_BIGQUERY_BILLING_PROJECT", "fixture-project")
    monkeypatch.delenv("CAI_0XROUTER_TOKEN", raising=False)


def _output(capsys) -> dict[str, object]:
    return json.loads(capsys.readouterr().out)


def _execution_arguments(mode: str) -> list[str]:
    return [
        "universe",
        "execute",
        "bigquery-strict-v2-s-materialization",
        mode,
        "--authorization-id",
        STRICT_V2_S_AUTHORIZATION_ID,
        "--acknowledge-billed-execution",
        "--destination-table-id",
        STRICT_V2_S_DESTINATION_TABLE_ID,
        "--expected-query-sha256",
        PINNED_STRICT_V2_S_QUERY_SHA256,
        "--expected-result-schema-sha256",
        STRICT_V2_S_CANDIDATE_SCHEMA_SHA256,
        "--expected-source-schema-sha256",
        SOURCE_SCHEMA_SHA256,
        "--expected-dry-run-bytes",
        str(STRICT_V2_S_EXPECTED_DRY_RUN_BYTES),
        "--expected-successful-query-jobs",
        "6",
        "--expected-month-to-date-billed-bytes",
        "1476768301056",
        "--maximum-bytes-billed",
        "650000000000",
        "--monthly-processing-budget-bytes",
        "2400000000000",
        "--reserve-bytes",
        "250000000000",
        "--expected-candidate-rows",
        "1090411",
        "--destination-expiration-hours",
        "168",
    ]


def _execution_receipt(path: Path) -> str:
    payload = {
        "schema_version": (
            "btc_strict_v2_s_materialization_execution_receipt_v1"
        ),
        "status": "completed",
        "job_id": "cai_btc_v2s_fixture",
        "destination_table_id": STRICT_V2_S_DESTINATION_TABLE_ID,
        "query_sha256": PINNED_STRICT_V2_S_QUERY_SHA256,
        "result_schema_sha256": STRICT_V2_S_CANDIDATE_SCHEMA_SHA256,
        "candidate_rows": 4,
        "candidate_materialized": True,
    }
    encoded = (
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("ascii")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(encoded)
    path.chmod(0o600)
    return hashlib.sha256(encoded).hexdigest()


def _publication_arguments(
    mode: str,
    *,
    receipt_path: Path,
    receipt_sha256: str,
) -> list[str]:
    return [
        "universe",
        "publish",
        "bigquery-strict-v2-s-candidates",
        mode,
        "--campaign-id",
        "btc-v2s-bootstrap-959187-cli-test",
        "--destination-table-id",
        STRICT_V2_S_DESTINATION_TABLE_ID,
        "--source-execution-receipt",
        str(receipt_path),
        "--expected-execution-receipt-sha256",
        receipt_sha256,
        "--expected-result-schema-sha256",
        STRICT_V2_S_CANDIDATE_SCHEMA_SHA256,
        "--page-size",
        "2",
    ]


def test_execution_cli_preview_is_offline(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_strict_v2_s_backend",
        lambda settings: (_ for _ in ()).throw(
            AssertionError("network boundary")
        ),
    )

    exit_code = main(_execution_arguments("--dry-run"))
    output = _output(capsys)

    assert exit_code == 0
    assert output["status"] == "dry_run"
    assert output["execution_calls"] == 0
    assert output["candidate_materialized"] is False
    assert output["written_paths"] == []
    assert not (tmp_path / "universe").exists()


def test_execution_cli_uses_one_shot_backend(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    _write_exact_population_receipts(
        tmp_path / "universe",
        monkeypatch,
    )
    backend = FakeMaterializationBackend()
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_strict_v2_s_backend",
        lambda settings: backend,
    )

    exit_code = main(_execution_arguments("--execute-once"))
    output = _output(capsys)

    assert exit_code == 0
    assert output["status"] == "completed", output
    assert output["candidate_rows"] == 1_090_411
    assert output["execution_calls"] == 1
    assert output["automatic_retries"] == 0
    assert backend.execution_calls == 1


def test_publication_cli_preview_is_offline(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    receipt_path = tmp_path / "execution.json"
    receipt_sha256 = _execution_receipt(receipt_path)
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_strict_v2_s_backend",
        lambda settings: (_ for _ in ()).throw(
            AssertionError("network boundary")
        ),
    )

    exit_code = main(
        _publication_arguments(
            "--dry-run",
            receipt_path=receipt_path,
            receipt_sha256=receipt_sha256,
        )
    )
    output = _output(capsys)

    assert exit_code == 0
    assert output["status"] == "dry_run"
    assert output["network_requests"] == 0
    assert output["written_paths"] == []
    assert not (tmp_path / "universe").exists()


def test_publication_cli_writes_address_artifact(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure(monkeypatch, tmp_path)
    receipt_path = tmp_path / "execution.json"
    receipt_sha256 = _execution_receipt(receipt_path)
    backend = FakeCandidateTableBackend(_rows())
    monkeypatch.setattr(
        "crypto_address_identity.cli._make_strict_v2_s_backend",
        lambda settings: backend,
    )
    monkeypatch.setattr(
        "crypto_address_identity.cli.StrictV2SCandidateArtifactPublisher",
        lambda *, backend: StrictV2SCandidateArtifactPublisher(
            backend=backend,
            expected_counts=CandidateArtifactExpectedCounts(
                total=4,
                p0=1,
                p1=1,
                edge=1,
                coarse_other=1,
            ),
        ),
    )

    exit_code = main(
        _publication_arguments(
            "--publish-once",
            receipt_path=receipt_path,
            receipt_sha256=receipt_sha256,
        )
    )
    output = _output(capsys)

    assert exit_code == 0
    assert output["status"] == "published"
    assert output["candidate_rows"] == 4
    assert (
        tmp_path
        / "universe"
        / "campaigns"
        / "btc-v2s-bootstrap-959187-cli-test"
        / "manifest.json"
    ).is_file()
