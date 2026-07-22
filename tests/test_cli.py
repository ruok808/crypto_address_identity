from __future__ import annotations

import json
from pathlib import Path

from crypto_address_identity.cli import main


BTC_ADDRESS = "1BoatSLRHtKNngkdXEeobR76b53LETtpyT"


def _configure(monkeypatch, env_mapping: dict[str, str]) -> None:
    for key, value in env_mapping.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("CAI_0XROUTER_TOKEN", raising=False)


def _candidate_file(path: Path) -> Path:
    file_path = path / "candidates.ndjson"
    file_path.write_text(
        json.dumps(
            {
                "chain_key": "bitcoin",
                "address": BTC_ADDRESS,
                "reason": "manual_review",
                "priority": 50,
                "source_reference": "fixture",
                "requested_at": "2026-07-22T00:00:00Z",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return file_path


def test_init_db_and_dry_run_candidate_import_return_structured_json(
    runtime_root, env_mapping, monkeypatch, capsys
) -> None:
    _configure(monkeypatch, env_mapping)

    assert main(["init-db"]) == 0
    init_output = json.loads(capsys.readouterr().out)
    assert init_output["status"] == "ok"

    assert main(["candidates", "import", "--file", str(_candidate_file(runtime_root)), "--dry-run"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output == {"records": 1, "status": "dry_run"}


def test_fetch_dry_run_does_not_require_token_or_call_network(runtime_root, env_mapping, monkeypatch, capsys) -> None:
    _configure(monkeypatch, env_mapping)
    candidate_file = _candidate_file(runtime_root)
    assert main(["init-db"]) == 0
    capsys.readouterr()
    assert main(["candidates", "import", "--file", str(candidate_file)]) == 0
    capsys.readouterr()

    assert main(["fetch", "run", "--dry-run", "--limit", "1"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "dry_run"
    assert output["request_count"] == 0
    assert output["written_paths"] == []


def test_execute_fetch_without_token_returns_safe_nonzero_json(runtime_root, env_mapping, monkeypatch, capsys) -> None:
    _configure(monkeypatch, env_mapping)
    candidate_file = _candidate_file(runtime_root)
    main(["init-db"])
    capsys.readouterr()
    main(["candidates", "import", "--file", str(candidate_file)])
    capsys.readouterr()

    assert main(["fetch", "run", "--limit", "1"]) == 2
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "error"
    assert output["error_code"] == "provider_token_missing"
    assert "token" not in output.get("detail", "").lower()
