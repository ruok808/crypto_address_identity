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


def _evidence_file(path: Path) -> Path:
    file_path = path / "evidence.ndjson"
    file_path.write_text(
        json.dumps(
            {
                "chain_key": "bitcoin",
                "address": BTC_ADDRESS,
                "assertion_type": "entity_control",
                "candidate_entity_id": "arkham:fixture",
                "candidate_entity_name": "Fixture Entity",
                "source_authority": "commercial_provider",
                "evidence_tier": "C",
                "verification_method": "api-observation",
                "source_url": "https://example.test/evidence",
                "artifact_sha256": "a" * 64,
                "license_ref": "fixture-license",
                "independence_group": "fixture-provider",
                "observed_at": "2026-07-22T00:00:00Z",
                "evidence_status": "valid",
                "imported_by": "fixture",
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


def test_replay_summary_only_omits_event_records_and_reports_non_interference(
    runtime_root, env_mapping, monkeypatch, capsys
) -> None:
    _configure(monkeypatch, env_mapping)
    input_file = runtime_root / "replay.ndjson"
    input_file.write_text(
        json.dumps(
            {
                "event_id": "bitcoin:fixture:outbox",
                "semantic_decision": "internal_candidate",
                "alert_decision": "send",
                "notification_action": "send_now_with_caveat",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    assert main(
        [
            "replay",
            "quant-crypto-btc",
            "--input",
            str(input_file),
            "--snapshot",
            str(runtime_root / "missing-snapshot"),
            "--summary-only",
        ]
    ) == 0
    output = json.loads(capsys.readouterr().out)

    assert output["events"] == 1
    assert output["impact"]["mail_action_changes"] == 0
    assert output["impact"]["suppression_action_changes"] == 0
    assert "enriched_events" not in output


def test_resolve_override_requires_existing_evidence_and_requires_rebuild(
    runtime_root, env_mapping, monkeypatch, capsys
) -> None:
    _configure(monkeypatch, env_mapping)
    assert main(["init-db"]) == 0
    capsys.readouterr()
    assert main(["evidence", "import", "--file", str(_evidence_file(runtime_root))]) == 0
    capsys.readouterr()

    assert main(
        [
            "resolve",
            "override",
            "--address",
            BTC_ADDRESS,
            "--asserted-value",
            "fixture entity",
            "--decision",
            "select",
            "--reviewer-ref",
            "fixture-review",
            "--reason-ref",
            "https://example.test/review",
            "--reviewed-at",
            "2026-07-22T01:00:00Z",
        ]
    ) == 0
    output = json.loads(capsys.readouterr().out)

    assert output["status"] == "ok"
    assert output["override_id"]
    assert output["requires_rebuild"] is True


def test_bilateral_replay_accepts_multiple_sanitized_input_shards(
    runtime_root, env_mapping, monkeypatch, capsys
) -> None:
    _configure(monkeypatch, env_mapping)
    first = runtime_root / "first.ndjson"
    second = runtime_root / "second.ndjson"
    first.write_text(json.dumps({"output_address": BTC_ADDRESS, "input_addresses": []}) + "\n")
    second.write_text(json.dumps({"output_address": BTC_ADDRESS, "input_addresses": []}) + "\n")

    assert main(
        [
            "replay",
            "btc-whale-bilateral",
            "--input",
            str(first),
            "--input",
            str(second),
            "--snapshot",
            str(runtime_root / "missing-snapshot"),
        ]
    ) == 0
    output = json.loads(capsys.readouterr().out)

    assert output["input_records"] == 2
    assert output["bilateral_impact"]["events"] == 2
    assert "output_address" not in json.dumps(output)
