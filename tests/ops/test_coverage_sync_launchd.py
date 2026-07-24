from __future__ import annotations

import plistlib
import os
from pathlib import Path
import subprocess


ROOT = Path(__file__).parents[2]
LAUNCHD_ROOT = ROOT / "ops" / "launchd"


def test_coverage_sync_launchagent_is_daily_and_nonresident() -> None:
    payload = plistlib.loads(
        (LAUNCHD_ROOT / "com.ruok808.crypto-address-identity.coverage-sync.plist.tmpl").read_bytes()
    )

    assert payload["Label"] == "com.ruok808.crypto-address-identity.coverage-sync"
    assert payload["ProgramArguments"] == ["__WORKER_PATH__"]
    assert payload["StartCalendarInterval"] == {"Hour": 3, "Minute": 20}
    assert payload["ThrottleInterval"] == 300
    assert "RunAtLoad" not in payload
    assert "KeepAlive" not in payload
    assert payload["EnvironmentVariables"] == {
        "CAI_PROJECT_ROOT": "__PROJECT_ROOT__",
        "CAI_RUNTIME_ENV_PATH": "__RUNTIME_ENV_PATH__",
        "CAI_PYTHON_BIN": "__PYTHON_BIN__",
    }


def test_coverage_sync_worker_requires_a_private_runtime_env_and_executes_once() -> None:
    payload = (LAUNCHD_ROOT / "coverage_sync_worker.sh").read_text(encoding="utf-8")

    assert "CAI_RUNTIME_ENV_PATH" in payload
    assert "mode 0600" in payload
    assert "coverage-sync run" in payload
    assert "--dry-run" not in payload
    assert "CAI_0XROUTER_TOKEN=<" not in payload
    assert "quant_crypto/configs" not in payload
    assert "source \"$CAI_RUNTIME_ENV_PATH\"" not in payload


def test_coverage_sync_worker_reads_only_a_single_token_assignment(tmp_path: Path) -> None:
    runtime_env = tmp_path / "coverage-sync.env"
    runtime_env.write_text("CAI_0XROUTER_TOKEN=fixture-token\n", encoding="utf-8")
    runtime_env.chmod(0o600)
    fake_python = tmp_path / "fake-python"
    fake_python.write_text("#!/bin/zsh\nprintf '%s\n' \"$*\"\n", encoding="utf-8")
    fake_python.chmod(0o755)
    environment = {
        **os.environ,
        "CAI_PROJECT_ROOT": str(ROOT),
        "CAI_RUNTIME_ENV_PATH": str(runtime_env),
        "CAI_PYTHON_BIN": str(fake_python),
    }

    completed = subprocess.run(
        ["zsh", str(LAUNCHD_ROOT / "coverage_sync_worker.sh")],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    assert "crypto_address_identity coverage-sync run" in completed.stdout
    assert "fixture-token" not in completed.stdout
    assert "fixture-token" not in completed.stderr

    runtime_env.write_text(
        "CAI_0XROUTER_TOKEN=fixture-token\nCAI_DATABASE_PATH=unexpected\n",
        encoding="utf-8",
    )
    runtime_env.chmod(0o600)
    rejected = subprocess.run(
        ["zsh", str(LAUNCHD_ROOT / "coverage_sync_worker.sh")],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert rejected.returncode == 78
    assert "must contain exactly one provider token assignment" in rejected.stderr
    assert "fixture-token" not in rejected.stderr


def test_installer_validates_runtime_env_before_loading_the_agent() -> None:
    payload = (LAUNCHD_ROOT / "install_coverage_sync_launch_agent.sh").read_text(encoding="utf-8")

    assert "CAI_0XROUTER_TOKEN=.+" in payload
    assert "mode 0600" in payload
    assert "plutil -lint" in payload
    assert "launchctl bootstrap" in payload
    assert "coverage-sync LaunchAgent is already loaded" in payload
