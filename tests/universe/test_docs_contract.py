from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).parents[2]
README = ROOT / "README.md"
OPERATIONS = ROOT / "docs" / "btc_identity_operations.md"
ENV_EXAMPLE = ROOT / "conf" / "env" / "address_identity.env.example"


def test_phase_one_docs_state_the_provider_free_boundary() -> None:
    operations = OPERATIONS.read_text(encoding="utf-8")

    assert "provider_requests=0" in operations
    assert "--execute-chain-read" in operations
    assert "--maximum-bytes-billed" in operations
    assert "does not approve the 1,000-address canary" in operations
    assert "BigQuery free tier is account-wide" in operations
    assert "public BigQuery dataset is not automatically free" in operations
    assert "pruned Bitcoin Core node cannot prove historical script coverage" in operations


def test_phase_one_docs_define_the_stop_before_canary_sequence() -> None:
    operations = OPERATIONS.read_text(encoding="utf-8")

    expected_order = (
        "Offline configuration validation",
        "BigQuery metadata and dry-run probe",
        "Bitcoin Core read-only probe",
        "Cutoff height/hash reconciliation",
        "Review dry-run bytes",
        "Separately approved chain read",
        "Campaign checksum verification",
        "Aggregate-only candidate dry-run",
        "Stop and report",
    )
    positions = [operations.index(item) for item in expected_order]

    assert positions == sorted(positions)


def test_readme_exposes_offline_universe_commands_and_approval_gate() -> None:
    readme = README.read_text(encoding="utf-8")

    assert "cai universe probe bigquery --dry-run" in readme
    assert "cai universe candidates" in readme
    assert "No BigQuery, Bitcoin Core, or 0xRouter request" in readme
    assert "separate explicit approval" in readme


def test_environment_example_contains_no_secret_material() -> None:
    lines = ENV_EXAMPLE.read_text(encoding="utf-8").splitlines()
    assignments = {
        name: value
        for line in lines
        if line and not line.startswith("#") and "=" in line
        for name, value in (line.split("=", 1),)
    }

    assert assignments.get("CAI_BIGQUERY_BILLING_PROJECT") == ""
    assert "GOOGLE_APPLICATION_CREDENTIALS" not in assignments
    assert "CAI_BITCOIN_RPC_PASSWORD" not in assignments
    assert "CAI_BITCOIN_RPC_COOKIE" not in assignments
    assert "CAI_0XROUTER_TOKEN" not in assignments
    assert assignments["CAI_BIGQUERY_MAXIMUM_BYTES_BILLED"] == "0"
