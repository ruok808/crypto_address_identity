from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from crypto_address_identity.core.config import Settings
from crypto_address_identity.universe.bitcoin_core import BitcoinCoreProbe


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "bitcoin_core_responses.json"


def settings(tmp_path: Path, **updates: object) -> Settings:
    cookie_file = tmp_path / "bitcoin.cookie"
    cookie_file.write_text("fixture-user:fixture-password", encoding="utf-8")
    values: dict[str, object] = {
        "CAI_DATABASE_PATH": tmp_path / "identity.sqlite3",
        "CAI_RAW_PAYLOAD_ROOT": tmp_path / "raw",
        "CAI_EXPORT_ROOT": tmp_path / "exports",
        "CAI_UNIVERSE_ROOT": tmp_path / "universe",
        "CAI_UNIVERSE_DUCKDB_PATH": tmp_path / "universe" / "catalog.duckdb",
        "CAI_BITCOIN_RPC_URL": "http://127.0.0.1:8332",
        "CAI_BITCOIN_RPC_COOKIE_FILE": cookie_file,
        "CAI_BITCOIN_FINALITY_DEPTH": 6,
        "CAI_BITCOIN_RPC_TIMEOUT_SECONDS": 5,
    }
    values.update(updates)
    return Settings.model_validate(values)


def fixture_values() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def rpc_fixture_transport(
    *,
    blockchain_info: dict[str, object],
    finalized_hash: str,
    finalized_header: dict[str, object],
    indexes: dict[str, object] | None = None,
) -> tuple[httpx.MockTransport, list[dict[str, object]]]:
    calls: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"].startswith("Basic ")
        payload = json.loads(request.content)
        calls.append({"method": payload["method"], "params": payload["params"]})
        results = {
            "getblockchaininfo": blockchain_info,
            "getblockhash": finalized_hash,
            "getblockheader": finalized_header,
            "getindexinfo": indexes or {},
        }
        return httpx.Response(
            200,
            json={
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": results[payload["method"]],
                "error": None,
            },
        )

    return httpx.MockTransport(handler), calls


def test_bitcoin_core_probe_accepts_synced_archival_mainnet(
    tmp_path: Path,
) -> None:
    fixture = fixture_values()
    transport, calls = rpc_fixture_transport(
        blockchain_info=fixture["blockchain_info"],
        finalized_hash=fixture["finalized_hash"],
        finalized_header=fixture["finalized_header"],
        indexes=fixture["indexes"],
    )
    result = BitcoinCoreProbe(settings(tmp_path), transport=transport).run()

    assert [call["method"] for call in calls] == [
        "getblockchaininfo",
        "getblockhash",
        "getblockheader",
        "getindexinfo",
    ]
    assert result.status == "accepted"
    assert result.finalized_height == 900_004
    assert result.finalized_hash == "11" * 32
    assert result.script_completeness is True
    assert "historical_block_scan" in result.capabilities


@pytest.mark.parametrize(
    ("updates", "reason"),
    [
        ({"chain": "test"}, "bitcoin_network_not_mainnet"),
        ({"initialblockdownload": True}, "bitcoin_initial_block_download"),
        ({"headers": 900_011}, "bitcoin_headers_ahead_of_blocks"),
        ({"blocks": 5, "headers": 5}, "bitcoin_finality_unavailable"),
    ],
)
def test_bitcoin_core_probe_blocks_unsafe_chain_state(
    tmp_path: Path, updates: dict[str, object], reason: str
) -> None:
    fixture = fixture_values()
    blockchain_info = {**fixture["blockchain_info"], **updates}
    transport, _ = rpc_fixture_transport(
        blockchain_info=blockchain_info,
        finalized_hash=fixture["finalized_hash"],
        finalized_header=fixture["finalized_header"],
    )

    result = BitcoinCoreProbe(settings(tmp_path), transport=transport).run()

    assert result.status == "blocked"
    assert reason in result.blocking_reasons


def test_bitcoin_core_probe_rejects_shallow_or_mismatched_finalized_header(
    tmp_path: Path,
) -> None:
    fixture = fixture_values()
    for header, reason in (
        (
            {**fixture["finalized_header"], "confirmations": 2},
            "bitcoin_finality_too_shallow",
        ),
        (
            {**fixture["finalized_header"], "height": 900_003},
            "bitcoin_finalized_height_mismatch",
        ),
    ):
        transport, _ = rpc_fixture_transport(
            blockchain_info=fixture["blockchain_info"],
            finalized_hash=fixture["finalized_hash"],
            finalized_header=header,
        )
        result = BitcoinCoreProbe(settings(tmp_path), transport=transport).run()
        assert result.status == "blocked"
        assert reason in result.blocking_reasons


def test_bitcoin_core_probe_marks_pruned_node_partial(tmp_path: Path) -> None:
    fixture = fixture_values()
    transport, _ = rpc_fixture_transport(
        blockchain_info={**fixture["blockchain_info"], "pruned": True},
        finalized_hash=fixture["finalized_hash"],
        finalized_header=fixture["finalized_header"],
        indexes=fixture["indexes"],
    )

    result = BitcoinCoreProbe(settings(tmp_path), transport=transport).run()

    assert result.status == "partial"
    assert result.script_completeness is False
    assert "utxo_probe" in result.capabilities
    assert "historical_block_scan" not in result.capabilities
    assert "bitcoin_pruned_node" in result.warnings


def test_bitcoin_core_probe_fails_closed_on_malformed_rpc_without_secret(
    tmp_path: Path,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "jsonrpc": "2.0",
                "id": 999,
                "result": {"cookie": "fixture-password"},
                "error": None,
            },
        )

    result = BitcoinCoreProbe(
        settings(tmp_path), transport=httpx.MockTransport(handler)
    ).run()
    serialized = result.model_dump_json()

    assert result.status == "blocked"
    assert result.blocking_reasons == ("bitcoin_rpc_rejected",)
    assert "fixture-password" not in serialized
    assert "authorization" not in serialized.lower()
