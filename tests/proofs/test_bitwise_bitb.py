from __future__ import annotations

import json
from datetime import UTC, datetime

import httpx
import pytest

import crypto_address_identity.cli as cli
from crypto_address_identity.evidence import EvidenceService, VerifierRegistry
from crypto_address_identity.proofs.bitwise_bitb import (
    BITWISE_BITB_PUBLIC_URL,
    BitwiseBitbEvidenceError,
    fetch_bitwise_bitb_snapshot,
    official_bitwise_evidence_records,
    parse_bitwise_bitb_page,
)
from crypto_address_identity.storage.sqlite import IdentityDatabase


FIRST_ADDRESS = "bc1qs3njm2cnmj4s2nuk444vm9cfyxs8ktzqzsx2qh"
SECOND_ADDRESS = "bc1qu97pnw3arh9gslvt84r3h8rzv2q7ssaevaq5ay"


def _page(*, include_second: bool = True) -> bytes:
    wallets = [{"active": True, "address": FIRST_ADDRESS, "balance": 1.0}]
    if include_second:
        wallets.extend(
            [
                {"active": False, "address": SECOND_ADDRESS, "balance": 2.0},
                {"active": True, "address": SECOND_ADDRESS, "balance": 3.0},
            ]
        )
    payload = {
        "props": {
            "pageProps": {
                "unrelatedSignedUrl": "https://example.test/object?token=redacted",
                "wallets": {
                    "updatedAt": "2026-07-22T14:15:00.516Z",
                    "walletBalances": wallets,
                },
            }
        }
    }
    return (
        '<html><script id="__NEXT_DATA__" type="application/json">'
        + json.dumps(payload)
        + "</script></html>"
    ).encode()


def test_parser_keeps_only_active_wallets_and_sanitizes_unrelated_page_values() -> None:
    snapshot = parse_bitwise_bitb_page(
        _page(),
        retrieved_at=datetime(2026, 7, 22, 15, 30, tzinfo=UTC),
    )
    safe_payload = json.loads(snapshot.safe_payload())

    assert snapshot.addresses == (FIRST_ADDRESS, SECOND_ADDRESS)
    assert safe_payload["active_addresses"] == [FIRST_ADDRESS, SECOND_ADDRESS]
    assert "unrelatedSignedUrl" not in json.dumps(safe_payload)
    assert "token=" not in json.dumps(safe_payload)


def test_parser_rejects_missing_or_invalid_wallet_data() -> None:
    with pytest.raises(BitwiseBitbEvidenceError):
        parse_bitwise_bitb_page(b"<html></html>", retrieved_at=datetime(2026, 7, 22, tzinfo=UTC))
    malformed = _page(include_second=False).replace(FIRST_ADDRESS.encode(), b"not-a-bitcoin-address")
    with pytest.raises(BitwiseBitbEvidenceError):
        parse_bitwise_bitb_page(malformed, retrieved_at=datetime(2026, 7, 22, tzinfo=UTC))


def test_fetcher_uses_the_fixed_public_issuer_url() -> None:
    seen_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_urls.append(str(request.url))
        return httpx.Response(200, content=_page())

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        snapshot = fetch_bitwise_bitb_snapshot(client, retrieved_at=datetime(2026, 7, 22, tzinfo=UTC))

    assert snapshot.addresses == (FIRST_ADDRESS, SECOND_ADDRESS)
    assert seen_urls == [BITWISE_BITB_PUBLIC_URL]


def test_official_publication_records_are_tier_b_and_expire(runtime_root) -> None:
    snapshot = parse_bitwise_bitb_page(_page(), retrieved_at=datetime(2026, 7, 22, tzinfo=UTC))
    evidence = official_bitwise_evidence_records(snapshot, artifact_sha256="a" * 64)
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()

    result = EvidenceService(database, VerifierRegistry()).import_records(evidence)

    assert result.inserted_count == 2
    with database.read_connection() as connection:
        rows = connection.execute(
            "SELECT evidence_tier, source_authority, expires_at FROM identity_evidence ORDER BY evidence_id"
        ).fetchall()
    assert {tuple(row[:2]) for row in rows} == {("B", "official")}
    assert all(row["expires_at"].startswith("2026-08-22") for row in rows)


def test_cli_dry_run_fetches_but_does_not_persist(runtime_root, env_mapping, monkeypatch, capsys) -> None:
    for key, value in env_mapping.items():
        monkeypatch.setenv(key, value)
    snapshot = parse_bitwise_bitb_page(_page(), retrieved_at=datetime(2026, 7, 22, tzinfo=UTC))
    monkeypatch.setattr(cli, "fetch_bitwise_bitb_snapshot", lambda client: snapshot)

    exit_code = cli.main(["evidence", "import-bitwise-bitb", "--dry-run"])

    assert exit_code == 0
    output = capsys.readouterr().out
    assert '"status": "dry_run"' in output
    assert '"address_count": 2' in output
    assert not (runtime_root / "raw").exists()
