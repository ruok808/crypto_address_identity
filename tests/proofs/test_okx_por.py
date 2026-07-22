from __future__ import annotations

import io
import zipfile
import base64
from datetime import UTC, datetime

from crypto_address_identity.cli import main
from crypto_address_identity.evidence import EvidenceService, VerificationResult, VerifierRegistry
from crypto_address_identity.proofs.okx_por import (
    OKX_BTC_MESSAGE,
    OKX_BTC_POR_VERIFIER,
    OKX_ENTITY_ID,
    OkxBitcoinPorVerifier,
    VerifiedOkxBitcoinPorRecord,
    official_okx_evidence_records,
    parse_okx_btc_multisig_rows,
    verified_okx_records,
)
from crypto_address_identity.proofs import okx_por
from crypto_address_identity.storage.sqlite import IdentityDatabase


P2SH_ADDRESS = "3QJmV3qfvL9SuYo34YihAf3sRCW3qSinyC"


def _archive(*rows: list[str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        lines = ["coin,amount", "BTC,1"]
        lines.extend(",".join(f'"{value}"' for value in row) for row in rows)
        archive.writestr("okx_por.csv", "\n".join(lines) + "\n")
    return buffer.getvalue()


def _signed_shape(*, address: str = P2SH_ADDRESS, signature: str = "not-base64") -> list[str]:
    return [
        "BTC",
        "BTC",
        "1",
        address,
        "1.0",
        OKX_BTC_MESSAGE,
        signature,
        signature,
        "52" + "21" + "02" * 33 + "21" + "03" * 33 + "21" + "02" * 33 + "53ae",
    ]


def test_parser_uses_detailed_btc_rows_not_two_column_aggregate() -> None:
    records = parse_okx_btc_multisig_rows(_archive(_signed_shape()))

    assert len(records) == 1
    assert records[0].address == P2SH_ADDRESS
    assert records[0].message == OKX_BTC_MESSAGE


def test_invalid_signatures_never_become_verified_evidence() -> None:
    verified, summary = verified_okx_records(_archive(_signed_shape()))

    assert verified == []
    assert summary.parsed_btc_multisig_rows == 1
    assert summary.verification_candidate_rows == 1
    assert summary.verified_rows == 0
    assert summary.invalid_rows == 1


def test_valid_compact_signatures_and_redeem_script_become_verified_evidence() -> None:
    archive = _valid_archive()
    expected_address = parse_okx_btc_multisig_rows(archive)[0].address
    verified, summary = verified_okx_records(archive)

    assert [record.address for record in verified] == [expected_address]
    assert summary.verified_rows == 1
    assert summary.invalid_rows == 0


def test_named_verifier_binds_tier_a_import_to_verified_address_and_artifact(runtime_root) -> None:
    artifact_sha256 = "b" * 64
    verified = [VerifiedOkxBitcoinPorRecord(P2SH_ADDRESS, "okx_por.csv", 3)]
    evidence = official_okx_evidence_records(
        verified,
        source_url="https://static.okx.com/example.zip",
        artifact_sha256=artifact_sha256,
        observed_at=datetime(2026, 7, 22, tzinfo=UTC),
    )
    verifier = OkxBitcoinPorVerifier(
        artifact_sha256=artifact_sha256,
        verified_addresses=[P2SH_ADDRESS],
    )
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()

    result = EvidenceService(database, VerifierRegistry([verifier])).import_records(evidence)

    assert verifier.name == OKX_BTC_POR_VERIFIER
    assert evidence[0].candidate_entity_id == OKX_ENTITY_ID
    assert result.inserted_count == 1
    with database.read_connection() as connection:
        row = connection.execute(
            "SELECT evidence_tier, verification_result FROM identity_evidence"
        ).fetchone()
    assert tuple(row) == ("A", VerificationResult.VALID.value)


def test_named_verifier_rejects_different_artifact() -> None:
    evidence = official_okx_evidence_records(
        [VerifiedOkxBitcoinPorRecord(P2SH_ADDRESS, "okx_por.csv", 3)],
        source_url="https://static.okx.com/example.zip",
        artifact_sha256="a" * 64,
        observed_at=datetime(2026, 7, 22, tzinfo=UTC),
    )[0]
    verifier = OkxBitcoinPorVerifier(
        artifact_sha256="b" * 64,
        verified_addresses=[P2SH_ADDRESS],
    )

    assert verifier.verify(evidence) is VerificationResult.INVALID


def test_okx_por_cli_dry_run_reports_invalid_rows_without_writing(tmp_path, capsys) -> None:
    archive = tmp_path / "okx_por.zip"
    archive.write_bytes(_archive(_signed_shape()))

    exit_code = main(
        [
            "evidence",
            "import-okx-btc-por",
            "--archive",
            str(archive),
            "--source-url",
            "https://static.okx.com/example.zip",
            "--observed-at",
            "2026-07-22T00:00:00Z",
            "--dry-run",
        ]
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert '"status": "dry_run"' in output
    assert '"verified_rows": 0' in output


def test_okx_por_cli_rejects_non_public_source_before_raw_persistence(
    tmp_path, runtime_root, env_mapping, monkeypatch, capsys
) -> None:
    archive = tmp_path / "okx_por.zip"
    archive.write_bytes(_valid_archive())
    for key, value in env_mapping.items():
        monkeypatch.setenv(key, value)

    exit_code = main(
        [
            "evidence",
            "import-okx-btc-por",
            "--archive",
            str(archive),
            "--source-url",
            "https://static.okx.com/archive.zip?token=not-allowed",
            "--observed-at",
            "2026-07-22T00:00:00Z",
        ]
    )

    assert exit_code == 2
    assert '"error_code": "invalid_input"' in capsys.readouterr().out
    assert not (runtime_root / "raw").exists()


def _compressed_public_key(private_key: int) -> bytes:
    point = okx_por._point_multiply(private_key, okx_por._G)
    assert point is not None
    return okx_por._serialize_public_key(point, compressed=True)


def _compact_signature(*, private_key: int, nonce: int) -> str:
    digest = okx_por._bitcoin_message_digest(OKX_BTC_MESSAGE)
    digest_int = int.from_bytes(digest, "big")
    nonce_point = okx_por._point_multiply(nonce, okx_por._G)
    assert nonce_point is not None
    r = nonce_point[0] % okx_por._N
    s = pow(nonce, -1, okx_por._N) * (digest_int + r * private_key) % okx_por._N
    recovery_id = (2 if nonce_point[0] >= okx_por._N else 0) | (nonce_point[1] & 1)
    compact = bytes([27 + 4 + recovery_id]) + r.to_bytes(32, "big") + s.to_bytes(32, "big")
    return base64.b64encode(compact).decode("ascii")


def _valid_archive() -> bytes:
    public_keys = [_compressed_public_key(private_key) for private_key in (1, 2, 3)]
    redeem_script = b"\x52" + b"".join(b"\x21" + key for key in public_keys) + b"\x53\xae"
    return _archive(
        [
            "BTC",
            "BTC",
            "1",
            okx_por._p2sh_address(redeem_script),
            "1.0",
            OKX_BTC_MESSAGE,
            _compact_signature(private_key=1, nonce=5),
            _compact_signature(private_key=2, nonce=7),
            redeem_script.hex(),
        ]
    )
