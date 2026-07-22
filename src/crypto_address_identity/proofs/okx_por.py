"""Verify the BTC 2-of-3 address proofs published in OKX PoR archives.

The source archive itself is retained as a restricted raw object by the CLI.
This module deliberately keeps messages and signatures in memory only; ledger
records retain the public source URL and artifact hash, never the signatures.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Iterable

from crypto_address_identity.chains.bitcoin import normalize_bitcoin_address
from crypto_address_identity.evidence import EvidenceInput, VerificationResult

OKX_BTC_MESSAGE = "I am an OKX address"
OKX_BTC_POR_VERIFIER = "okx-btc-por-v1"
OKX_ENTITY_ID = "official:okx"
OKX_ENTITY_NAME = "OKX"

_P = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F
_N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
_G = (
    0x79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798,
    0x483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8,
)
_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


class OkxPorProofError(ValueError):
    """Raised when an expected OKX public proof archive is malformed."""


@dataclass(frozen=True)
class OkxBitcoinPorRecord:
    """One BTC 2-of-3 proof row from an OKX public audit archive."""

    address: str
    message: str
    signatures: tuple[str, str]
    redeem_script_hex: str
    source_member: str
    row_number: int


@dataclass(frozen=True)
class VerifiedOkxBitcoinPorRecord:
    """A proof row that passed address, signature, and redeem-script checks."""

    address: str
    source_member: str
    row_number: int


@dataclass(frozen=True)
class OkxPorVerificationSummary:
    """Safe aggregate outcome for one official archive."""

    parsed_btc_multisig_rows: int
    verification_candidate_rows: int
    verified_rows: int
    invalid_rows: int
    selected_rows: int


class OkxBitcoinPorVerifier:
    """Named Tier-A verifier bound to an in-memory set of verified proofs."""

    name = OKX_BTC_POR_VERIFIER

    def __init__(self, *, artifact_sha256: str, verified_addresses: Iterable[str]) -> None:
        self._artifact_sha256 = artifact_sha256
        self._verified_addresses = frozenset(verified_addresses)

    def verify(self, record: EvidenceInput) -> VerificationResult:
        if (
            record.artifact_sha256 != self._artifact_sha256
            or record.chain_key != "bitcoin"
            or record.assertion_type != "entity_control"
            or record.candidate_entity_id != OKX_ENTITY_ID
            or record.candidate_entity_name != OKX_ENTITY_NAME
        ):
            return VerificationResult.INVALID
        return (
            VerificationResult.VALID
            if record.subject.normalized_address in self._verified_addresses
            else VerificationResult.INVALID
        )


def parse_okx_btc_multisig_rows(archive_payload: bytes) -> list[OkxBitcoinPorRecord]:
    """Extract eligible BTC 2-of-3 rows from an OKX public ZIP archive.

    The published CSV mixes simple two-column aggregate rows with detailed
    address-audit rows, so parsing uses row shape rather than the first header.
    """

    try:
        with zipfile.ZipFile(io.BytesIO(archive_payload)) as archive:
            members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if not members:
                raise OkxPorProofError("OKX archive has no CSV member")
            records: list[OkxBitcoinPorRecord] = []
            for member in sorted(members):
                content = archive.read(member).decode("utf-8-sig")
                for row_number, row in enumerate(csv.reader(io.StringIO(content)), start=1):
                    record = _parse_row(row, source_member=member, row_number=row_number)
                    if record is not None:
                        records.append(record)
    except (OSError, UnicodeDecodeError, zipfile.BadZipFile) as exc:
        raise OkxPorProofError("Invalid OKX PoR archive") from exc
    return sorted(records, key=lambda record: (record.address, record.source_member, record.row_number))


def verify_okx_btc_multisig_record(record: OkxBitcoinPorRecord) -> bool:
    """Verify two compact BTC message signatures against a 2-of-3 P2SH proof."""

    try:
        subject = normalize_bitcoin_address(record.address)
        if subject.address_type != "p2sh" or record.message != OKX_BTC_MESSAGE:
            return False
        redeem_script = bytes.fromhex(record.redeem_script_hex)
        public_keys = _parse_2_of_3_redeem_script(redeem_script)
        if _p2sh_address(redeem_script) != subject.normalized_address:
            return False
        digest = _bitcoin_message_digest(record.message)
        recovered = {_recover_compact_public_key(signature, digest) for signature in record.signatures}
        return None not in recovered and len(recovered) == 2 and recovered.issubset(public_keys)
    except (ValueError, BitcoinMessageError):
        return False


def verified_okx_records(
    archive_payload: bytes, *, limit: int | None = None
) -> tuple[list[VerifiedOkxBitcoinPorRecord], OkxPorVerificationSummary]:
    """Verify a deterministically ordered bounded seed from one archive."""

    parsed = parse_okx_btc_multisig_rows(archive_payload)
    candidates = parsed[: max(0, limit)] if limit is not None else parsed
    verified = [
        VerifiedOkxBitcoinPorRecord(record.address, record.source_member, record.row_number)
        for record in candidates
        if verify_okx_btc_multisig_record(record)
    ]
    selected = verified
    return selected, OkxPorVerificationSummary(
        parsed_btc_multisig_rows=len(parsed),
        verification_candidate_rows=len(candidates),
        verified_rows=len(verified),
        invalid_rows=len(candidates) - len(verified),
        selected_rows=len(selected),
    )


def official_okx_evidence_records(
    verified_records: Iterable[VerifiedOkxBitcoinPorRecord],
    *,
    source_url: str,
    artifact_sha256: str,
    observed_at: datetime,
) -> list[EvidenceInput]:
    """Create Tier-A evidence without retaining raw signature material."""

    if observed_at.tzinfo is None:
        raise OkxPorProofError("observed_at must be timezone-aware")
    timestamp = observed_at.astimezone(UTC)
    return [
        EvidenceInput.model_validate(
            {
                "chain_key": "bitcoin",
                "address": record.address,
                "assertion_type": "entity_control",
                "candidate_entity_id": OKX_ENTITY_ID,
                "candidate_entity_name": OKX_ENTITY_NAME,
                "source_authority": "official",
                "evidence_tier": "A",
                "verification_method": OKX_BTC_POR_VERIFIER,
                "verification_result": "valid",
                "source_url": source_url,
                "artifact_sha256": artifact_sha256,
                "license_ref": "OKX Proof of Reserves public audit file",
                "independence_group": "okx_por",
                "asserted_at": timestamp,
                "observed_at": timestamp,
                "evidence_status": "valid",
                "imported_by": "okx_por_import",
            }
        )
        for record in verified_records
    ]


def _parse_row(row: list[str], *, source_member: str, row_number: int) -> OkxBitcoinPorRecord | None:
    if len(row) < 9 or row[0].strip().upper() != "BTC" or row[5].strip() != OKX_BTC_MESSAGE:
        return None
    address = row[3].strip()
    first_signature = row[6].strip()
    second_signature = row[7].strip()
    redeem_script_hex = row[8].strip().lower()
    if not address or not first_signature or not second_signature or not redeem_script_hex:
        return None
    try:
        if normalize_bitcoin_address(address).address_type != "p2sh":
            return None
        bytes.fromhex(redeem_script_hex)
    except ValueError:
        return None
    return OkxBitcoinPorRecord(
        address=address,
        message=row[5].strip(),
        signatures=(first_signature, second_signature),
        redeem_script_hex=redeem_script_hex,
        source_member=source_member,
        row_number=row_number,
    )


class BitcoinMessageError(ValueError):
    """Raised when a legacy compact Bitcoin signed-message proof is invalid."""


def _bitcoin_message_digest(message: str) -> bytes:
    payload = message.encode("utf-8")
    magic = b"Bitcoin Signed Message:\n"
    encoded = _compact_size(len(magic)) + magic + _compact_size(len(payload)) + payload
    return _sha256d(encoded)


def _compact_size(size: int) -> bytes:
    if size < 0:
        raise BitcoinMessageError("Negative compact-size value")
    if size < 253:
        return bytes([size])
    if size <= 0xFFFF:
        return b"\xfd" + size.to_bytes(2, "little")
    if size <= 0xFFFFFFFF:
        return b"\xfe" + size.to_bytes(4, "little")
    return b"\xff" + size.to_bytes(8, "little")


def _recover_compact_public_key(signature_text: str, digest: bytes) -> bytes | None:
    try:
        encoded = base64.b64decode(signature_text, validate=True)
    except (ValueError, base64.binascii.Error):
        return None
    if len(encoded) != 65:
        return None
    header = encoded[0]
    if not 27 <= header <= 34:
        return None
    flag = header - 27
    recovery_id = flag & 3
    compressed = flag >= 4
    r = int.from_bytes(encoded[1:33], "big")
    s = int.from_bytes(encoded[33:65], "big")
    if not 0 < r < _N or not 0 < s < _N:
        return None
    x = r + (recovery_id >> 1) * _N
    if x >= _P:
        return None
    y_squared = (pow(x, 3, _P) + 7) % _P
    y = pow(y_squared, (_P + 1) // 4, _P)
    if (y * y) % _P != y_squared:
        return None
    if (y & 1) != (recovery_id & 1):
        y = _P - y
    point_r = (x, y)
    if _point_multiply(_N, point_r) is not None:
        return None
    digest_int = int.from_bytes(digest, "big")
    inverse_r = pow(r, -1, _N)
    public_key = _point_add(
        _point_multiply((s * inverse_r) % _N, point_r),
        _point_multiply((-digest_int * inverse_r) % _N, _G),
    )
    if public_key is None or not _verify_ecdsa(public_key, digest_int, r, s):
        return None
    return _serialize_public_key(public_key, compressed=compressed)


def _verify_ecdsa(public_key: tuple[int, int], digest_int: int, r: int, s: int) -> bool:
    inverse_s = pow(s, -1, _N)
    point = _point_add(
        _point_multiply((digest_int * inverse_s) % _N, _G),
        _point_multiply((r * inverse_s) % _N, public_key),
    )
    return point is not None and point[0] % _N == r


def _point_add(
    left: tuple[int, int] | None, right: tuple[int, int] | None
) -> tuple[int, int] | None:
    if left is None:
        return right
    if right is None:
        return left
    x1, y1 = left
    x2, y2 = right
    if x1 == x2 and (y1 + y2) % _P == 0:
        return None
    if left == right:
        slope = (3 * x1 * x1) * pow(2 * y1, -1, _P) % _P
    else:
        slope = (y2 - y1) * pow(x2 - x1, -1, _P) % _P
    x3 = (slope * slope - x1 - x2) % _P
    return x3, (slope * (x1 - x3) - y1) % _P


def _point_multiply(scalar: int, point: tuple[int, int]) -> tuple[int, int] | None:
    result: tuple[int, int] | None = None
    addend = point
    while scalar:
        if scalar & 1:
            result = _point_add(result, addend)
        addend = _point_add(addend, addend)
        scalar >>= 1
    return result


def _serialize_public_key(point: tuple[int, int], *, compressed: bool) -> bytes:
    x, y = point
    if compressed:
        return bytes([2 + (y & 1)]) + x.to_bytes(32, "big")
    return b"\x04" + x.to_bytes(32, "big") + y.to_bytes(32, "big")


def _parse_2_of_3_redeem_script(script: bytes) -> frozenset[bytes]:
    if len(script) != 105 or script[0] != 0x52 or script[-2:] != b"\x53\xae":
        raise BitcoinMessageError("Unsupported redeem script")
    public_keys: list[bytes] = []
    offset = 1
    for _ in range(3):
        if script[offset] != 33:
            raise BitcoinMessageError("Unexpected public-key push")
        public_keys.append(script[offset + 1 : offset + 34])
        offset += 34
    if offset != 103 or len(set(public_keys)) != 3:
        raise BitcoinMessageError("Invalid 2-of-3 public keys")
    return frozenset(public_keys)


def _p2sh_address(redeem_script: bytes) -> str:
    body = b"\x05" + _hash160(redeem_script)
    return _base58_encode(body + _sha256d(body)[:4])


def _hash160(value: bytes) -> bytes:
    return hashlib.new("ripemd160", hashlib.sha256(value).digest()).digest()


def _sha256d(value: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(value).digest()).digest()


def _base58_encode(value: bytes) -> str:
    number = int.from_bytes(value, "big")
    encoded = ""
    while number:
        number, remainder = divmod(number, 58)
        encoded = _BASE58_ALPHABET[remainder] + encoded
    leading_zeroes = len(value) - len(value.lstrip(b"\x00"))
    return "1" * leading_zeroes + (encoded or "1")
