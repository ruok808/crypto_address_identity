"""Bitcoin mainnet address validation using BIP173/BIP350 checksums."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass


class BitcoinAddressError(ValueError):
    """Raised when a value is not a supported Bitcoin mainnet address."""


@dataclass(frozen=True)
class BitcoinAddressSubject:
    chain_key: str
    canonical_chain: str
    normalized_address: str
    display_address: str
    address_id: str
    address_type: str


_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BASE58_INDEX = {character: index for index, character in enumerate(_BASE58_ALPHABET)}
_BECH32_CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
_BECH32_INDEX = {character: index for index, character in enumerate(_BECH32_CHARSET)}
_BECH32_CONST = 1
_BECH32M_CONST = 0x2BC830A3


def normalize_bitcoin_address(address: str) -> BitcoinAddressSubject:
    """Validate a supported mainnet address and return its canonical subject."""

    if not isinstance(address, str) or not address:
        raise BitcoinAddressError("Bitcoin address must be a non-empty string")

    if address.lower().startswith("bc1"):
        normalized, address_type = _validate_segwit(address)
    elif address.lower().startswith(("tb1", "bcrt1")):
        raise BitcoinAddressError("Only Bitcoin mainnet addresses are supported")
    else:
        normalized, address_type = _validate_base58(address)

    address_id = hashlib.sha256(f"bitcoin:{normalized}".encode("ascii")).hexdigest()
    return BitcoinAddressSubject(
        chain_key="bitcoin",
        canonical_chain="bitcoin:mainnet",
        normalized_address=normalized,
        display_address=normalized,
        address_id=address_id,
        address_type=address_type,
    )


def _validate_base58(address: str) -> tuple[str, str]:
    try:
        value = 0
        for character in address:
            value = value * 58 + _BASE58_INDEX[character]
    except KeyError as exc:
        raise BitcoinAddressError("Invalid Base58 character") from exc

    payload = value.to_bytes((value.bit_length() + 7) // 8, "big")
    leading_zeroes = len(address) - len(address.lstrip("1"))
    decoded = b"\x00" * leading_zeroes + payload
    if len(decoded) != 25:
        raise BitcoinAddressError("Invalid Base58Check address length")

    body, checksum = decoded[:-4], decoded[-4:]
    expected = hashlib.sha256(hashlib.sha256(body).digest()).digest()[:4]
    if checksum != expected:
        raise BitcoinAddressError("Invalid Base58Check checksum")

    version = body[0]
    if version == 0:
        return address, "p2pkh"
    if version == 5:
        return address, "p2sh"
    raise BitcoinAddressError("Only Bitcoin mainnet P2PKH and P2SH are supported")


def _validate_segwit(address: str) -> tuple[str, str]:
    if address.lower() != address and address.upper() != address:
        raise BitcoinAddressError("Mixed-case Bech32 addresses are invalid")
    if any(ord(character) < 33 or ord(character) > 126 for character in address):
        raise BitcoinAddressError("Invalid Bech32 character range")

    normalized = address.lower()
    separator = normalized.rfind("1")
    if separator < 1 or separator + 7 > len(normalized):
        raise BitcoinAddressError("Invalid Bech32 separator or checksum length")

    human_readable_part = normalized[:separator]
    if human_readable_part != "bc":
        raise BitcoinAddressError("Only Bitcoin mainnet Bech32 addresses are supported")
    try:
        data = [_BECH32_INDEX[character] for character in normalized[separator + 1 :]]
    except KeyError as exc:
        raise BitcoinAddressError("Invalid Bech32 data character") from exc

    checksum_variant = _bech32_variant(human_readable_part, data)
    if checksum_variant is None:
        raise BitcoinAddressError("Invalid Bech32 checksum")

    payload = data[:-6]
    if not payload:
        raise BitcoinAddressError("Missing SegWit witness version")
    witness_version = payload[0]
    if witness_version > 16:
        raise BitcoinAddressError("Unsupported SegWit witness version")
    witness_program = _convert_bits(payload[1:], from_bits=5, to_bits=8, pad=False)
    if witness_program is None or not 2 <= len(witness_program) <= 40:
        raise BitcoinAddressError("Invalid SegWit witness program")

    if witness_version == 0:
        if checksum_variant != "bech32" or len(witness_program) not in (20, 32):
            raise BitcoinAddressError("Invalid version 0 SegWit address")
        return normalized, "p2wpkh" if len(witness_program) == 20 else "p2wsh"
    if checksum_variant != "bech32m":
        raise BitcoinAddressError("SegWit version 1+ must use Bech32m")
    return normalized, "p2tr" if witness_version == 1 and len(witness_program) == 32 else "segwit_v1_plus"


def _bech32_variant(human_readable_part: str, data: list[int]) -> str | None:
    polymod = _polymod(_hrp_expand(human_readable_part) + data)
    if polymod == _BECH32_CONST:
        return "bech32"
    if polymod == _BECH32M_CONST:
        return "bech32m"
    return None


def _polymod(values: list[int]) -> int:
    checksum = 1
    generators = (0x3B6A57B2, 0x26508E6D, 0x1EA119FA, 0x3D4233DD, 0x2A1462B3)
    for value in values:
        high_bits = checksum >> 25
        checksum = (checksum & 0x1FFFFFF) << 5 ^ value
        for bit, generator in enumerate(generators):
            if (high_bits >> bit) & 1:
                checksum ^= generator
    return checksum


def _hrp_expand(human_readable_part: str) -> list[int]:
    return [ord(character) >> 5 for character in human_readable_part] + [0] + [
        ord(character) & 31 for character in human_readable_part
    ]


def _convert_bits(
    values: list[int], *, from_bits: int, to_bits: int, pad: bool
) -> list[int] | None:
    accumulator = 0
    bit_count = 0
    output: list[int] = []
    maximum_value = (1 << to_bits) - 1
    maximum_accumulator = (1 << (from_bits + to_bits - 1)) - 1
    for value in values:
        if value < 0 or value >> from_bits:
            return None
        accumulator = ((accumulator << from_bits) | value) & maximum_accumulator
        bit_count += from_bits
        while bit_count >= to_bits:
            bit_count -= to_bits
            output.append((accumulator >> bit_count) & maximum_value)
    if pad:
        if bit_count:
            output.append((accumulator << (to_bits - bit_count)) & maximum_value)
    elif bit_count >= from_bits or ((accumulator << (to_bits - bit_count)) & maximum_value):
        return None
    return output
