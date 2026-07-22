from __future__ import annotations

import pytest

from crypto_address_identity.chains.bitcoin import (
    BitcoinAddressError,
    normalize_bitcoin_address,
)


@pytest.mark.parametrize(
    ("address", "expected"),
    [
        ("1BoatSLRHtKNngkdXEeobR76b53LETtpyT", "1BoatSLRHtKNngkdXEeobR76b53LETtpyT"),
        ("3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy", "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy"),
        ("bc1qcr8te4kr609gcawutmrza0j4xv80jy8z306fyu", "bc1qcr8te4kr609gcawutmrza0j4xv80jy8z306fyu"),
        ("BC1SW50QGDZ25J", "bc1sw50qgdz25j"),
        ("BC1QCR8TE4KR609GCAWUTMRZA0J4XV80JY8Z306FYU", "bc1qcr8te4kr609gcawutmrza0j4xv80jy8z306fyu"),
    ],
)
def test_normalizes_valid_bitcoin_mainnet_addresses(address: str, expected: str) -> None:
    subject = normalize_bitcoin_address(address)

    assert subject.chain_key == "bitcoin"
    assert subject.canonical_chain == "bitcoin:mainnet"
    assert subject.normalized_address == expected
    assert len(subject.address_id) == 64


@pytest.mark.parametrize(
    "address",
    [
        "1BoatSLRHtKNngkdXEeobR76b53LETtpyU",
        "tb1qfmcy0kp0txx7l3hpd0ue5v4qah7q0y2azcjj7r",
        "bc1Qcr8te4kr609gcawutmrza0j4xv80jy8z306fyu",
        "not-a-bitcoin-address",
    ],
)
def test_rejects_invalid_or_non_mainnet_addresses(address: str) -> None:
    with pytest.raises(BitcoinAddressError):
        normalize_bitcoin_address(address)
