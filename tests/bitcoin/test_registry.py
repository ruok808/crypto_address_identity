from __future__ import annotations

import pytest

from crypto_address_identity.chains.registry import (
    UnsupportedChainError,
    enabled_registry,
    require_enabled_chain,
)
from crypto_address_identity.core.config import Settings


def test_bitcoin_is_the_only_enabled_chain(env_mapping: dict[str, str]) -> None:
    settings = Settings.model_validate(env_mapping)

    assert tuple(chain.key for chain in enabled_registry(settings)) == ("bitcoin",)
    assert require_enabled_chain("bitcoin", settings).canonical_id == "bitcoin:mainnet"


@pytest.mark.parametrize("chain_key", ["ethereum", "bsc", "solana", "zcash", "unknown"])
def test_non_btc_chain_is_rejected_before_dispatch(
    env_mapping: dict[str, str], chain_key: str
) -> None:
    settings = Settings.model_validate(env_mapping)

    with pytest.raises(UnsupportedChainError):
        require_enabled_chain(chain_key, settings)
