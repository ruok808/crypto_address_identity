"""Static multi-chain registry with BTC-first enablement enforcement."""

from __future__ import annotations

from dataclasses import dataclass

from crypto_address_identity.core.config import Settings


class UnsupportedChainError(ValueError):
    """Raised before a disabled chain can reach any provider boundary."""


@dataclass(frozen=True)
class ChainDefinition:
    key: str
    canonical_id: str
    family: str


_REGISTRY = {
    "bitcoin": ChainDefinition("bitcoin", "bitcoin:mainnet", "utxo"),
    "ethereum": ChainDefinition("ethereum", "eip155:1", "evm"),
    "bsc": ChainDefinition("bsc", "eip155:56", "evm"),
    "solana": ChainDefinition("solana", "solana:mainnet", "solana"),
    "zcash": ChainDefinition("zcash", "zcash:mainnet", "utxo_privacy"),
}


def enabled_registry(settings: Settings) -> tuple[ChainDefinition, ...]:
    return tuple(_REGISTRY[key] for key in settings.enabled_chains)


def require_enabled_chain(chain_key: str, settings: Settings) -> ChainDefinition:
    normalized = chain_key.strip().lower()
    definition = _REGISTRY.get(normalized)
    if definition is None or normalized not in settings.enabled_chains:
        raise UnsupportedChainError(f"Unsupported chain: {normalized}")
    return definition
