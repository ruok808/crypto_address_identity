"""Read-only BTC enrichment contract for future quant_crypto integration."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

from crypto_address_identity.chains.bitcoin import BitcoinAddressError, normalize_bitcoin_address
from crypto_address_identity.exports import ResolverSnapshot


@dataclass(frozen=True)
class IdentityLookup:
    identity_lookup_status: str
    identity_state: str
    identity_resolution_version: str | None
    identity_resolved_at: str | None
    identity_entity_display: str | None
    identity_wallet_role_display: str | None
    identity_operational_tier: str
    identity_conflict_set_id: str | None


@dataclass(frozen=True)
class ReplayResult:
    events: tuple[dict[str, Any], ...]
    changed_business_fields: int


class IdentityEnricher:
    """Loads an immutable snapshot or degrades every lookup into a caveat."""

    def __init__(self, snapshot: ResolverSnapshot | None) -> None:
        self._snapshot = snapshot

    @classmethod
    def from_snapshot_directory(cls, directory: Path) -> "IdentityEnricher":
        try:
            return cls(ResolverSnapshot.load(directory))
        except ValueError:
            return cls(None)

    def lookup(self, address: str, *, assertion_type: str = "entity_control") -> IdentityLookup:
        if self._snapshot is None:
            return _lookup("snapshot_invalid", "unattributed", None, None, None, "none", None)
        try:
            subject = normalize_bitcoin_address(address)
        except BitcoinAddressError:
            return _lookup("unsupported", "unsupported", None, None, None, "none", None)
        record = self._snapshot.lookup("bitcoin", subject.normalized_address, assertion_type)
        if record is None:
            return _lookup("not_found", "unattributed", None, None, None, "none", None)
        state = record["state"]
        status = {
            "ambiguous": "ambiguous",
            "stale": "stale",
            "unsupported": "unsupported",
        }.get(state, "found")
        return _lookup(
            status,
            state,
            record.get("resolution_version"),
            record.get("resolved_at"),
            record.get("accepted_entity"),
            record.get("operational_tier", "none"),
            record.get("conflict_set_id"),
        )


def replay_events(
    events: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...], enricher: IdentityEnricher
) -> ReplayResult:
    """Add only identity fields to event copies and count any prohibited change."""

    protected_fields = (
        "event_id",
        "amount_btc",
        "direction",
        "threshold_result",
        "quality_decision",
        "alert_decision",
        "ownership_semantics",
    )
    output: list[dict[str, Any]] = []
    changed_business_fields = 0
    for event in events:
        enriched = dict(event)
        address = _event_address(event)
        lookup = enricher.lookup(address) if address else _lookup(
            "not_found", "unattributed", None, None, None, "none", None
        )
        enriched.update(asdict(lookup))
        changed_business_fields += sum(
            event.get(field) != enriched.get(field) for field in protected_fields
        )
        output.append(enriched)
    return ReplayResult(tuple(output), changed_business_fields)


def _event_address(event: Mapping[str, Any]) -> str | None:
    for field in ("watched_address", "output_address", "counterparty_address", "input_address"):
        value = event.get(field)
        if isinstance(value, str):
            return value
    return None


def _lookup(
    status: str,
    state: str,
    resolution_version: str | None,
    resolved_at: str | None,
    entity_display: str | None,
    operational_tier: str,
    conflict_set_id: str | None,
) -> IdentityLookup:
    return IdentityLookup(
        identity_lookup_status=status,
        identity_state=state,
        identity_resolution_version=resolution_version,
        identity_resolved_at=resolved_at,
        identity_entity_display=entity_display,
        identity_wallet_role_display=None,
        identity_operational_tier=operational_tier,
        identity_conflict_set_id=conflict_set_id,
    )
