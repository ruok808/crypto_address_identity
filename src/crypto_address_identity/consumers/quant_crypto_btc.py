"""Read-only BTC enrichment contract for future quant_crypto integration."""

from __future__ import annotations

from collections import Counter
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
    identity_resolution_policy: str


@dataclass(frozen=True)
class ReplayResult:
    events: tuple[dict[str, Any], ...]
    changed_business_fields: int


@dataclass(frozen=True)
class ReplayImpact:
    """Read-only impact accounting for a prospective consumer integration.

    The adapter deliberately cannot alter dispatch or suppression fields. The
    remaining counterfactual limit is reported explicitly: historic outbox
    records retain an output address but not the input-side context needed to
    recompute ownership/suppression decisions.
    """

    events: int
    changed_business_fields: int
    mail_action_changes: int
    suppression_action_changes: int
    internal_candidate_events: int
    internal_candidate_identity_coverage: int
    internal_candidate_provider_default_events: int
    internal_candidate_local_override_events: int
    internal_candidate_conflict_first_events: int
    internal_candidate_missing_input_context: int
    lookup_status_counts: dict[str, int]
    resolution_policy_counts: dict[str, int]


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
            return _lookup("snapshot_invalid", "unattributed", None, None, None, None, "none", None)
        try:
            subject = normalize_bitcoin_address(address)
        except BitcoinAddressError:
            return _lookup("unsupported", "unsupported", None, None, None, None, "none", None)
        record = self._snapshot.lookup("bitcoin", subject.normalized_address, assertion_type)
        if record is None:
            return _lookup("not_found", "unattributed", None, None, None, None, "none", None)
        role_record = self._snapshot.lookup("bitcoin", subject.normalized_address, "wallet_role")
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
            record.get("resolved_entity_display") or record.get("accepted_entity"),
            _resolved_role_display(role_record),
            record.get("operational_tier", "none"),
            record.get("conflict_set_id"),
            record.get("resolution_policy", "legacy_conservative"),
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
        lookup = (
            enricher.lookup(address)
            if address
            else _lookup("not_found", "unattributed", None, None, None, None, "none", None)
        )
        enriched.update(asdict(lookup))
        changed_business_fields += sum(
            event.get(field) != enriched.get(field) for field in protected_fields
        )
        output.append(enriched)
    return ReplayResult(tuple(output), changed_business_fields)


def replay_impact(
    events: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...], enricher: IdentityEnricher
) -> ReplayImpact:
    """Measure coverage without changing an alert, email, or suppression action."""

    replay = replay_events(events, enricher)
    mail_fields = ("alert_decision", "notification_action")
    suppression_fields = ("status", "suppression_rule", "would_suppress")
    lookup_counts: Counter[str] = Counter()
    policy_counts: Counter[str] = Counter()
    internal_candidate_events = 0
    internal_candidate_identity_coverage = 0
    internal_candidate_provider_default_events = 0
    internal_candidate_local_override_events = 0
    internal_candidate_conflict_first_events = 0
    internal_candidate_missing_input_context = 0
    mail_action_changes = 0
    suppression_action_changes = 0
    weighted_events = 0

    for source, enriched in zip(events, replay.events, strict=True):
        weight = _replay_weight(source)
        weighted_events += weight
        lookup_status = str(enriched["identity_lookup_status"])
        policy = str(enriched["identity_resolution_policy"])
        lookup_counts[lookup_status] += weight
        policy_counts[policy] += weight
        mail_action_changes += weight * sum(
            source.get(field) != enriched.get(field) for field in mail_fields
        )
        suppression_action_changes += sum(
            source.get(field) != enriched.get(field) for field in suppression_fields
        ) * weight
        if source.get("semantic_decision") != "internal_candidate":
            continue
        internal_candidate_events += weight
        if lookup_status == "found":
            internal_candidate_identity_coverage += weight
        if policy == "provider_default":
            internal_candidate_provider_default_events += weight
        elif policy == "local_override":
            internal_candidate_local_override_events += weight
        elif policy == "conflict_first":
            internal_candidate_conflict_first_events += weight
        if not isinstance(source.get("input_address"), str):
            internal_candidate_missing_input_context += weight

    return ReplayImpact(
        events=weighted_events,
        changed_business_fields=replay.changed_business_fields,
        mail_action_changes=mail_action_changes,
        suppression_action_changes=suppression_action_changes,
        internal_candidate_events=internal_candidate_events,
        internal_candidate_identity_coverage=internal_candidate_identity_coverage,
        internal_candidate_provider_default_events=internal_candidate_provider_default_events,
        internal_candidate_local_override_events=internal_candidate_local_override_events,
        internal_candidate_conflict_first_events=internal_candidate_conflict_first_events,
        internal_candidate_missing_input_context=internal_candidate_missing_input_context,
        lookup_status_counts=dict(sorted(lookup_counts.items())),
        resolution_policy_counts=dict(sorted(policy_counts.items())),
    )


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
    wallet_role_display: str | None,
    operational_tier: str,
    conflict_set_id: str | None,
    resolution_policy: str = "legacy_conservative",
) -> IdentityLookup:
    return IdentityLookup(
        identity_lookup_status=status,
        identity_state=state,
        identity_resolution_version=resolution_version,
        identity_resolved_at=resolved_at,
        identity_entity_display=entity_display,
        identity_wallet_role_display=wallet_role_display,
        identity_operational_tier=operational_tier,
        identity_conflict_set_id=conflict_set_id,
        identity_resolution_policy=resolution_policy,
    )


def _resolved_role_display(record: Mapping[str, Any] | None) -> str | None:
    if record is None or record.get("operational_tier") != "lookup_usable":
        return None
    value = record.get("resolved_asserted_value")
    return value if isinstance(value, str) else None


def _replay_weight(source: Mapping[str, Any]) -> int:
    value = source.get("replay_weight", 1)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        return 1
    return value
