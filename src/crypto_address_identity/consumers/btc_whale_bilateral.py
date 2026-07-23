"""Read-only bilateral BTC whale replay for suppression-safety analysis."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any, Mapping

from crypto_address_identity.consumers.quant_crypto_btc import IdentityEnricher, IdentityLookup


_INDEPENDENT_POLICIES = frozenset({"reviewed_evidence", "local_override"})


@dataclass(frozen=True)
class BilateralReplayImpact:
    """Aggregate-only assessment of chain structure plus resolver evidence.

    This object intentionally carries no address, transaction, or event
    identifier. It is an observation artifact, not an alert-action request.
    """

    events: int
    internal_candidate_events: int
    malformed_events: int
    events_with_missing_input_address: int
    output_identity_found_events: int
    input_addresses_observed: int
    input_identity_found_addresses: int
    conflict_first_side_events: int
    same_entity_bilateral_events: int
    same_entity_provider_default_events: int
    same_entity_independent_evidence_events: int
    source_strong_condition_events: int
    provider_default_suppression_candidates: int
    independent_evidence_suppression_candidates: int
    live_action_changes: int
    provider_default_candidate_status_counts: dict[str, int]
    independent_candidate_status_counts: dict[str, int]
    output_policy_counts: dict[str, int]
    input_policy_counts: dict[str, int]


def replay_bilateral_whale_events(
    events: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...],
    enricher: IdentityEnricher,
) -> BilateralReplayImpact:
    """Rebuild bilateral identity context without changing live alert behavior.

    Input rows are sanitized raw-transaction projections. They must contain an
    output address and a list of input addresses; optional semantic scores and
    quality tags are used only to model the already-approved future rule shape.
    """

    malformed_events = 0
    internal_candidate_events = 0
    events_with_missing_input_address = 0
    output_identity_found_events = 0
    input_addresses_observed = 0
    input_identity_found_addresses = 0
    conflict_first_side_events = 0
    same_entity_bilateral_events = 0
    same_entity_provider_default_events = 0
    same_entity_independent_evidence_events = 0
    source_strong_condition_events = 0
    provider_default_suppression_candidates = 0
    independent_evidence_suppression_candidates = 0
    output_policy_counts: Counter[str] = Counter()
    input_policy_counts: Counter[str] = Counter()
    provider_default_candidate_status_counts: Counter[str] = Counter()
    independent_candidate_status_counts: Counter[str] = Counter()

    for event in events:
        output_address = event.get("output_address")
        input_addresses = event.get("input_addresses")
        if not isinstance(output_address, str) or not isinstance(input_addresses, list):
            malformed_events += 1
            continue

        if event.get("semantic_decision") == "internal_candidate":
            internal_candidate_events += 1
        output_lookup = enricher.lookup(output_address)
        output_policy_counts[output_lookup.identity_resolution_policy] += 1
        if output_lookup.identity_lookup_status == "found":
            output_identity_found_events += 1

        valid_inputs = [address for address in input_addresses if isinstance(address, str)]
        missing_input_count = _positive_int(event.get("missing_input_address_count"))
        input_context_complete = bool(valid_inputs) and not (
            missing_input_count or len(valid_inputs) != len(input_addresses)
        )
        if not input_context_complete:
            events_with_missing_input_address += 1
        input_addresses_observed += len(valid_inputs)
        input_lookups = [enricher.lookup(address) for address in dict.fromkeys(valid_inputs)]
        for lookup in input_lookups:
            input_policy_counts[lookup.identity_resolution_policy] += 1
            if lookup.identity_lookup_status == "found":
                input_identity_found_addresses += 1

        match_lookups = _same_entity_input_lookups(output_lookup, input_lookups)
        same_entity = bool(match_lookups)
        if same_entity:
            same_entity_bilateral_events += 1
        provider_default_match = any(
            _has_provider_default(output_lookup, input_lookup) for input_lookup in match_lookups
        )
        if provider_default_match:
            same_entity_provider_default_events += 1
        independent_match = any(
            _has_independent_evidence(output_lookup, input_lookup) for input_lookup in match_lookups
        )
        if independent_match:
            same_entity_independent_evidence_events += 1
        has_conflict_first = _has_conflict_first(output_lookup, input_lookups)
        if has_conflict_first:
            conflict_first_side_events += 1

        source_strong_condition = _has_source_strong_condition(event)
        if source_strong_condition:
            source_strong_condition_events += 1
        if (
            source_strong_condition
            and input_context_complete
            and not has_conflict_first
            and same_entity
            and provider_default_match
        ):
            provider_default_suppression_candidates += 1
            provider_default_candidate_status_counts[_source_status(event)] += 1
        if (
            source_strong_condition
            and input_context_complete
            and not has_conflict_first
            and same_entity
            and independent_match
        ):
            independent_evidence_suppression_candidates += 1
            independent_candidate_status_counts[_source_status(event)] += 1

    return BilateralReplayImpact(
        events=len(events),
        internal_candidate_events=internal_candidate_events,
        malformed_events=malformed_events,
        events_with_missing_input_address=events_with_missing_input_address,
        output_identity_found_events=output_identity_found_events,
        input_addresses_observed=input_addresses_observed,
        input_identity_found_addresses=input_identity_found_addresses,
        conflict_first_side_events=conflict_first_side_events,
        same_entity_bilateral_events=same_entity_bilateral_events,
        same_entity_provider_default_events=same_entity_provider_default_events,
        same_entity_independent_evidence_events=same_entity_independent_evidence_events,
        source_strong_condition_events=source_strong_condition_events,
        provider_default_suppression_candidates=provider_default_suppression_candidates,
        independent_evidence_suppression_candidates=independent_evidence_suppression_candidates,
        live_action_changes=0,
        provider_default_candidate_status_counts=dict(
            sorted(provider_default_candidate_status_counts.items())
        ),
        independent_candidate_status_counts=dict(sorted(independent_candidate_status_counts.items())),
        output_policy_counts=dict(sorted(output_policy_counts.items())),
        input_policy_counts=dict(sorted(input_policy_counts.items())),
    )


def _same_entity_input_lookups(
    output_lookup: IdentityLookup, input_lookups: list[IdentityLookup]
) -> list[IdentityLookup]:
    output_entity = _entity_key(output_lookup)
    if output_entity is None:
        return []
    return [lookup for lookup in input_lookups if _entity_key(lookup) == output_entity]


def _entity_key(lookup: IdentityLookup) -> str | None:
    if lookup.identity_lookup_status != "found" or not lookup.identity_entity_display:
        return None
    return lookup.identity_entity_display.strip().casefold()


def _has_provider_default(output_lookup: IdentityLookup, input_lookup: IdentityLookup) -> bool:
    return "provider_default" in {
        output_lookup.identity_resolution_policy,
        input_lookup.identity_resolution_policy,
    }


def _has_independent_evidence(output_lookup: IdentityLookup, input_lookup: IdentityLookup) -> bool:
    return {
        output_lookup.identity_resolution_policy,
        input_lookup.identity_resolution_policy,
    } <= _INDEPENDENT_POLICIES


def _has_conflict_first(output_lookup: IdentityLookup, input_lookups: list[IdentityLookup]) -> bool:
    return any(
        lookup.identity_resolution_policy == "conflict_first"
        for lookup in [output_lookup, *input_lookups]
    )


def _has_source_strong_condition(event: Mapping[str, Any]) -> bool:
    """Reproduce the existing semantic/quality precondition without changing it."""
    tags = event.get("quality_tags")
    return (
        event.get("semantic_decision") == "internal_candidate"
        and isinstance(tags, list)
        and "self_churn_possible" in tags
        and _positive_int(event.get("internal_transfer_score")) >= 85
        and _nonnegative_int(event.get("ownership_transfer_score")) < 60
    )


def _positive_int(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else 0


def _nonnegative_int(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0


def _source_status(event: Mapping[str, Any]) -> str:
    value = event.get("status")
    return value if isinstance(value, str) and value else "missing"
