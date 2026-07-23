"""Read-only audit views over the local evidence ledger."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from crypto_address_identity.candidates import CandidateInput, CandidateService
from crypto_address_identity.storage.sqlite import IdentityDatabase


_SUPPORTED_OFFICIAL_TIERS = frozenset({"A", "B", "C", "D", "E"})


@dataclass(frozen=True)
class CalibrationSeedResult:
    """Result for a reproducible, source-scoped provider calibration intake."""

    independence_group: str
    source_reference: str
    eligible_address_count: int
    evidence_tiers: tuple[str, ...]
    imported_count: int
    dry_run: bool


def seed_official_calibration_candidates(
    database: IdentityDatabase,
    *,
    independence_group: str,
    source_reference: str,
    requested_at: datetime,
    priority: int = 70,
    dry_run: bool = False,
) -> CalibrationSeedResult:
    """Queue valid, current official entity evidence as discovery-only samples.

    This preserves the selected official source group in candidate provenance.
    It intentionally creates neither a claim review nor a consumer-facing
    operational decision.
    """

    group = independence_group.strip()
    reference = source_reference.strip()
    if not group or not reference:
        raise ValueError("independence_group and source_reference are required")
    if priority < 0 or priority > 100:
        raise ValueError("priority must be between 0 and 100")
    if requested_at.tzinfo is None:
        raise ValueError("requested_at must be timezone-aware")
    timestamp = requested_at.astimezone(UTC)

    with database.read_connection() as connection:
        rows = connection.execute(
            """
            SELECT DISTINCT a.normalized_address, ie.evidence_tier
            FROM identity_evidence AS ie
            JOIN address_subject AS a ON a.address_id = ie.address_id
            WHERE ie.source_authority IN ('official', 'regulator')
              AND ie.evidence_status = 'valid'
              AND ie.assertion_type = 'entity_control'
              AND ie.independence_group = ?
              AND (ie.expires_at IS NULL OR ie.expires_at > ?)
            ORDER BY a.normalized_address
            """,
            (group, _format_utc(timestamp)),
        ).fetchall()

    addresses = tuple(sorted({str(row["normalized_address"]) for row in rows}))
    tiers = tuple(sorted({str(row["evidence_tier"]) for row in rows}))
    if dry_run or not rows:
        return CalibrationSeedResult(
            independence_group=group,
            source_reference=reference,
            eligible_address_count=len(addresses),
            evidence_tiers=tiers,
            imported_count=0,
            dry_run=dry_run,
        )

    candidates = [
        CandidateInput(
            chain_key="bitcoin",
            address=address,
            reason="official_evidence",
            priority=priority,
            source_reference=reference,
            requested_at=timestamp,
        )
        for address in addresses
    ]
    result = CandidateService(database).import_candidates(candidates, created_at=timestamp)
    return CalibrationSeedResult(
        independence_group=group,
        source_reference=reference,
        eligible_address_count=len(addresses),
        evidence_tiers=tiers,
        imported_count=result.imported_count,
        dry_run=False,
    )


def build_provider_reliability_panel(
    database: IdentityDatabase,
    *,
    source_reference_prefix: str,
    official_evidence_tiers: tuple[str, ...] = ("A",),
    official_independence_group: str | None = None,
) -> dict[str, Any]:
    """Summarize a bounded provider sample without exposing addresses or raw data.

    An entity match or conflict is counted only where the exact address also
    has independent official entity-control evidence in the explicitly selected
    evidence tier(s) and source group. Unlabelled historical addresses therefore
    contribute coverage metrics, never a precision claim.
    """

    tiers = _validated_tiers(official_evidence_tiers)
    group = official_independence_group.strip() if official_independence_group else None
    if official_independence_group is not None and not group:
        raise ValueError("official_independence_group must not be empty")

    with database.read_connection() as connection:
        candidate_rows = connection.execute(
            """
            SELECT a.address_id, a.normalized_address, MIN(cr.source_reference) AS source_reference
            FROM candidate_request AS cr
            JOIN address_subject AS a ON a.address_id = cr.address_id
            WHERE substr(cr.source_reference, 1, ?) = ?
            GROUP BY a.address_id, a.normalized_address
            ORDER BY a.address_id
            """,
            (len(source_reference_prefix), source_reference_prefix),
        ).fetchall()
        address_ids = {row["address_id"] for row in candidate_rows}
        observations = connection.execute(
            """
            SELECT address_id, outcome, payload_sha256, completed_at
            FROM source_observation
            WHERE source_id = '0xrouter' AND query_profile = 'discovery'
            ORDER BY completed_at DESC, observation_id DESC
            """
        ).fetchall()
        evidence_rows = connection.execute(
            """
            SELECT address_id, assertion_type, candidate_entity_name, candidate_label,
                   candidate_wallet_role, provider_tag_id, source_authority, evidence_tier,
                   evidence_status, independence_group
            FROM identity_evidence
            WHERE evidence_status = 'valid'
            """
        ).fetchall()
        raw_rows = connection.execute(
            "SELECT payload_sha256, retention_status FROM raw_payload_object"
        ).fetchall()

    latest_observation: dict[str, Any] = {}
    for row in observations:
        if row["address_id"] in address_ids and row["address_id"] not in latest_observation:
            latest_observation[row["address_id"]] = row

    provider_entities: dict[str, set[str]] = defaultdict(set)
    provider_primary_labels: dict[str, set[str]] = defaultdict(set)
    provider_tags: dict[str, set[str]] = defaultdict(set)
    provider_roles: dict[str, set[str]] = defaultdict(set)
    official_entities: dict[str, set[str]] = defaultdict(set)
    for row in evidence_rows:
        address_id = row["address_id"]
        if address_id not in address_ids:
            continue
        if row["source_authority"] == "commercial_provider" and row["evidence_tier"] == "C":
            if row["assertion_type"] == "entity_control" and row["candidate_entity_name"]:
                provider_entities[address_id].add(_canonical(row["candidate_entity_name"]))
            if row["assertion_type"] == "address_label" and row["candidate_label"]:
                if row["provider_tag_id"]:
                    provider_tags[address_id].add(str(row["candidate_label"]))
                else:
                    provider_primary_labels[address_id].add(str(row["candidate_label"]))
            if row["candidate_wallet_role"]:
                provider_roles[address_id].add(str(row["candidate_wallet_role"]))
        if (
            row["source_authority"] in {"official", "regulator"}
            and row["evidence_tier"] in tiers
            and (group is None or row["independence_group"] == group)
            and row["assertion_type"] == "entity_control"
            and row["candidate_entity_name"]
        ):
            official_entities[address_id].add(_canonical(row["candidate_entity_name"]))

    raw_status = {row["payload_sha256"]: row["retention_status"] for row in raw_rows}
    outcome_counts: Counter[str] = Counter()
    stratum_counts: dict[str, Counter[str]] = defaultdict(Counter)
    entity_supported = 0
    primary_label_supported = 0
    tag_supported = 0
    label_or_tag_supported = 0
    fully_empty = 0
    role_supported = 0
    raw_referenced = 0
    raw_active = 0
    comparable = 0
    entity_match = 0
    entity_conflict = 0
    comparison_indeterminate = 0

    for row in candidate_rows:
        address_id = row["address_id"]
        stratum = _stratum(row["source_reference"])
        observation = latest_observation.get(address_id)
        outcome = str(observation["outcome"]) if observation else "not_fetched"
        outcome_counts[outcome] += 1
        stratum_counts[stratum]["candidate_addresses"] += 1
        stratum_counts[stratum][f"outcome:{outcome}"] += 1
        if provider_entities[address_id]:
            entity_supported += 1
            stratum_counts[stratum]["entity_supported"] += 1
        if provider_primary_labels[address_id]:
            primary_label_supported += 1
            stratum_counts[stratum]["primary_address_label_supported"] += 1
        if provider_tags[address_id]:
            tag_supported += 1
            stratum_counts[stratum]["tag_supported"] += 1
        if provider_primary_labels[address_id] or provider_tags[address_id]:
            label_or_tag_supported += 1
            stratum_counts[stratum]["address_label_or_tag_supported"] += 1
        if not provider_entities[address_id] and not provider_primary_labels[address_id] and not provider_tags[address_id]:
            fully_empty += 1
            stratum_counts[stratum]["fully_empty_attribution"] += 1
        if provider_roles[address_id]:
            role_supported += 1
            stratum_counts[stratum]["formal_wallet_role_supported"] += 1
        if observation and observation["payload_sha256"]:
            raw_referenced += 1
            if raw_status.get(observation["payload_sha256"]) == "active":
                raw_active += 1
        if official_entities[address_id] and provider_entities[address_id]:
            comparable += 1
            if official_entities[address_id] & provider_entities[address_id]:
                entity_match += 1
            else:
                entity_conflict += 1
        elif official_entities[address_id]:
            comparison_indeterminate += 1

    return {
        "status": "ok",
        "source_reference_prefix": source_reference_prefix,
        "official_evidence_tiers": sorted(tiers),
        "official_independence_group": group,
        "candidate_addresses": len(candidate_rows),
        "provider_outcome_counts": dict(sorted(outcome_counts.items())),
        "entity_name_supported_count": entity_supported,
        "primary_address_label_supported_count": primary_label_supported,
        "tag_supported_count": tag_supported,
        "address_label_or_tag_supported_count": label_or_tag_supported,
        "fully_empty_attribution_count": fully_empty,
        "formal_wallet_role_supported_count": role_supported,
        "raw_payload_referenced_count": raw_referenced,
        "raw_payload_active_metadata_count": raw_active,
        "official_entity_comparable_count": comparable,
        "official_entity_match_count": entity_match,
        "official_entity_conflict_count": entity_conflict,
        "official_entity_indeterminate_count": comparison_indeterminate,
        "strata": {key: dict(sorted(value.items())) for key, value in sorted(stratum_counts.items())},
        "interpretation": {
            "provider_entity_precision_supported": comparable > 0,
            "provider_entity_precision_passed": comparable > 0 and entity_conflict == 0,
            "provider_wallet_role_precision_supported": False,
            "historical_coverage_is_not_entity_ground_truth": True,
            "official_comparison_basis": _comparison_basis(tiers),
            "signature_verified_reference": "A" in tiers,
        },
    }


def _canonical(value: object) -> str:
    return " ".join(str(value).split()).casefold()


def _stratum(source_reference: object) -> str:
    text = str(source_reference)
    return text.rsplit(":", 1)[-1] if ":" in text else "unspecified"


def _validated_tiers(values: tuple[str, ...]) -> frozenset[str]:
    tiers = frozenset(str(value).upper() for value in values)
    if not tiers or not tiers.issubset(_SUPPORTED_OFFICIAL_TIERS):
        raise ValueError("official_evidence_tiers must contain supported evidence tiers")
    return tiers


def _comparison_basis(tiers: frozenset[str]) -> str:
    if tiers == {"A"}:
        return "tier_a_cryptographic_proof"
    if tiers == {"B"}:
        return "tier_b_direct_publication"
    return "explicit_official_evidence_tiers"


def _format_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
