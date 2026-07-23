"""Conservative claim construction, review records, and resolver revisions."""

from __future__ import annotations

import hashlib
import json
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from crypto_address_identity.chains.bitcoin import normalize_bitcoin_address
from crypto_address_identity.evidence import EvidenceInput
from crypto_address_identity.storage.sqlite import IdentityDatabase


@dataclass(frozen=True)
class RebuildResult:
    resolution_count: int
    resolution_ids: tuple[str, ...]


@dataclass(frozen=True)
class ResolutionView:
    chain_key: str
    normalized_address: str
    assertion_type: str
    state: str
    operational_tier: str
    resolution_policy: str
    accepted_entity: str | None
    accepted_entity_display: str | None
    conflict_set_id: str | None
    resolution_version: str | None
    resolved_at: str | None
    freshness_status: str


def canonical_asserted_value(record: EvidenceInput) -> str:
    """Stable assertion value used for claim grouping and review decisions."""

    return _canonical_asserted_value(
        assertion_type=record.assertion_type,
        candidate_entity_id=record.candidate_entity_id,
        candidate_entity_name=record.candidate_entity_name,
        candidate_label=record.candidate_label,
        candidate_wallet_role=record.candidate_wallet_role,
    )


class ResolverService:
    """Materializes policy-explicit point-in-time claim and resolution revisions."""

    def __init__(self, database: IdentityDatabase) -> None:
        self.database = database

    def record_review(
        self,
        *,
        chain_key: str,
        address: str,
        assertion_type: str,
        asserted_value: str,
        reviewer_ref: str,
        decision: Literal["accept", "reject"],
        reviewed_at: str,
    ) -> str:
        if chain_key != "bitcoin":
            raise ValueError("BTC-first resolver accepts only bitcoin reviews")
        subject = normalize_bitcoin_address(address)
        reviewed = _parse_utc(reviewed_at)
        fingerprint = _hash_json(
            {
                "address_id": subject.address_id,
                "assertion_type": assertion_type,
                "asserted_value": asserted_value,
                "decision": decision,
                "reviewer_ref": reviewer_ref,
                "reviewed_at": reviewed,
            }
        )
        with self.database.write_transaction() as connection:
            subject_row = connection.execute(
                "SELECT address_id FROM address_subject WHERE address_id = ?", (subject.address_id,)
            ).fetchone()
            if subject_row is None:
                raise ValueError("Cannot review an address with no imported evidence")
            existing = connection.execute(
                "SELECT review_id FROM claim_review WHERE review_fingerprint = ?", (fingerprint,)
            ).fetchone()
            if existing:
                return existing["review_id"]
            review_id = str(uuid.uuid4())
            connection.execute(
                """
                INSERT INTO claim_review (
                    review_id, review_fingerprint, address_id, assertion_type,
                    asserted_value, decision, reviewer_ref, reviewed_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    review_id,
                    fingerprint,
                    subject.address_id,
                    assertion_type,
                    asserted_value,
                    decision,
                    reviewer_ref,
                    reviewed,
                    reviewed,
                ),
            )
        return review_id

    def record_local_override(
        self,
        *,
        chain_key: str,
        address: str,
        assertion_type: str,
        asserted_value: str,
        decision: Literal["select", "reject"],
        reviewer_ref: str,
        reason_ref: str,
        reviewed_at: str,
    ) -> str:
        """Append a local correction without deleting provider or official evidence.

        A local correction can only select or reject a value that is already
        supported by evidence for the subject. New assertions must enter through
        the evidence ledger first, preserving their independent provenance.
        """

        if chain_key != "bitcoin":
            raise ValueError("BTC-first resolver accepts only bitcoin overrides")
        if decision not in {"select", "reject"}:
            raise ValueError("local override decision must be select or reject")
        value = asserted_value.strip().casefold()
        if not value or not reviewer_ref.strip() or not reason_ref.strip():
            raise ValueError("local override requires value, reviewer_ref, and reason_ref")
        subject = normalize_bitcoin_address(address)
        reviewed = _parse_utc(reviewed_at)
        fingerprint = _hash_json(
            {
                "address_id": subject.address_id,
                "assertion_type": assertion_type,
                "asserted_value": value,
                "decision": decision,
                "reviewer_ref": reviewer_ref.strip(),
                "reason_ref": reason_ref.strip(),
                "reviewed_at": reviewed,
            }
        )
        with self.database.write_transaction() as connection:
            evidence_rows = connection.execute(
                """
                SELECT assertion_type, candidate_entity_id, candidate_entity_name, candidate_label,
                       candidate_wallet_role
                FROM identity_evidence
                WHERE address_id = ? AND assertion_type = ? AND observed_at <= ?
                """,
                (subject.address_id, assertion_type, reviewed),
            ).fetchall()
            if not any(_canonical_asserted_value_from_row(row) == value for row in evidence_rows):
                raise ValueError("local override value requires existing evidence")
            existing = connection.execute(
                "SELECT override_id FROM resolver_local_override WHERE override_fingerprint = ?",
                (fingerprint,),
            ).fetchone()
            if existing:
                return existing["override_id"]
            override_id = str(uuid.uuid4())
            connection.execute(
                """
                INSERT INTO resolver_local_override (
                    override_id, override_fingerprint, address_id, assertion_type,
                    asserted_value, decision, reviewer_ref, reason_ref, reviewed_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    override_id,
                    fingerprint,
                    subject.address_id,
                    assertion_type,
                    value,
                    decision,
                    reviewer_ref.strip(),
                    reason_ref.strip(),
                    reviewed,
                    reviewed,
                ),
            )
        return override_id

    def rebuild(self, *, as_of: str) -> RebuildResult:
        as_of_utc = _parse_utc(as_of)
        version = f"btc_resolver_v2:{as_of_utc}"
        resolution_ids: list[str] = []
        with self.database.write_transaction() as connection:
            subjects = connection.execute(
                """
                SELECT DISTINCT address_id, assertion_type
                FROM identity_evidence
                WHERE observed_at <= ?
                ORDER BY address_id, assertion_type
                """,
                (as_of_utc,),
            ).fetchall()
            for subject in subjects:
                resolution_id = self._rebuild_subject(
                    connection,
                    address_id=subject["address_id"],
                    assertion_type=subject["assertion_type"],
                    as_of=as_of_utc,
                    resolution_version=version,
                )
                resolution_ids.append(resolution_id)
        return RebuildResult(len(resolution_ids), tuple(resolution_ids))

    def show(self, chain_key: str, address: str, *, assertion_type: str = "entity_control") -> ResolutionView:
        if chain_key != "bitcoin":
            return ResolutionView(
                chain_key=chain_key,
                normalized_address=address,
                assertion_type=assertion_type,
                state="unsupported",
                operational_tier="none",
                resolution_policy="unsupported",
                accepted_entity=None,
                accepted_entity_display=None,
                conflict_set_id=None,
                resolution_version=None,
                resolved_at=None,
                freshness_status="unsupported",
            )
        subject = normalize_bitcoin_address(address)
        with self.database.read_connection() as connection:
            row = connection.execute(
                """
                SELECT r.*, c.entity_id
                FROM identity_resolution AS r
                LEFT JOIN identity_claim AS c ON c.claim_id = r.primary_claim_id
                WHERE r.address_id = ? AND r.assertion_type = ?
                ORDER BY r.resolved_at DESC, r.resolution_id DESC
                LIMIT 1
                """,
                (subject.address_id, assertion_type),
            ).fetchone()
        if row is None:
            return ResolutionView(
                chain_key="bitcoin",
                normalized_address=subject.normalized_address,
                assertion_type=assertion_type,
                state="unattributed",
                operational_tier="none",
                resolution_policy="unattributed",
                accepted_entity=None,
                accepted_entity_display=None,
                conflict_set_id=None,
                resolution_version=None,
                resolved_at=None,
                freshness_status="unknown",
            )
        accepted_entity = row["entity_id"] if row["operational_tier"] == "lookup_usable" else None
        accepted_entity_display = (
            row["primary_entity_display"] if row["operational_tier"] == "lookup_usable" else None
        )
        return ResolutionView(
            chain_key="bitcoin",
            normalized_address=subject.normalized_address,
            assertion_type=assertion_type,
            state=row["state"],
            operational_tier=row["operational_tier"],
            resolution_policy=row["resolution_policy"],
            accepted_entity=accepted_entity,
            accepted_entity_display=accepted_entity_display,
            conflict_set_id=row["conflict_set_id"],
            resolution_version=row["resolution_version"],
            resolved_at=row["resolved_at"],
            freshness_status=row["freshness_status"],
        )

    def _rebuild_subject(
        self,
        connection,
        *,
        address_id: str,
        assertion_type: str,
        as_of: str,
        resolution_version: str,
    ) -> str:
        evidence_rows = connection.execute(
            """
            SELECT * FROM identity_evidence
            WHERE address_id = ? AND assertion_type = ? AND observed_at <= ?
            ORDER BY observed_at, evidence_id
            """,
            (address_id, assertion_type, as_of),
        ).fetchall()
        active_rows = [row for row in evidence_rows if _is_active(row, as_of)]
        stale_rows = [row for row in evidence_rows if not _is_active(row, as_of)]
        groups: dict[str, list] = defaultdict(list)
        for row in active_rows:
            if not _claim_eligible(row):
                continue
            groups[_canonical_asserted_value_from_row(row)].append(row)

        override_decisions = self._active_override_decisions(
            connection,
            address_id=address_id,
            assertion_type=assertion_type,
            as_of=as_of,
        )
        materialized_claims = [
            self._materialize_claim(
                connection,
                address_id=address_id,
                assertion_type=assertion_type,
                asserted_value=value,
                evidence_rows=rows,
                local_override_decision=override_decisions.get(value),
                as_of=as_of,
            )
            for value, rows in sorted(groups.items())
        ]
        active_claims = [claim for claim in materialized_claims if claim["claim_status"] != "rejected"]
        conflict_set_id = self._materialize_conflict(
            connection,
            address_id=address_id,
            assertion_type=assertion_type,
            claim_rows=active_claims,
            as_of=as_of,
        )

        selected_values = {
            value for value, decision in override_decisions.items() if decision == "select"
        }
        selected_claims = [
            claim for claim in active_claims if claim["asserted_value"] in selected_values
        ]

        if not active_claims:
            state = "stale" if stale_rows else "unattributed"
            operational_tier = "none"
            primary_claim_id = None
            resolution_policy = "local_override" if override_decisions else "unattributed"
            primary_entity_display = None
            freshness_status = "stale" if stale_rows else (
                "locally_rejected" if override_decisions else "unknown"
            )
        elif len(selected_claims) == 1:
            claim = selected_claims[0]
            state = "resolved"
            operational_tier = "lookup_usable"
            resolution_policy = "local_override"
            primary_claim_id = claim["claim_id"]
            primary_entity_display = _entity_display_for_assertion(
                assertion_type, groups[claim["asserted_value"]]
            )
            freshness_status = "fresh"
        elif conflict_set_id is not None or len(selected_claims) > 1:
            state = "ambiguous"
            operational_tier = "lookup_only"
            resolution_policy = "conflict_first"
            primary_claim_id = None
            primary_entity_display = None
            freshness_status = "conflicted"
        else:
            claim = active_claims[0]
            primary_claim_id = claim["claim_id"]
            primary_entity_display = _entity_display_for_assertion(
                assertion_type, groups[claim["asserted_value"]]
            )
            if claim["claim_status"] == "accepted":
                state = "resolved"
                operational_tier = "lookup_usable"
                resolution_policy = "reviewed_evidence"
            elif _is_provider_default(assertion_type, groups[claim["asserted_value"]]):
                state = "resolved"
                operational_tier = "lookup_usable"
                resolution_policy = "provider_default"
            else:
                state = "resolved"
                operational_tier = "discovery_only"
                resolution_policy = "unreviewed_evidence"
            freshness_status = "fresh"

        candidate_ids = [claim["claim_id"] for claim in materialized_claims]
        fingerprint = _hash_json(
            {
                "address_id": address_id,
                "assertion_type": assertion_type,
                "state": state,
                "operational_tier": operational_tier,
                "resolution_policy": resolution_policy,
                "primary_claim_id": primary_claim_id,
                "primary_entity_display": primary_entity_display,
                "candidate_claim_ids": candidate_ids,
                "conflict_set_id": conflict_set_id,
                "as_of": as_of,
                "resolution_version": resolution_version,
                "freshness_status": freshness_status,
            }
        )
        existing = connection.execute(
            "SELECT resolution_id FROM identity_resolution WHERE resolution_fingerprint = ?",
            (fingerprint,),
        ).fetchone()
        if existing:
            return existing["resolution_id"]

        resolution_id = str(uuid.uuid4())
        connection.execute(
            """
            INSERT INTO identity_resolution (
                resolution_id, resolution_fingerprint, address_id, assertion_type,
                state, primary_claim_id, candidate_claim_ids_json, operational_tier,
                conflict_set_id, resolved_at, resolution_version, freshness_status,
                resolution_policy, primary_entity_display
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                resolution_id,
                fingerprint,
                address_id,
                assertion_type,
                state,
                primary_claim_id,
                json.dumps(candidate_ids, separators=(",", ":")),
                operational_tier,
                conflict_set_id,
                as_of,
                resolution_version,
                freshness_status,
                resolution_policy,
                primary_entity_display,
            ),
        )
        return resolution_id

    def _materialize_claim(
        self,
        connection,
        *,
        address_id: str,
        assertion_type: str,
        asserted_value: str,
        evidence_rows: list,
        local_override_decision: str | None,
        as_of: str,
    ):
        review = connection.execute(
            """
            SELECT decision, reviewer_ref, reviewed_at
            FROM claim_review
            WHERE address_id = ? AND assertion_type = ? AND asserted_value = ?
              AND reviewed_at <= ?
            ORDER BY reviewed_at DESC, review_id DESC
            LIMIT 1
            """,
            (address_id, assertion_type, asserted_value, as_of),
        ).fetchone()
        tiers = {row["evidence_tier"] for row in evidence_rows}
        if local_override_decision == "reject":
            claim_status = "rejected"
        elif review and review["decision"] == "accept" and tiers & {"A", "B"}:
            claim_status = "accepted"
        elif review and review["decision"] == "reject":
            claim_status = "rejected"
        else:
            claim_status = "unreviewed_external"
        evidence_fingerprints = sorted(row["evidence_fingerprint"] for row in evidence_rows)
        fingerprint = _hash_json(
            {
                "address_id": address_id,
                "assertion_type": assertion_type,
                "asserted_value": asserted_value,
                "evidence_fingerprints": evidence_fingerprints,
                "claim_status": claim_status,
                "local_override_decision": local_override_decision,
                "reviewed_at": review["reviewed_at"] if review else None,
            }
        )
        existing = connection.execute(
            "SELECT * FROM identity_claim WHERE claim_fingerprint = ?", (fingerprint,)
        ).fetchone()
        if existing:
            return existing

        previous = connection.execute(
            """
            SELECT claim_id FROM identity_claim
            WHERE address_id = ? AND assertion_type = ? AND asserted_value = ?
            ORDER BY created_at DESC, claim_id DESC LIMIT 1
            """,
            (address_id, assertion_type, asserted_value),
        ).fetchone()
        claim_id = str(uuid.uuid4())
        entity_id = _entity_id_for_assertion(assertion_type, evidence_rows)
        entity_name = _entity_name_for_assertion(assertion_type, evidence_rows)
        strength = min((row["evidence_tier"] for row in evidence_rows), key=_tier_rank)
        independent_count = len({row["independence_group"] for row in evidence_rows})
        connection.execute(
            """
            INSERT INTO identity_claim (
                claim_id, claim_fingerprint, address_id, assertion_type,
                asserted_value, entity_id, claim_status, evidence_strength,
                corroboration_count, independence_count, effective_from,
                effective_to, reviewed_at, reviewer_ref, supersedes_claim_id,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                claim_id,
                fingerprint,
                address_id,
                assertion_type,
                asserted_value,
                entity_id,
                claim_status,
                strength,
                len(evidence_rows),
                independent_count,
                min((row["effective_from"] for row in evidence_rows if row["effective_from"]), default=None),
                max((row["effective_to"] for row in evidence_rows if row["effective_to"]), default=None),
                review["reviewed_at"] if review else None,
                review["reviewer_ref"] if review else None,
                previous["claim_id"] if previous else None,
                as_of,
            ),
        )
        return connection.execute("SELECT * FROM identity_claim WHERE claim_id = ?", (claim_id,)).fetchone()

    @staticmethod
    def _active_override_decisions(
        connection,
        *,
        address_id: str,
        assertion_type: str,
        as_of: str,
    ) -> dict[str, str]:
        rows = connection.execute(
            """
            SELECT asserted_value, decision
            FROM resolver_local_override
            WHERE address_id = ? AND assertion_type = ? AND reviewed_at <= ?
            ORDER BY reviewed_at DESC, override_id DESC
            """,
            (address_id, assertion_type, as_of),
        ).fetchall()
        decisions: dict[str, str] = {}
        for row in rows:
            decisions.setdefault(row["asserted_value"], row["decision"])
        return decisions

    def _materialize_conflict(
        self,
        connection,
        *,
        address_id: str,
        assertion_type: str,
        claim_rows: list,
        as_of: str,
    ) -> str | None:
        existing = connection.execute(
            """
            SELECT conflict_set_id FROM conflict_set
            WHERE address_id = ? AND assertion_type = ? AND status = 'active'
            """,
            (address_id, assertion_type),
        ).fetchone()
        if len(claim_rows) < 2:
            if existing:
                connection.execute(
                    "UPDATE conflict_set SET status = 'resolved', resolved_at = ? WHERE conflict_set_id = ?",
                    (as_of, existing["conflict_set_id"]),
                )
            return None
        conflict_set_id = existing["conflict_set_id"] if existing else str(uuid.uuid4())
        if not existing:
            connection.execute(
                """
                INSERT INTO conflict_set (
                    conflict_set_id, address_id, assertion_type, created_at, status
                ) VALUES (?, ?, ?, ?, 'active')
                """,
                (conflict_set_id, address_id, assertion_type, as_of),
            )
        for claim in claim_rows:
            connection.execute(
                """
                INSERT OR IGNORE INTO conflict_member (conflict_set_id, claim_id, added_at)
                VALUES (?, ?, ?)
                """,
                (conflict_set_id, claim["claim_id"], as_of),
            )
        return conflict_set_id


def _canonical_asserted_value(
    *,
    assertion_type: str,
    candidate_entity_id: str | None,
    candidate_entity_name: str | None,
    candidate_label: str | None,
    candidate_wallet_role: str | None,
) -> str:
    if assertion_type == "entity_control":
        # Provider IDs live in source namespaces (for example `arkham:*`) and
        # cannot by themselves define a distinct real-world entity. Exact
        # normalized names let independent evidence corroborate one claim while
        # retaining all provider IDs on the underlying immutable evidence rows.
        raw = candidate_entity_name or candidate_entity_id
    elif assertion_type == "wallet_role":
        raw = candidate_wallet_role
    else:
        raw = candidate_label
    if raw is None:
        raise ValueError("Evidence has no canonical asserted value")
    return raw.strip().casefold()


def _canonical_asserted_value_from_row(row) -> str:
    return _canonical_asserted_value(
        assertion_type=row["assertion_type"],
        candidate_entity_id=row["candidate_entity_id"],
        candidate_entity_name=row["candidate_entity_name"],
        candidate_label=row["candidate_label"],
        candidate_wallet_role=row["candidate_wallet_role"],
    )


def _is_active(row, as_of: str) -> bool:
    if row["evidence_status"] != "valid":
        return False
    if row["effective_from"] and row["effective_from"] > as_of:
        return False
    if row["effective_to"] and row["effective_to"] < as_of:
        return False
    if row["expires_at"] and row["expires_at"] < as_of:
        return False
    return True


def _claim_eligible(row) -> bool:
    """Exclude multi-valued provider tags from a single-value label claim.

    `arkhamLabel` has no provider tag id and remains a primary address label.
    `populatedTags` carry a provider tag id and are retained as evidence only:
    they can co-exist and must not manufacture a resolver conflict.
    """

    return not (
        row["assertion_type"] == "address_label"
        and row["source_authority"] == "commercial_provider"
        and row["provider_tag_id"]
    )


def _tier_rank(tier: str) -> int:
    return {"A": 0, "B": 1, "C": 2, "D": 3, "E": 4}[tier]


def _entity_id_for_assertion(assertion_type: str, evidence_rows: list) -> str | None:
    if assertion_type != "entity_control":
        return None
    return next(
        (
            row["candidate_entity_id"]
            for row in _preferred_evidence_rows(evidence_rows)
            if row["candidate_entity_id"]
        ),
        None,
    )


def _entity_name_for_assertion(assertion_type: str, evidence_rows: list) -> str | None:
    if assertion_type != "entity_control":
        return None
    return next(
        (
            row["candidate_entity_name"]
            for row in _preferred_evidence_rows(evidence_rows)
            if row["candidate_entity_name"]
        ),
        None,
    )


def _entity_display_for_assertion(assertion_type: str, evidence_rows: list) -> str | None:
    if assertion_type != "entity_control":
        return None
    return _entity_name_for_assertion(assertion_type, evidence_rows)


def _is_provider_default(assertion_type: str, evidence_rows: list) -> bool:
    """Allow a single commercial entity assertion, but not generic provider tags.

    Address labels and tags remain discovery evidence: their semantics are too
    loose to stand in for an entity-control conclusion. A mixture with an
    unreviewed official/local observation is also not a provider-default case;
    that requires a review or an explicit local override.
    """

    return assertion_type == "entity_control" and bool(evidence_rows) and all(
        row["source_authority"] == "commercial_provider" and row["evidence_tier"] == "C"
        for row in evidence_rows
    )


def _preferred_evidence_rows(evidence_rows: list) -> list:
    authority_rank = {
        "local_inference": 0,
        "official": 1,
        "regulator": 1,
        "commercial_provider": 2,
        "public_explorer": 3,
    }
    return sorted(
        evidence_rows,
        key=lambda row: (
            authority_rank.get(row["source_authority"], 9),
            _tier_rank(row["evidence_tier"]),
            row["observed_at"],
            row["evidence_id"],
        ),
    )


def _parse_utc(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timestamp must be timezone-aware")
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _hash_json(value: dict[str, object]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
