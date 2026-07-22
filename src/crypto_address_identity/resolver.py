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
    accepted_entity: str | None
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
    """Materializes conservative point-in-time claim and resolution revisions."""

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

    def rebuild(self, *, as_of: str) -> RebuildResult:
        as_of_utc = _parse_utc(as_of)
        version = f"btc_resolver_v1:{as_of_utc}"
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
                accepted_entity=None,
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
                accepted_entity=None,
                conflict_set_id=None,
                resolution_version=None,
                resolved_at=None,
                freshness_status="unknown",
            )
        accepted_entity = row["entity_id"] if row["operational_tier"] == "lookup_usable" else None
        return ResolutionView(
            chain_key="bitcoin",
            normalized_address=subject.normalized_address,
            assertion_type=assertion_type,
            state=row["state"],
            operational_tier=row["operational_tier"],
            accepted_entity=accepted_entity,
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
            groups[_canonical_asserted_value_from_row(row)].append(row)

        active_claims = [
            self._materialize_claim(
                connection,
                address_id=address_id,
                assertion_type=assertion_type,
                asserted_value=value,
                evidence_rows=rows,
                as_of=as_of,
            )
            for value, rows in sorted(groups.items())
        ]
        conflict_set_id = self._materialize_conflict(
            connection,
            address_id=address_id,
            assertion_type=assertion_type,
            claim_rows=active_claims,
            as_of=as_of,
        )

        if not active_claims:
            state = "stale" if stale_rows else "unattributed"
            operational_tier = "none"
            primary_claim_id = None
            freshness_status = "stale" if stale_rows else "unknown"
        elif conflict_set_id is not None:
            state = "ambiguous"
            operational_tier = "lookup_only"
            primary_claim_id = None
            freshness_status = "conflicted"
        else:
            claim = active_claims[0]
            primary_claim_id = claim["claim_id"]
            if claim["claim_status"] == "accepted":
                state = "resolved"
                operational_tier = "lookup_usable"
            elif claim["claim_status"] == "rejected":
                state = "unattributed"
                operational_tier = "none"
                primary_claim_id = None
            else:
                state = "resolved"
                operational_tier = "discovery_only"
            freshness_status = "fresh"

        candidate_ids = [claim["claim_id"] for claim in active_claims]
        fingerprint = _hash_json(
            {
                "address_id": address_id,
                "assertion_type": assertion_type,
                "state": state,
                "operational_tier": operational_tier,
                "primary_claim_id": primary_claim_id,
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
                conflict_set_id, resolved_at, resolution_version, freshness_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        if review and review["decision"] == "accept" and tiers & {"A", "B"}:
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
        raw = candidate_entity_id or candidate_entity_name
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


def _tier_rank(tier: str) -> int:
    return {"A": 0, "B": 1, "C": 2, "D": 3, "E": 4}[tier]


def _entity_id_for_assertion(assertion_type: str, evidence_rows: list) -> str | None:
    if assertion_type != "entity_control":
        return None
    return next((row["candidate_entity_id"] for row in evidence_rows if row["candidate_entity_id"]), None)


def _entity_name_for_assertion(assertion_type: str, evidence_rows: list) -> str | None:
    if assertion_type != "entity_control":
        return None
    return next((row["candidate_entity_name"] for row in evidence_rows if row["candidate_entity_name"]), None)


def _parse_utc(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timestamp must be timezone-aware")
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _hash_json(value: dict[str, object]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
