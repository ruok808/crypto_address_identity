"""Candidate intake, run ledger, and SQLite-backed request reservations."""

from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from crypto_address_identity.chains.bitcoin import BitcoinAddressSubject, normalize_bitcoin_address
from crypto_address_identity.storage.sqlite import IdentityDatabase


CandidateReason = Literal[
    "known_watchlist",
    "whale_counterparty",
    "transfer_counterparty",
    "official_evidence",
    "manual_review",
    "replay",
]


class RateLimitExceeded(RuntimeError):
    """Raised when a shared rolling request window has no remaining slot."""


class ByteBudgetExceeded(RuntimeError):
    """Raised before dispatch would exceed the run response-byte budget."""


class CandidateInput(BaseModel):
    """Validated, auditable candidate handoff record."""

    chain_key: str
    address: str
    reason: CandidateReason
    priority: int = Field(ge=0, le=100)
    source_reference: str = Field(min_length=1, max_length=512)
    requested_at: datetime

    @field_validator("chain_key")
    @classmethod
    def validate_chain_key(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized != "bitcoin":
            raise ValueError("BTC-first candidate intake accepts only bitcoin")
        return normalized

    @field_validator("requested_at")
    @classmethod
    def validate_requested_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("requested_at must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_address(self) -> "CandidateInput":
        normalize_bitcoin_address(self.address)
        return self

    @property
    def subject(self) -> BitcoinAddressSubject:
        return normalize_bitcoin_address(self.address)


@dataclass(frozen=True)
class CandidateImportResult:
    imported_count: int
    candidate_request_ids: tuple[str, ...]


@dataclass(frozen=True)
class SelectedCandidate:
    candidate_request_id: str
    address_id: str
    normalized_address: str
    reason: str
    priority: int
    source_reference: str
    requested_at: str


@dataclass(frozen=True)
class QuotaReservation:
    reservation_id: str
    run_id: str
    reserved_at: str
    estimated_response_bytes: int


class CandidateService:
    """Writes candidate provenance and selects queue work deterministically."""

    def __init__(self, database: IdentityDatabase) -> None:
        self.database = database

    def import_candidates(
        self, candidates: list[CandidateInput], *, created_at: datetime | None = None
    ) -> CandidateImportResult:
        timestamp = _as_utc_string(created_at or datetime.now(UTC))
        request_ids: list[str] = []
        with self.database.write_transaction() as connection:
            for candidate in candidates:
                subject = candidate.subject
                connection.execute(
                    """
                    INSERT OR IGNORE INTO address_subject (
                        address_id, chain_key, normalized_address, display_address,
                        address_type, first_seen_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        subject.address_id,
                        subject.chain_key,
                        subject.normalized_address,
                        subject.display_address,
                        subject.address_type,
                        timestamp,
                    ),
                )
                candidate_request_id = str(uuid.uuid4())
                request_ids.append(candidate_request_id)
                connection.execute(
                    """
                    INSERT INTO candidate_request (
                        candidate_request_id, address_id, reason, priority,
                        source_reference, requested_at, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        candidate_request_id,
                        subject.address_id,
                        candidate.reason,
                        candidate.priority,
                        candidate.source_reference,
                        _as_utc_string(candidate.requested_at),
                        timestamp,
                    ),
                )
        return CandidateImportResult(len(request_ids), tuple(request_ids))

    def select_candidates(
        self,
        *,
        limit: int,
        source_reference_prefix: str | None = None,
        fresh_discovery_after: datetime | None = None,
    ) -> tuple[SelectedCandidate, ...]:
        if limit < 1:
            raise ValueError("limit must be positive")
        prefix = source_reference_prefix.strip() if source_reference_prefix else None
        if source_reference_prefix is not None and not prefix:
            raise ValueError("source_reference_prefix must not be empty")
        if fresh_discovery_after is not None and fresh_discovery_after.tzinfo is None:
            raise ValueError("fresh_discovery_after must be timezone-aware")
        where_conditions: list[str] = []
        parameters: list[object] = []
        if prefix:
            where_conditions.append("substr(cr.source_reference, 1, ?) = ?")
            parameters.extend((len(prefix), prefix))
        if fresh_discovery_after is not None:
            where_conditions.append(
                """
                NOT EXISTS (
                    SELECT 1 FROM source_observation AS observation
                    WHERE observation.address_id = cr.address_id
                      AND observation.source_id = '0xrouter'
                      AND observation.query_profile = 'discovery'
                      AND observation.outcome = 'success'
                      AND observation.completed_at > ?
                )
                """
            )
            parameters.append(_as_utc_string(fresh_discovery_after))
        where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
        parameters.append(limit)
        with self.database.read_connection() as connection:
            rows = connection.execute(
                f"""
                SELECT cr.candidate_request_id, cr.address_id, a.normalized_address,
                       cr.reason, cr.priority, cr.source_reference, cr.requested_at
                FROM candidate_request AS cr
                JOIN address_subject AS a ON a.address_id = cr.address_id
                {where_clause}
                ORDER BY cr.priority DESC, cr.requested_at ASC, cr.created_at ASC,
                         cr.candidate_request_id ASC
                LIMIT ?
                """,
                tuple(parameters),
            ).fetchall()
        return tuple(
            SelectedCandidate(
                candidate_request_id=row["candidate_request_id"],
                address_id=row["address_id"],
                normalized_address=row["normalized_address"],
                reason=row["reason"],
                priority=row["priority"],
                source_reference=row["source_reference"],
                requested_at=row["requested_at"],
            )
            for row in rows
        )


class QuotaManager:
    """Coordinates rolling request and per-run byte budgets through SQLite."""

    def __init__(self, database: IdentityDatabase) -> None:
        self.database = database

    def create_run(
        self,
        *,
        mode: Literal["dry_run", "execute"],
        request_limit: int,
        response_bytes_budget: int,
        started_at: datetime | None = None,
    ) -> str:
        if request_limit < 1 or response_bytes_budget < 1:
            raise ValueError("request_limit and response_bytes_budget must be positive")
        run_id = str(uuid.uuid4())
        timestamp = _as_utc_string(started_at or datetime.now(UTC))
        with self.database.write_transaction() as connection:
            connection.execute(
                """
                INSERT INTO ingestion_run (
                    ingestion_run_id, mode, status, started_at, request_limit,
                    response_bytes_budget
                ) VALUES (?, ?, 'running', ?, ?, ?)
                """,
                (run_id, mode, timestamp, request_limit, response_bytes_budget),
            )
        return run_id

    def reserve(
        self,
        *,
        run_id: str,
        now: datetime,
        estimated_response_bytes: int,
    ) -> QuotaReservation:
        if estimated_response_bytes < 1:
            raise ValueError("estimated_response_bytes must be at least one")
        timestamp = _as_utc_string(now)
        cutoff = _as_utc_string(now.astimezone(UTC) - timedelta(minutes=1))
        with self.database.write_transaction() as connection:
            run = connection.execute(
                """
                SELECT request_limit, response_bytes_budget, status
                FROM ingestion_run WHERE ingestion_run_id = ?
                """,
                (run_id,),
            ).fetchone()
            if run is None:
                raise ValueError("Unknown ingestion run")
            if run["status"] != "running":
                raise ValueError("Cannot reserve a request for a non-running ingestion run")

            window_count = connection.execute(
                "SELECT COUNT(*) FROM request_reservation WHERE reserved_at > ?",
                (cutoff,),
            ).fetchone()[0]
            if window_count >= run["request_limit"]:
                raise RateLimitExceeded("Shared request limit reached")

            consumed_bytes = connection.execute(
                """
                SELECT COALESCE(SUM(COALESCE(actual_response_bytes, estimated_response_bytes)), 0)
                FROM request_reservation
                WHERE ingestion_run_id = ? AND outcome != 'budget_exhausted'
                """,
                (run_id,),
            ).fetchone()[0]
            if consumed_bytes + estimated_response_bytes > run["response_bytes_budget"]:
                raise ByteBudgetExceeded("Response byte budget would be exceeded")

            reservation_id = str(uuid.uuid4())
            connection.execute(
                """
                INSERT INTO request_reservation (
                    request_reservation_id, ingestion_run_id, reserved_at,
                    rolling_window_start, request_limit, response_bytes_budget,
                    estimated_response_bytes, outcome
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'reserved')
                """,
                (
                    reservation_id,
                    run_id,
                    timestamp,
                    cutoff,
                    run["request_limit"],
                    run["response_bytes_budget"],
                    estimated_response_bytes,
                ),
            )
        return QuotaReservation(reservation_id, run_id, timestamp, estimated_response_bytes)

    def complete(
        self, reservation_id: str, *, actual_response_bytes: int, outcome: str
    ) -> None:
        if actual_response_bytes < 0:
            raise ValueError("actual_response_bytes cannot be negative")
        allowed_outcomes = {"dispatched", "completed", "failed", "rate_limited"}
        if outcome not in allowed_outcomes:
            raise ValueError("Invalid reservation outcome")
        with self.database.write_transaction() as connection:
            reservation = connection.execute(
                "SELECT ingestion_run_id FROM request_reservation WHERE request_reservation_id = ?",
                (reservation_id,),
            ).fetchone()
            if reservation is None:
                raise ValueError("Unknown request reservation")
            connection.execute(
                """
                UPDATE request_reservation
                SET actual_response_bytes = ?, outcome = ?
                WHERE request_reservation_id = ?
                """,
                (actual_response_bytes, outcome, reservation_id),
            )
            run_id = reservation["ingestion_run_id"]
            connection.execute(
                """
                UPDATE ingestion_run
                SET request_count = (
                        SELECT COUNT(*) FROM request_reservation
                        WHERE ingestion_run_id = ?
                    ),
                    response_bytes_received = (
                        SELECT COALESCE(SUM(COALESCE(actual_response_bytes, 0)), 0)
                        FROM request_reservation WHERE ingestion_run_id = ?
                    )
                WHERE ingestion_run_id = ?
                """,
                (run_id, run_id, run_id),
            )


def _as_utc_string(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("Timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
