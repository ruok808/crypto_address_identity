"""Checksum-pinned, read-only calibration anchors from identity SQLite."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from urllib.parse import quote

from pydantic import computed_field, field_validator, model_validator

from crypto_address_identity.chains.bitcoin import normalize_bitcoin_address
from crypto_address_identity.universe.models import UniverseModel


AnchorReason = Literal[
    "official_or_signed_evidence",
    "existing_provider_conflict",
    "provider_entity_prediction",
    "existing_system_watchlist",
]


class AnchorIntegrityError(RuntimeError):
    """Raised when the immutable calibration snapshot cannot be trusted."""


class CalibrationAnchorRow(UniverseModel):
    address_id: str
    normalized_address: str
    reason_code: AnchorReason

    @model_validator(mode="after")
    def validate_address(self) -> "CalibrationAnchorRow":
        subject = normalize_bitcoin_address(self.normalized_address)
        if subject.normalized_address != self.normalized_address:
            raise ValueError("anchor address must be canonical")
        if subject.address_id != self.address_id:
            raise ValueError("anchor address_id mismatch")
        return self


class CalibrationAnchorSnapshot(UniverseModel):
    as_of: datetime
    database_sha256: str
    rows: tuple[CalibrationAnchorRow, ...]

    @field_validator("as_of")
    @classmethod
    def validate_as_of(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("anchor as_of must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator("database_sha256")
    @classmethod
    def validate_database_hash(cls, value: str) -> str:
        if len(value) != 64 or value != value.lower():
            raise ValueError("database_sha256 must be a lower-case SHA-256 digest")
        try:
            bytes.fromhex(value)
        except ValueError as exc:
            raise ValueError("database_sha256 must be a SHA-256 digest") from exc
        return value

    @computed_field
    @property
    def snapshot_sha256(self) -> str:
        payload = {
            "as_of": self.as_of.isoformat().replace("+00:00", "Z"),
            "database_sha256": self.database_sha256,
            "rows": [row.model_dump(mode="json") for row in self.rows],
        }
        return hashlib.sha256(
            json.dumps(
                payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
            ).encode("ascii")
        ).hexdigest()


class CalibrationAnchorReader:
    """Extract the minimum calibration surface without migrations or writes."""

    def __init__(self, database_path: Path) -> None:
        self._database_path = Path(database_path)

    def read(self, *, as_of: datetime) -> CalibrationAnchorSnapshot:
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise AnchorIntegrityError("anchor as_of must be timezone-aware")
        timestamp = as_of.astimezone(UTC)
        timestamp_text = timestamp.isoformat().replace("+00:00", "Z")
        _reject_uncheckpointed_wal(self._database_path)
        try:
            with self._database_path.open("rb") as descriptor:
                pre_descriptor_stat = os.fstat(descriptor.fileno())
                pre_path_stat = self._database_path.stat()
                before_sha256 = _sha256_file(self._database_path)
                rows = self._read_rows(timestamp_text)
                after_sha256 = _sha256_file(self._database_path)
                post_descriptor_stat = os.fstat(descriptor.fileno())
                post_path_stat = self._database_path.stat()
                _reject_uncheckpointed_wal(self._database_path)
        except (OSError, sqlite3.Error, ValueError) as exc:
            raise AnchorIntegrityError("calibration database read failed") from exc

        if (
            before_sha256 != after_sha256
            or pre_descriptor_stat.st_ino != post_descriptor_stat.st_ino
            or pre_descriptor_stat.st_size != post_descriptor_stat.st_size
            or pre_descriptor_stat.st_mtime_ns != post_descriptor_stat.st_mtime_ns
            or pre_path_stat.st_ino != post_path_stat.st_ino
            or pre_path_stat.st_size != post_path_stat.st_size
            or pre_path_stat.st_mtime_ns != post_path_stat.st_mtime_ns
        ):
            raise AnchorIntegrityError("calibration database changed during read")

        try:
            validated = tuple(
                CalibrationAnchorRow(
                    address_id=address_id,
                    normalized_address=normalized_address,
                    reason_code=reason_code,
                )
                for address_id, normalized_address, reason_code in rows
            )
        except ValueError as exc:
            raise AnchorIntegrityError("calibration anchor row is invalid") from exc
        return CalibrationAnchorSnapshot(
            as_of=timestamp,
            database_sha256=before_sha256,
            rows=validated,
        )

    def _read_rows(self, as_of: str) -> tuple[tuple[str, str, AnchorReason], ...]:
        uri = (
            f"file:{quote(str(self._database_path.resolve()), safe='/')}"
            "?mode=ro&immutable=1"
        )
        connection = sqlite3.connect(uri, uri=True, timeout=5.0)
        connection.row_factory = sqlite3.Row
        try:
            candidates: set[tuple[str, str, AnchorReason]] = set()
            candidates.update(
                (
                    row["address_id"],
                    row["normalized_address"],
                    "official_or_signed_evidence",
                )
                for row in connection.execute(
                    """
                    SELECT DISTINCT subject.address_id, subject.normalized_address
                    FROM identity_evidence AS evidence
                    JOIN address_subject AS subject
                      ON subject.address_id = evidence.address_id
                    WHERE subject.chain_key = 'bitcoin'
                      AND evidence.evidence_tier IN ('A', 'B')
                      AND evidence.evidence_status = 'valid'
                      AND evidence.observed_at <= ?
                      AND (evidence.effective_from IS NULL OR evidence.effective_from <= ?)
                      AND (evidence.effective_to IS NULL OR evidence.effective_to >= ?)
                      AND (evidence.expires_at IS NULL OR evidence.expires_at >= ?)
                      AND (
                        evidence.evidence_tier != 'A'
                        OR evidence.verification_result = 'valid'
                      )
                    """,
                    (as_of, as_of, as_of, as_of),
                )
            )
            candidates.update(
                (
                    row["address_id"],
                    row["normalized_address"],
                    "existing_provider_conflict",
                )
                for row in connection.execute(
                    """
                    SELECT DISTINCT subject.address_id, subject.normalized_address
                    FROM conflict_set AS conflict
                    JOIN address_subject AS subject
                      ON subject.address_id = conflict.address_id
                    WHERE subject.chain_key = 'bitcoin'
                      AND conflict.status = 'active'
                      AND conflict.created_at <= ?
                      AND (conflict.resolved_at IS NULL OR conflict.resolved_at > ?)
                    """,
                    (as_of, as_of),
                )
            )
            candidates.update(
                (
                    row["address_id"],
                    row["normalized_address"],
                    "provider_entity_prediction",
                )
                for row in connection.execute(
                    """
                    SELECT DISTINCT subject.address_id, subject.normalized_address
                    FROM coverage_entity_prediction AS prediction
                    JOIN address_subject AS subject
                      ON subject.address_id = prediction.address_id
                    WHERE subject.chain_key = 'bitcoin'
                      AND prediction.observed_at <= ?
                    """,
                    (as_of,),
                )
            )
            candidates.update(
                (
                    row["address_id"],
                    row["normalized_address"],
                    "existing_system_watchlist",
                )
                for row in connection.execute(
                    """
                    SELECT DISTINCT subject.address_id, subject.normalized_address
                    FROM candidate_request AS candidate
                    JOIN address_subject AS subject
                      ON subject.address_id = candidate.address_id
                    WHERE subject.chain_key = 'bitcoin'
                      AND candidate.reason = 'known_watchlist'
                      AND candidate.requested_at <= ?
                    """,
                    (as_of,),
                )
            )
            return tuple(sorted(candidates))
        finally:
            connection.close()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _reject_uncheckpointed_wal(database_path: Path) -> None:
    wal_path = Path(f"{database_path}-wal")
    try:
        if wal_path.exists() and wal_path.stat().st_size > 0:
            raise AnchorIntegrityError(
                "calibration database has an uncheckpointed WAL"
            )
    except OSError as exc:
        raise AnchorIntegrityError("calibration WAL state is unavailable") from exc
