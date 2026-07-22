"""Restricted, content-addressed raw source payload storage."""

from __future__ import annotations

import gzip
import hashlib
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from crypto_address_identity.storage.sqlite import IdentityDatabase


@dataclass(frozen=True)
class StoredPayload:
    payload_sha256: str
    relative_path: str
    compression: str
    byte_count: int


@dataclass(frozen=True)
class PayloadVerification:
    payload_sha256: str
    status: str


class RawPayloadStore:
    """Stores payload bytes outside Git while retaining only safe DB metadata."""

    def __init__(self, database: IdentityDatabase, root: Path) -> None:
        self.database = database
        self.root = Path(root)

    def persist(self, payload: bytes) -> StoredPayload:
        if not isinstance(payload, bytes):
            raise TypeError("Raw payload must be bytes")
        payload_sha256 = hashlib.sha256(payload).hexdigest()
        relative_path = str(Path("sha256") / payload_sha256[:2] / f"{payload_sha256}.json.gz")
        destination = self._resolve_relative_path(relative_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if not destination.exists():
            compressed = gzip.compress(payload, mtime=0)
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=".payload-", suffix=".tmp", dir=destination.parent
            )
            try:
                with os.fdopen(descriptor, "wb") as temporary_file:
                    temporary_file.write(compressed)
                    temporary_file.flush()
                    os.fsync(temporary_file.fileno())
                os.replace(temporary_name, destination)
            finally:
                if os.path.exists(temporary_name):
                    os.unlink(temporary_name)

        with self.database.write_transaction() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO raw_payload_object (
                    payload_sha256, relative_path, compression, byte_count,
                    retention_status, created_at
                ) VALUES (?, ?, 'gzip', ?, 'active', ?)
                """,
                (payload_sha256, relative_path, len(payload), _utc_now()),
            )
            row = connection.execute(
                """
                SELECT payload_sha256, relative_path, compression, byte_count
                FROM raw_payload_object WHERE payload_sha256 = ?
                """,
                (payload_sha256,),
            ).fetchone()
        return StoredPayload(
            payload_sha256=row["payload_sha256"],
            relative_path=row["relative_path"],
            compression=row["compression"],
            byte_count=row["byte_count"],
        )

    def verify(self, payload_sha256: str) -> PayloadVerification:
        with self.database.read_connection() as connection:
            row = connection.execute(
                "SELECT relative_path, compression FROM raw_payload_object WHERE payload_sha256 = ?",
                (payload_sha256,),
            ).fetchone()
        if row is None:
            return PayloadVerification(payload_sha256, "unknown")
        path = self._resolve_relative_path(row["relative_path"])
        if not path.is_file():
            return PayloadVerification(payload_sha256, "missing")
        try:
            raw = gzip.decompress(path.read_bytes()) if row["compression"] == "gzip" else path.read_bytes()
        except (OSError, EOFError):
            return PayloadVerification(payload_sha256, "corrupt")
        status = "active" if hashlib.sha256(raw).hexdigest() == payload_sha256 else "corrupt"
        return PayloadVerification(payload_sha256, status)

    def _resolve_relative_path(self, relative_path: str) -> Path:
        relative = Path(relative_path)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("Raw payload path must be a safe relative path")
        root = self.root.resolve()
        candidate = (root / relative).resolve()
        if not candidate.is_relative_to(root):
            raise ValueError("Raw payload path escapes configured root")
        return candidate


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")
