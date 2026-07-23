"""Immutable, checksum-pinned resolver snapshot exports."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from crypto_address_identity.chains.bitcoin import normalize_bitcoin_address
from crypto_address_identity.storage.sqlite import IdentityDatabase


class ExportCollisionError(RuntimeError):
    """Raised when an immutable snapshot path exists with different content."""


@dataclass(frozen=True)
class ExportResult:
    snapshot_id: str | None
    directory: Path
    manifest_sha256: str
    resolution_count: int
    evidence_summary_count: int
    written: bool


class ResolverExporter:
    """Writes deterministic resolver snapshots and registers their manifests."""

    def __init__(self, database: IdentityDatabase, root: Path) -> None:
        self.database = database
        self.root = Path(root)

    def export(self, *, chain_key: str, as_of: str, dry_run: bool = False) -> ExportResult:
        if chain_key != "bitcoin":
            raise ValueError("BTC-first export supports only bitcoin")
        as_of_utc = _parse_utc(as_of)
        records, summaries = self._materialize_records(chain_key, as_of_utc)
        files = {
            "resolutions.ndjson": _ndjson_bytes(records),
            "evidence_summary.ndjson": _ndjson_bytes(summaries),
        }
        manifest = {
            "schema_version": "btc_identity_export_v2",
            "chain_key": chain_key,
            "as_of": as_of_utc,
            "resolver_versions": sorted({record["resolution_version"] for record in records}),
            "resolution_count": len(records),
            "evidence_summary_count": len(summaries),
            "files": {
                name: {"sha256": _sha256(content), "bytes": len(content)}
                for name, content in sorted(files.items())
            },
        }
        manifest_bytes = _json_bytes(manifest)
        manifest_sha256 = _sha256(manifest_bytes)
        # Resolver v2 adds a policy-bearing record contract. Keep the older
        # v1 immutable exports valid instead of colliding with them by date.
        relative_path = Path(chain_key) / "v2" / _snapshot_name(as_of_utc)
        directory = self.root / relative_path
        result = ExportResult(
            snapshot_id=None,
            directory=directory,
            manifest_sha256=manifest_sha256,
            resolution_count=len(records),
            evidence_summary_count=len(summaries),
            written=not dry_run,
        )
        if dry_run:
            return result

        self._write_or_verify_existing(directory, files, manifest_bytes, manifest_sha256)
        snapshot_id = self._register_snapshot(
            chain_key=chain_key,
            as_of=as_of_utc,
            relative_path=str(relative_path),
            manifest_sha256=manifest_sha256,
            resolution_count=len(records),
            evidence_summary_count=len(summaries),
        )
        return ExportResult(
            snapshot_id=snapshot_id,
            directory=directory,
            manifest_sha256=manifest_sha256,
            resolution_count=len(records),
            evidence_summary_count=len(summaries),
            written=True,
        )

    def _materialize_records(self, chain_key: str, as_of: str) -> tuple[list[dict], list[dict]]:
        with self.database.read_connection() as connection:
            rows = connection.execute(
                """
                SELECT r.*, a.normalized_address, a.chain_key, c.entity_id,
                       c.asserted_value AS primary_asserted_value
                FROM identity_resolution AS r
                JOIN address_subject AS a ON a.address_id = r.address_id
                LEFT JOIN identity_claim AS c ON c.claim_id = r.primary_claim_id
                WHERE a.chain_key = ? AND r.resolved_at <= ?
                ORDER BY r.address_id, r.assertion_type, r.resolved_at DESC, r.resolution_id DESC
                """,
                (chain_key, as_of),
            ).fetchall()

            latest_rows = []
            seen: set[tuple[str, str]] = set()
            for row in rows:
                key = (row["address_id"], row["assertion_type"])
                if key not in seen:
                    latest_rows.append(row)
                    seen.add(key)

            records: list[dict] = []
            summaries: list[dict] = []
            for row in latest_rows:
                candidate_claim_ids = json.loads(row["candidate_claim_ids_json"])
                candidate_entities = []
                if candidate_claim_ids:
                    placeholders = ",".join("?" for _ in candidate_claim_ids)
                    claim_rows = connection.execute(
                        f"""
                        SELECT claim_id, entity_id, asserted_value, evidence_strength,
                               corroboration_count, independence_count
                        FROM identity_claim WHERE claim_id IN ({placeholders})
                        ORDER BY claim_id
                        """,
                        candidate_claim_ids,
                    ).fetchall()
                    candidate_entities = [
                        claim["entity_id"] or claim["asserted_value"] for claim in claim_rows
                    ]
                record = {
                    "chain_key": row["chain_key"],
                    "normalized_address": row["normalized_address"],
                    "address_id": row["address_id"],
                    "assertion_type": row["assertion_type"],
                    "state": row["state"],
                    "operational_tier": row["operational_tier"],
                    "accepted_entity": row["entity_id"]
                    if row["operational_tier"] == "lookup_usable"
                    else None,
                    "resolved_entity_display": row["primary_entity_display"]
                    if row["operational_tier"] == "lookup_usable"
                    else None,
                    "resolved_asserted_value": row["primary_asserted_value"]
                    if row["operational_tier"] == "lookup_usable"
                    else None,
                    "resolution_policy": row["resolution_policy"],
                    "entity_candidates": candidate_entities,
                    "wallet_role_candidates": [],
                    "conflict_set_id": row["conflict_set_id"],
                    "resolved_at": row["resolved_at"],
                    "resolution_version": row["resolution_version"],
                    "freshness_status": row["freshness_status"],
                }
                records.append(record)
                summaries.append(
                    {
                        "chain_key": row["chain_key"],
                        "normalized_address": row["normalized_address"],
                        "assertion_type": row["assertion_type"],
                        "state": row["state"],
                        "candidate_claim_count": len(candidate_claim_ids),
                        "conflict_set_id": row["conflict_set_id"],
                    }
                )
        return records, summaries

    def _write_or_verify_existing(
        self,
        directory: Path,
        files: dict[str, bytes],
        manifest_bytes: bytes,
        manifest_sha256: str,
    ) -> None:
        if directory.exists():
            manifest_path = directory / "manifest.json"
            if not manifest_path.is_file() or _sha256(manifest_path.read_bytes()) != manifest_sha256:
                raise ExportCollisionError("Immutable snapshot path already exists with different content")
            for name, expected_content in files.items():
                existing_path = directory / name
                if not existing_path.is_file() or existing_path.read_bytes() != expected_content:
                    raise ExportCollisionError("Immutable snapshot path contains corrupted content")
            return

        directory.parent.mkdir(parents=True, exist_ok=True)
        temporary_directory = Path(tempfile.mkdtemp(prefix=".snapshot-", dir=directory.parent))
        try:
            for name, content in files.items():
                (temporary_directory / name).write_bytes(content)
            (temporary_directory / "manifest.json").write_bytes(manifest_bytes)
            try:
                os.rename(temporary_directory, directory)
            except FileExistsError as exc:
                raise ExportCollisionError("Immutable snapshot path was created concurrently") from exc
        except Exception:
            shutil.rmtree(temporary_directory, ignore_errors=True)
            raise

    def _register_snapshot(
        self,
        *,
        chain_key: str,
        as_of: str,
        relative_path: str,
        manifest_sha256: str,
        resolution_count: int,
        evidence_summary_count: int,
    ) -> str:
        with self.database.write_transaction() as connection:
            existing = connection.execute(
                "SELECT snapshot_id, manifest_sha256 FROM resolver_snapshot WHERE relative_path = ?",
                (relative_path,),
            ).fetchone()
            if existing:
                if existing["manifest_sha256"] != manifest_sha256:
                    raise ExportCollisionError("Snapshot registry path has a different manifest")
                return existing["snapshot_id"]
            snapshot_id = str(uuid.uuid4())
            connection.execute(
                """
                INSERT INTO resolver_snapshot (
                    snapshot_id, chain_key, resolver_version, as_of, relative_path,
                    manifest_sha256, resolution_count, evidence_summary_count, created_at
                ) VALUES (?, ?, 'btc_resolver_v2', ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id,
                    chain_key,
                    as_of,
                    relative_path,
                    manifest_sha256,
                    resolution_count,
                    evidence_summary_count,
                    as_of,
                ),
            )
        return snapshot_id


class ResolverSnapshot:
    """Validated in-memory snapshot reader for downstream read-only consumers."""

    def __init__(self, directory: Path, manifest: dict, records: list[dict]) -> None:
        self.directory = directory
        self.manifest = manifest
        self._records = {
            (record["chain_key"], record["normalized_address"], record["assertion_type"]): record
            for record in records
        }

    @classmethod
    def load(cls, directory: Path) -> "ResolverSnapshot":
        directory = Path(directory)
        manifest_path = directory / "manifest.json"
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError("Resolver snapshot manifest is unavailable") from exc
        if manifest.get("schema_version") not in {
            "btc_identity_export_v1",
            "btc_identity_export_v2",
        }:
            raise ValueError("Resolver snapshot schema is unsupported")
        files = manifest.get("files")
        if not isinstance(files, dict):
            raise ValueError("Resolver snapshot manifest has no file checksums")
        for name, details in files.items():
            path = directory / name
            if not path.is_file() or not isinstance(details, dict):
                raise ValueError("Resolver snapshot checksum validation failed")
            if _sha256(path.read_bytes()) != details.get("sha256"):
                raise ValueError("Resolver snapshot checksum validation failed")
        try:
            records = [
                json.loads(line)
                for line in (directory / "resolutions.ndjson").read_text(encoding="utf-8").splitlines()
                if line
            ]
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError("Resolver snapshot records are invalid") from exc
        return cls(directory, manifest, records)

    def lookup(self, chain_key: str, address: str, assertion_type: str) -> dict | None:
        if chain_key != "bitcoin":
            return None
        subject = normalize_bitcoin_address(address)
        return self._records.get(("bitcoin", subject.normalized_address, assertion_type))


def _ndjson_bytes(records: list[dict]) -> bytes:
    lines = [json.dumps(record, sort_keys=True, separators=(",", ":")) for record in records]
    return ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")


def _json_bytes(value: dict) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _parse_utc(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timestamp must be timezone-aware")
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _snapshot_name(as_of: str) -> str:
    return as_of.replace("-", "").replace(":", "").replace(".", "").replace("Z", "Z")
