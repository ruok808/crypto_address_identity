"""Atomic, checksum-pinned Parquet campaigns for BTC universe facts."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import uuid
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import resources
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import ValidationError

from crypto_address_identity.universe.models import (
    AddressFeatureRow,
    CampaignManifest,
    ScriptSubjectRow,
    SourceManifest,
    SourceProbeResult,
    UniverseCoverageCounters,
)


ADDRESS_FEATURE_SCHEMA = pa.schema(
    [
        pa.field("feature_version", pa.string(), nullable=False),
        pa.field("address_id", pa.string(), nullable=False),
        pa.field("normalized_address", pa.string(), nullable=False),
        pa.field("address_type", pa.string(), nullable=False),
        pa.field("first_seen_height", pa.int64(), nullable=False),
        pa.field("last_seen_height", pa.int64(), nullable=False),
        pa.field("first_seen_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("last_seen_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("output_count", pa.int64(), nullable=False),
        pa.field("spent_output_count", pa.int64(), nullable=False),
        pa.field("transaction_count", pa.int64(), nullable=False),
        pa.field("current_utxo_sats", pa.int64(), nullable=False),
        pa.field("lifetime_received_sats", pa.int64(), nullable=False),
        pa.field("lifetime_spent_sats", pa.int64(), nullable=False),
        pa.field("max_single_output_sats", pa.int64(), nullable=False),
        pa.field("max_same_tx_received_sats", pa.int64(), nullable=False),
        pa.field("inflow_30d_sats", pa.int64(), nullable=False),
        pa.field("outflow_30d_sats", pa.int64(), nullable=False),
        pa.field("gross_flow_30d_sats", pa.int64(), nullable=False),
        pa.field("inflow_90d_sats", pa.int64(), nullable=False),
        pa.field("outflow_90d_sats", pa.int64(), nullable=False),
        pa.field("gross_flow_90d_sats", pa.int64(), nullable=False),
        pa.field("gross_flow_365d_sats", pa.int64(), nullable=False),
        pa.field("direct_large_counterparty_count", pa.int64(), nullable=False),
    ]
)
SCRIPT_SUBJECT_SCHEMA = pa.schema(
    [
        pa.field("script_id", pa.string(), nullable=False),
        pa.field("script_hex", pa.string(), nullable=False),
        pa.field("script_type", pa.string(), nullable=False),
        pa.field("normalized_address", pa.string(), nullable=True),
        pa.field("address_id", pa.string(), nullable=True),
        pa.field("provider_enrichable", pa.bool_(), nullable=False),
    ]
)
SOURCE_ACCOUNTING_SCHEMA = pa.schema(
    [
        pa.field("total_output_rows", pa.int64(), nullable=False),
        pa.field("total_input_rows", pa.int64(), nullable=False),
        pa.field("distinct_script_subjects", pa.int64(), nullable=False),
        pa.field("standard_single_address_rows", pa.int64(), nullable=False),
        pa.field("empty_address_rows", pa.int64(), nullable=False),
        pa.field("multi_address_rows", pa.int64(), nullable=False),
        pa.field("nonstandard_rows", pa.int64(), nullable=False),
        pa.field("unmatched_input_rows", pa.int64(), nullable=False),
    ]
)


class UniverseIntegrityError(RuntimeError):
    """Raised before incomplete or ambiguous universe facts can publish."""


@dataclass(frozen=True)
class UniverseVerification:
    status: str
    campaign_id: str
    errors: tuple[str, ...]
    checked_files: int


@dataclass(frozen=True)
class PublishedCampaign:
    root: Path
    source_manifest: SourceManifest
    campaign_manifest: CampaignManifest

    @property
    def campaign_id(self) -> str:
        return self.source_manifest.campaign_id

    @property
    def manifest_sha256(self) -> str:
        return self.source_manifest.manifest_sha256

    @property
    def address_feature_rows(self) -> int:
        return self.campaign_manifest.address_feature_rows

    @property
    def script_subject_rows(self) -> int:
        return self.campaign_manifest.script_subject_rows

    def open_duckdb(self) -> duckdb.DuckDBPyConnection:
        connection = duckdb.connect(":memory:")
        sql = (
            resources.files("crypto_address_identity.universe")
            .joinpath("sql/duckdb/schema.sql")
            .read_text(encoding="utf-8")
        )
        parameters = {
            "address_feature_glob": str(
                self.root / "address_features" / "prefix=*" / "*.parquet"
            ),
            "script_subject_glob": str(
                self.root / "script_subjects" / "prefix=*" / "*.parquet"
            ),
            "source_accounting_glob": str(
                self.root / "source_accounting" / "*.parquet"
            ),
        }
        try:
            for statement in (item.strip() for item in sql.split(";")):
                if statement:
                    names = set(re.findall(r"\$([a-z_]+)", statement))
                    rendered = statement
                    for name in names:
                        rendered = rendered.replace(
                            f"${name}", _duckdb_string_literal(parameters[name])
                        )
                    connection.execute(rendered)
        except Exception:
            connection.close()
            raise
        return connection


class UniverseStore:
    """Own immutable campaign directories without touching identity SQLite."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.campaigns_root = root / "campaigns"
        self.staging_root = root / ".staging"

    def begin_campaign(self, manifest: SourceManifest) -> "CampaignWriter":
        self.campaigns_root.mkdir(parents=True, exist_ok=True)
        self.staging_root.mkdir(parents=True, exist_ok=True)
        final_root = self.campaigns_root / manifest.campaign_id
        if final_root.exists():
            raise UniverseIntegrityError("campaign_id already exists")
        staging = self.staging_root / f"{manifest.campaign_id}-{uuid.uuid4().hex}"
        staging.mkdir()
        for relative in (
            "source_probes",
            "address_features",
            "script_subjects",
            "calibration_anchors",
            "source_accounting",
        ):
            (staging / relative).mkdir()
        return CampaignWriter(
            store=self,
            source_manifest=manifest,
            staging_root=staging,
            final_root=final_root,
        )

    def load(self, campaign_id: str) -> PublishedCampaign:
        root = self.campaigns_root / campaign_id
        if not root.is_dir():
            raise UniverseIntegrityError("campaign not found")
        manifest = _read_campaign_manifest(root / "manifest.json")
        return PublishedCampaign(
            root=root,
            source_manifest=manifest.source_manifest,
            campaign_manifest=manifest,
        )

    def verify(self, campaign_id: str) -> UniverseVerification:
        root = self.campaigns_root / campaign_id
        if not root.is_dir():
            return UniverseVerification(
                status="failed",
                campaign_id=campaign_id,
                errors=("campaign_not_found",),
                checked_files=0,
            )
        return _verify_root(root, campaign_id)


class CampaignWriter:
    def __init__(
        self,
        *,
        store: UniverseStore,
        source_manifest: SourceManifest,
        staging_root: Path,
        final_root: Path,
    ) -> None:
        self._store = store
        self._source_manifest = source_manifest
        self._staging_root = staging_root
        self._final_root = final_root
        self.duplicate_tracker_path = staging_root / ".dedupe.duckdb"
        self._duplicate_connection: duckdb.DuckDBPyConnection | None = duckdb.connect(
            str(self.duplicate_tracker_path)
        )
        self._duplicate_connection.execute(
            "CREATE TABLE address_ids (identifier VARCHAR PRIMARY KEY)"
        )
        self._duplicate_connection.execute(
            "CREATE TABLE script_ids (identifier VARCHAR PRIMARY KEY)"
        )
        self._part_numbers: dict[tuple[str, str], int] = defaultdict(int)
        self._address_feature_rows = 0
        self._script_subject_rows = 0
        self._source_accounting_rows = 0
        self._source_probes = 0
        self._published = False

    def write_address_features(
        self, rows: Iterable[AddressFeatureRow | Mapping[str, object]]
    ) -> None:
        try:
            validated = [
                row
                if isinstance(row, AddressFeatureRow)
                else AddressFeatureRow.model_validate(row)
                for row in rows
            ]
            self._reject_duplicate_ids(
                (row.address_id for row in validated),
                table="address_ids",
                label="address_id",
            )
            self._write_partitioned(
                directory="address_features",
                rows=validated,
                id_field="address_id",
                schema=ADDRESS_FEATURE_SCHEMA,
            )
            self._address_feature_rows += len(validated)
        except (
            UniverseIntegrityError,
            ValidationError,
            ValueError,
            OSError,
            pa.ArrowException,
        ) as exc:
            self.abort()
            raise UniverseIntegrityError("address feature write failed") from exc

    def write_script_subjects(
        self, rows: Iterable[ScriptSubjectRow | Mapping[str, object]]
    ) -> None:
        try:
            validated = [
                row
                if isinstance(row, ScriptSubjectRow)
                else ScriptSubjectRow.model_validate(row)
                for row in rows
            ]
            self._reject_duplicate_ids(
                (row.script_id for row in validated),
                table="script_ids",
                label="script_id",
            )
            self._write_partitioned(
                directory="script_subjects",
                rows=validated,
                id_field="script_id",
                schema=SCRIPT_SUBJECT_SCHEMA,
            )
            self._script_subject_rows += len(validated)
        except (
            UniverseIntegrityError,
            ValidationError,
            ValueError,
            OSError,
            pa.ArrowException,
        ) as exc:
            self.abort()
            raise UniverseIntegrityError("script subject write failed") from exc

    def write_source_accounting(
        self, counters: UniverseCoverageCounters | Mapping[str, object]
    ) -> None:
        if self._source_accounting_rows:
            self.abort()
            raise UniverseIntegrityError("source accounting may be written once")
        try:
            row = (
                counters
                if isinstance(counters, UniverseCoverageCounters)
                else UniverseCoverageCounters.model_validate(counters)
            )
            self._write_table(
                self._staging_root
                / "source_accounting"
                / "part-00000.parquet",
                [row.model_dump(mode="python")],
                SOURCE_ACCOUNTING_SCHEMA,
            )
            self._source_accounting_rows = 1
        except (ValidationError, ValueError, OSError, pa.ArrowException) as exc:
            self.abort()
            raise UniverseIntegrityError("source accounting write failed") from exc

    def write_source_probe(self, probe: SourceProbeResult) -> None:
        path = self._staging_root / "source_probes" / f"{probe.source_kind}.json"
        _write_json(path, probe.model_dump(mode="json"))
        self._source_probes += 1

    def publish(self) -> PublishedCampaign:
        if self._published:
            raise UniverseIntegrityError("campaign writer is already published")
        if (
            self._address_feature_rows == 0
            or self._script_subject_rows == 0
            or self._source_accounting_rows != 1
        ):
            self.abort()
            raise UniverseIntegrityError("campaign is incomplete")
        try:
            self._close_duplicate_tracker()
            artifact_hashes = {
                str(path.relative_to(self._staging_root)): _sha256_file(path)
                for path in _artifact_files(self._staging_root)
            }
            campaign_manifest = CampaignManifest(
                campaign_id=self._source_manifest.campaign_id,
                source_manifest=self._source_manifest,
                created_at=self._source_manifest.cutoff_time,
                address_feature_rows=self._address_feature_rows,
                script_subject_rows=self._script_subject_rows,
                calibration_anchor_rows=0,
                source_accounting_rows=self._source_accounting_rows,
                artifact_sha256=artifact_hashes,
            )
            _write_json(
                self._staging_root / "manifest.json",
                _campaign_manifest_payload(campaign_manifest),
            )
            _write_checksums(self._staging_root)
            verification = _verify_root(
                self._staging_root, self._source_manifest.campaign_id
            )
            if verification.status != "ok":
                raise UniverseIntegrityError("staged campaign verification failed")
            _fsync_tree(self._staging_root)
            os.replace(self._staging_root, self._final_root)
            _fsync_directory(self._store.campaigns_root)
            self._published = True
            return PublishedCampaign(
                root=self._final_root,
                source_manifest=self._source_manifest,
                campaign_manifest=campaign_manifest,
            )
        except Exception:
            self.abort()
            raise

    def abort(self) -> None:
        if not self._published:
            self._close_duplicate_tracker()
            shutil.rmtree(self._staging_root, ignore_errors=True)

    def _write_partitioned(
        self,
        *,
        directory: str,
        rows: list[AddressFeatureRow] | list[ScriptSubjectRow],
        id_field: str,
        schema: pa.Schema,
    ) -> None:
        grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
        for row in rows:
            identifier = getattr(row, id_field)
            grouped[identifier[:2]].append(row.model_dump(mode="python"))
        for prefix, values in sorted(grouped.items()):
            part_number = self._part_numbers[(directory, prefix)]
            self._part_numbers[(directory, prefix)] += 1
            path = (
                self._staging_root
                / directory
                / f"prefix={prefix}"
                / f"part-{part_number:05d}.parquet"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            self._write_table(path, values, schema)

    @staticmethod
    def _write_table(
        path: Path, rows: list[dict[str, object]], schema: pa.Schema
    ) -> None:
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(
            table,
            path,
            compression="zstd",
            version="2.6",
            write_statistics=True,
        )
        _fsync_file(path)

    def _reject_duplicate_ids(
        self, values: Iterable[str], *, table: str, label: str
    ) -> None:
        if table not in {"address_ids", "script_ids"}:
            raise UniverseIntegrityError("invalid duplicate tracker table")
        if self._duplicate_connection is None:
            raise UniverseIntegrityError("duplicate tracker is closed")
        identifiers = [(value,) for value in values]
        try:
            self._duplicate_connection.execute("BEGIN TRANSACTION")
            self._duplicate_connection.executemany(
                f"INSERT INTO {table} VALUES (?)", identifiers
            )
            self._duplicate_connection.execute("COMMIT")
        except duckdb.ConstraintException as exc:
            self._duplicate_connection.execute("ROLLBACK")
            raise UniverseIntegrityError(f"duplicate {label}") from exc

    def _close_duplicate_tracker(self) -> None:
        if self._duplicate_connection is not None:
            self._duplicate_connection.close()
            self._duplicate_connection = None
        for path in (
            self.duplicate_tracker_path,
            self.duplicate_tracker_path.with_suffix(".duckdb.wal"),
        ):
            path.unlink(missing_ok=True)


def _campaign_manifest_payload(manifest: CampaignManifest) -> dict[str, object]:
    payload = manifest.model_dump(
        mode="json",
        exclude={
            "manifest_sha256": True,
            "source_manifest": {"manifest_sha256"},
        },
    )
    return payload


def _duckdb_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _read_campaign_manifest(path: Path) -> CampaignManifest:
    try:
        return CampaignManifest.model_validate(
            json.loads(path.read_text(encoding="utf-8"))
        )
    except (OSError, ValueError, ValidationError) as exc:
        raise UniverseIntegrityError("campaign manifest is invalid") from exc


def _artifact_files(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*")
        if path.is_file()
        and path.name not in {"manifest.json", "checksums.sha256"}
    )


def _write_json(path: Path, payload: object) -> None:
    encoded = (
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        + "\n"
    ).encode("ascii")
    path.write_bytes(encoded)
    _fsync_file(path)


def _write_checksums(root: Path) -> None:
    files = sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and path.name != "checksums.sha256"
    )
    lines = [
        f"{_sha256_file(path)}  {path.relative_to(root).as_posix()}"
        for path in files
    ]
    path = root / "checksums.sha256"
    path.write_text("\n".join(lines) + "\n", encoding="ascii")
    _fsync_file(path)


def _read_checksums(root: Path) -> dict[str, str]:
    checksums: dict[str, str] = {}
    try:
        lines = (root / "checksums.sha256").read_text(encoding="ascii").splitlines()
    except OSError as exc:
        raise UniverseIntegrityError("checksums are unavailable") from exc
    for line in lines:
        digest, separator, relative = line.partition("  ")
        if (
            not separator
            or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)
            or not relative
            or relative.startswith("/")
            or ".." in Path(relative).parts
        ):
            raise UniverseIntegrityError("checksums are invalid")
        checksums[relative] = digest
    return checksums


def _verify_root(root: Path, campaign_id: str) -> UniverseVerification:
    errors: list[str] = []
    try:
        expected = _read_checksums(root)
    except UniverseIntegrityError:
        return UniverseVerification(
            status="failed",
            campaign_id=campaign_id,
            errors=("checksums_invalid",),
            checked_files=0,
        )
    actual_files = {
        path.relative_to(root).as_posix(): path
        for path in root.rglob("*")
        if path.is_file() and path.name != "checksums.sha256"
    }
    if set(expected) != set(actual_files):
        errors.append("file_set_mismatch")
    for relative, digest in expected.items():
        path = actual_files.get(relative)
        if path is None or _sha256_file(path) != digest:
            errors.append("checksum_mismatch")
            break
    try:
        manifest = _read_campaign_manifest(root / "manifest.json")
        if manifest.campaign_id != campaign_id:
            errors.append("campaign_id_mismatch")
        if manifest.artifact_sha256 != {
            relative: digest
            for relative, digest in expected.items()
            if relative != "manifest.json"
        }:
            errors.append("artifact_manifest_mismatch")
        _verify_parquet_schema(
            root / "address_features", ADDRESS_FEATURE_SCHEMA
        )
        _verify_parquet_schema(root / "script_subjects", SCRIPT_SUBJECT_SCHEMA)
        _verify_parquet_schema(
            root / "source_accounting", SOURCE_ACCOUNTING_SCHEMA
        )
    except (UniverseIntegrityError, pa.ArrowException, OSError):
        errors.append("manifest_or_schema_invalid")
    return UniverseVerification(
        status="failed" if errors else "ok",
        campaign_id=campaign_id,
        errors=tuple(sorted(set(errors))),
        checked_files=len(actual_files),
    )


def _verify_parquet_schema(root: Path, expected: pa.Schema) -> None:
    paths = sorted(root.rglob("*.parquet"))
    if not paths:
        raise UniverseIntegrityError("required parquet data is missing")
    for path in paths:
        if not pq.read_schema(path).equals(expected):
            raise UniverseIntegrityError("parquet schema mismatch")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_tree(root: Path) -> None:
    for path in sorted(
        (item for item in root.rglob("*") if item.is_dir()),
        key=lambda item: len(item.parts),
        reverse=True,
    ):
        _fsync_directory(path)
    _fsync_directory(root)
