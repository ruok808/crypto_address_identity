"""Checksummed SQLite migrations and connection boundaries."""

from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote


class MigrationError(RuntimeError):
    """Raised when the immutable migration history is invalid."""


@dataclass(frozen=True)
class Migration:
    migration_id: str
    sql: str

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode("utf-8")).hexdigest()


_MIGRATION_001_SQL = """
CREATE TABLE chain_registry (
    chain_key TEXT PRIMARY KEY,
    canonical_id TEXT NOT NULL UNIQUE,
    family TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    normalizer_version TEXT NOT NULL
);

CREATE TABLE address_subject (
    address_id TEXT PRIMARY KEY,
    chain_key TEXT NOT NULL REFERENCES chain_registry(chain_key),
    normalized_address TEXT NOT NULL,
    display_address TEXT NOT NULL,
    address_type TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    UNIQUE (chain_key, normalized_address)
);

CREATE TABLE ingestion_run (
    ingestion_run_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL CHECK (mode IN ('dry_run', 'execute')),
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'partial', 'blocked', 'failed')),
    started_at TEXT NOT NULL,
    completed_at TEXT,
    request_limit INTEGER NOT NULL,
    response_bytes_budget INTEGER NOT NULL,
    request_count INTEGER NOT NULL DEFAULT 0,
    response_bytes_received INTEGER NOT NULL DEFAULT 0,
    summary_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE candidate_request (
    candidate_request_id TEXT PRIMARY KEY,
    address_id TEXT NOT NULL REFERENCES address_subject(address_id),
    reason TEXT NOT NULL CHECK (reason IN ('known_watchlist', 'whale_counterparty', 'transfer_counterparty', 'official_evidence', 'manual_review', 'replay')),
    priority INTEGER NOT NULL CHECK (priority BETWEEN 0 AND 100),
    source_reference TEXT NOT NULL,
    requested_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX candidate_request_by_address ON candidate_request(address_id, requested_at);
CREATE INDEX candidate_request_priority ON candidate_request(priority DESC, requested_at ASC);

CREATE TABLE candidate_attempt (
    candidate_attempt_id TEXT PRIMARY KEY,
    candidate_request_id TEXT NOT NULL REFERENCES candidate_request(candidate_request_id),
    ingestion_run_id TEXT REFERENCES ingestion_run(ingestion_run_id),
    profile TEXT CHECK (profile IN ('discovery', 'detail')),
    outcome TEXT NOT NULL,
    attempted_at TEXT NOT NULL,
    detail_reason TEXT,
    observation_id TEXT
);

CREATE INDEX candidate_attempt_by_request ON candidate_attempt(candidate_request_id, attempted_at DESC);

CREATE TABLE request_reservation (
    request_reservation_id TEXT PRIMARY KEY,
    ingestion_run_id TEXT REFERENCES ingestion_run(ingestion_run_id),
    reserved_at TEXT NOT NULL,
    rolling_window_start TEXT NOT NULL,
    request_limit INTEGER NOT NULL,
    response_bytes_budget INTEGER NOT NULL,
    estimated_response_bytes INTEGER NOT NULL DEFAULT 0,
    actual_response_bytes INTEGER,
    outcome TEXT NOT NULL CHECK (outcome IN ('reserved', 'dispatched', 'completed', 'failed', 'rate_limited', 'budget_exhausted'))
);

CREATE INDEX request_reservation_window ON request_reservation(reserved_at, outcome);

CREATE TABLE raw_payload_object (
    payload_sha256 TEXT PRIMARY KEY,
    relative_path TEXT NOT NULL UNIQUE,
    compression TEXT NOT NULL,
    byte_count INTEGER NOT NULL CHECK (byte_count >= 0),
    retention_status TEXT NOT NULL CHECK (retention_status IN ('active', 'expired', 'missing')),
    created_at TEXT NOT NULL
);

CREATE TABLE source_observation (
    observation_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    source_version TEXT NOT NULL,
    source_kind TEXT NOT NULL CHECK (source_kind IN ('provider', 'import')),
    endpoint_template TEXT NOT NULL,
    query_profile TEXT NOT NULL,
    requested_at TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    http_status INTEGER,
    outcome TEXT NOT NULL,
    response_bytes INTEGER NOT NULL DEFAULT 0 CHECK (response_bytes >= 0),
    payload_sha256 TEXT REFERENCES raw_payload_object(payload_sha256),
    schema_fingerprint TEXT,
    chain_key TEXT NOT NULL REFERENCES chain_registry(chain_key),
    address_id TEXT REFERENCES address_subject(address_id),
    ingestion_run_id TEXT REFERENCES ingestion_run(ingestion_run_id)
);

CREATE INDEX source_observation_subject ON source_observation(address_id, completed_at DESC);
CREATE INDEX source_observation_payload ON source_observation(payload_sha256);

CREATE TABLE identity_evidence (
    evidence_id TEXT PRIMARY KEY,
    evidence_fingerprint TEXT NOT NULL UNIQUE,
    address_id TEXT NOT NULL REFERENCES address_subject(address_id),
    observation_id TEXT REFERENCES source_observation(observation_id),
    assertion_type TEXT NOT NULL CHECK (assertion_type IN ('entity_control', 'address_label', 'wallet_role', 'address_kind', 'relationship')),
    candidate_entity_id TEXT,
    candidate_entity_name TEXT,
    candidate_label TEXT,
    candidate_wallet_role TEXT,
    provider_entity_id TEXT,
    provider_tag_id TEXT,
    source_authority TEXT NOT NULL,
    evidence_tier TEXT NOT NULL CHECK (evidence_tier IN ('A', 'B', 'C', 'D', 'E')),
    verification_method TEXT NOT NULL,
    verification_result TEXT,
    source_url TEXT NOT NULL,
    artifact_sha256 TEXT,
    license_ref TEXT NOT NULL,
    independence_group TEXT NOT NULL,
    asserted_at TEXT,
    observed_at TEXT NOT NULL,
    effective_from TEXT,
    effective_to TEXT,
    expires_at TEXT,
    evidence_status TEXT NOT NULL CHECK (evidence_status IN ('valid', 'stale', 'revoked', 'disputed', 'superseded')),
    imported_by TEXT NOT NULL
);

CREATE INDEX identity_evidence_subject ON identity_evidence(address_id, assertion_type, observed_at DESC);

CREATE TABLE identity_claim (
    claim_id TEXT PRIMARY KEY,
    claim_fingerprint TEXT NOT NULL UNIQUE,
    address_id TEXT NOT NULL REFERENCES address_subject(address_id),
    assertion_type TEXT NOT NULL,
    asserted_value TEXT NOT NULL,
    entity_id TEXT,
    claim_status TEXT NOT NULL CHECK (claim_status IN ('unreviewed_external', 'accepted', 'contested', 'rejected', 'deprecated', 'expired')),
    evidence_strength TEXT NOT NULL,
    corroboration_count INTEGER NOT NULL CHECK (corroboration_count >= 0),
    independence_count INTEGER NOT NULL CHECK (independence_count >= 0),
    effective_from TEXT,
    effective_to TEXT,
    reviewed_at TEXT,
    reviewer_ref TEXT,
    supersedes_claim_id TEXT REFERENCES identity_claim(claim_id),
    created_at TEXT NOT NULL
);

CREATE INDEX identity_claim_subject ON identity_claim(address_id, assertion_type, created_at DESC);

CREATE TABLE conflict_set (
    conflict_set_id TEXT PRIMARY KEY,
    address_id TEXT NOT NULL REFERENCES address_subject(address_id),
    assertion_type TEXT NOT NULL,
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    status TEXT NOT NULL CHECK (status IN ('active', 'resolved'))
);

CREATE UNIQUE INDEX active_conflict_per_subject ON conflict_set(address_id, assertion_type) WHERE status = 'active';

CREATE TABLE conflict_member (
    conflict_set_id TEXT NOT NULL REFERENCES conflict_set(conflict_set_id),
    claim_id TEXT NOT NULL REFERENCES identity_claim(claim_id),
    added_at TEXT NOT NULL,
    PRIMARY KEY (conflict_set_id, claim_id)
);

CREATE TABLE identity_resolution (
    resolution_id TEXT PRIMARY KEY,
    resolution_fingerprint TEXT NOT NULL UNIQUE,
    address_id TEXT NOT NULL REFERENCES address_subject(address_id),
    assertion_type TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('resolved', 'ambiguous', 'unattributed', 'stale', 'unsupported')),
    primary_claim_id TEXT REFERENCES identity_claim(claim_id),
    candidate_claim_ids_json TEXT NOT NULL,
    operational_tier TEXT NOT NULL CHECK (operational_tier IN ('none', 'discovery_only', 'lookup_only', 'lookup_usable')),
    conflict_set_id TEXT REFERENCES conflict_set(conflict_set_id),
    resolved_at TEXT NOT NULL,
    resolution_version TEXT NOT NULL,
    freshness_status TEXT NOT NULL,
    UNIQUE (address_id, assertion_type, resolution_version)
);

CREATE TABLE resolver_snapshot (
    snapshot_id TEXT PRIMARY KEY,
    chain_key TEXT NOT NULL REFERENCES chain_registry(chain_key),
    resolver_version TEXT NOT NULL,
    as_of TEXT NOT NULL,
    relative_path TEXT NOT NULL UNIQUE,
    manifest_sha256 TEXT NOT NULL UNIQUE,
    resolution_count INTEGER NOT NULL CHECK (resolution_count >= 0),
    evidence_summary_count INTEGER NOT NULL CHECK (evidence_summary_count >= 0),
    created_at TEXT NOT NULL
);

INSERT INTO chain_registry (chain_key, canonical_id, family, enabled, normalizer_version) VALUES
    ('bitcoin', 'bitcoin:mainnet', 'utxo', 1, 'bitcoin_v1'),
    ('ethereum', 'eip155:1', 'evm', 0, 'disabled_v1'),
    ('bsc', 'eip155:56', 'evm', 0, 'disabled_v1'),
    ('solana', 'solana:mainnet', 'solana', 0, 'disabled_v1'),
    ('zcash', 'zcash:mainnet', 'utxo_privacy', 0, 'disabled_v1');
"""

_MIGRATION_002_SQL = """
CREATE TABLE claim_review (
    review_id TEXT PRIMARY KEY,
    review_fingerprint TEXT NOT NULL UNIQUE,
    address_id TEXT NOT NULL REFERENCES address_subject(address_id),
    assertion_type TEXT NOT NULL,
    asserted_value TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('accept', 'reject')),
    reviewer_ref TEXT NOT NULL,
    reviewed_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX claim_review_lookup ON claim_review(
    address_id, assertion_type, asserted_value, reviewed_at DESC
);
"""

_MIGRATION_003_SQL = """
CREATE TRIGGER source_observation_no_update
BEFORE UPDATE ON source_observation
BEGIN
    SELECT RAISE(ABORT, 'source_observation is immutable');
END;

CREATE TRIGGER source_observation_no_delete
BEFORE DELETE ON source_observation
BEGIN
    SELECT RAISE(ABORT, 'source_observation is immutable');
END;

CREATE TRIGGER identity_evidence_no_update
BEFORE UPDATE ON identity_evidence
BEGIN
    SELECT RAISE(ABORT, 'identity_evidence is immutable');
END;

CREATE TRIGGER identity_evidence_no_delete
BEFORE DELETE ON identity_evidence
BEGIN
    SELECT RAISE(ABORT, 'identity_evidence is immutable');
END;

CREATE TRIGGER identity_claim_no_update
BEFORE UPDATE ON identity_claim
BEGIN
    SELECT RAISE(ABORT, 'identity_claim is immutable');
END;

CREATE TRIGGER identity_claim_no_delete
BEFORE DELETE ON identity_claim
BEGIN
    SELECT RAISE(ABORT, 'identity_claim is immutable');
END;

CREATE TRIGGER identity_resolution_no_update
BEFORE UPDATE ON identity_resolution
BEGIN
    SELECT RAISE(ABORT, 'identity_resolution is immutable');
END;

CREATE TRIGGER identity_resolution_no_delete
BEFORE DELETE ON identity_resolution
BEGIN
    SELECT RAISE(ABORT, 'identity_resolution is immutable');
END;
"""

_MIGRATION_004_SQL = """
CREATE TABLE resolver_local_override (
    override_id TEXT PRIMARY KEY,
    override_fingerprint TEXT NOT NULL UNIQUE,
    address_id TEXT NOT NULL REFERENCES address_subject(address_id),
    assertion_type TEXT NOT NULL,
    asserted_value TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('select', 'reject')),
    reviewer_ref TEXT NOT NULL,
    reason_ref TEXT NOT NULL,
    reviewed_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX resolver_local_override_lookup ON resolver_local_override(
    address_id, assertion_type, asserted_value, reviewed_at DESC
);

CREATE TRIGGER resolver_local_override_no_update
BEFORE UPDATE ON resolver_local_override
BEGIN
    SELECT RAISE(ABORT, 'resolver_local_override is immutable');
END;

CREATE TRIGGER resolver_local_override_no_delete
BEFORE DELETE ON resolver_local_override
BEGIN
    SELECT RAISE(ABORT, 'resolver_local_override is immutable');
END;

ALTER TABLE identity_resolution
    ADD COLUMN resolution_policy TEXT NOT NULL DEFAULT 'legacy_conservative';

ALTER TABLE identity_resolution
    ADD COLUMN primary_entity_display TEXT;
"""

_MIGRATION_005_SQL = """
CREATE TABLE coverage_entity_seed (
    seed_id TEXT PRIMARY KEY,
    seed_fingerprint TEXT NOT NULL UNIQUE,
    provider_entity_id TEXT NOT NULL,
    priority INTEGER NOT NULL CHECK (priority BETWEEN 0 AND 100),
    source_reference TEXT NOT NULL,
    requested_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX coverage_entity_seed_priority ON coverage_entity_seed(
    priority DESC, requested_at ASC, created_at ASC
);

CREATE TABLE coverage_entity_observation (
    entity_observation_id TEXT PRIMARY KEY,
    entity_observation_fingerprint TEXT NOT NULL UNIQUE,
    observation_id TEXT NOT NULL REFERENCES source_observation(observation_id),
    provider_entity_id TEXT NOT NULL,
    provider_entity_name TEXT,
    provider_entity_type TEXT,
    discovery_method TEXT NOT NULL CHECK (
        discovery_method IN ('balance_changes', 'entity_detail', 'seed')
    ),
    discovery_rank INTEGER,
    observed_at TEXT NOT NULL
);

CREATE INDEX coverage_entity_observation_lookup ON coverage_entity_observation(
    provider_entity_id, observed_at DESC
);

CREATE TABLE coverage_entity_prediction (
    prediction_id TEXT PRIMARY KEY,
    prediction_fingerprint TEXT NOT NULL UNIQUE,
    observation_id TEXT NOT NULL REFERENCES source_observation(observation_id),
    provider_entity_id TEXT NOT NULL,
    address_id TEXT NOT NULL REFERENCES address_subject(address_id),
    prediction_rank INTEGER,
    observed_at TEXT NOT NULL
);

CREATE INDEX coverage_entity_prediction_entity ON coverage_entity_prediction(
    provider_entity_id, observed_at DESC
);
CREATE INDEX coverage_entity_prediction_address ON coverage_entity_prediction(
    address_id, observed_at DESC
);

CREATE TRIGGER coverage_entity_seed_no_update
BEFORE UPDATE ON coverage_entity_seed
BEGIN
    SELECT RAISE(ABORT, 'coverage_entity_seed is immutable');
END;

CREATE TRIGGER coverage_entity_seed_no_delete
BEFORE DELETE ON coverage_entity_seed
BEGIN
    SELECT RAISE(ABORT, 'coverage_entity_seed is immutable');
END;

CREATE TRIGGER coverage_entity_observation_no_update
BEFORE UPDATE ON coverage_entity_observation
BEGIN
    SELECT RAISE(ABORT, 'coverage_entity_observation is immutable');
END;

CREATE TRIGGER coverage_entity_observation_no_delete
BEFORE DELETE ON coverage_entity_observation
BEGIN
    SELECT RAISE(ABORT, 'coverage_entity_observation is immutable');
END;

CREATE TRIGGER coverage_entity_prediction_no_update
BEFORE UPDATE ON coverage_entity_prediction
BEGIN
    SELECT RAISE(ABORT, 'coverage_entity_prediction is immutable');
END;

CREATE TRIGGER coverage_entity_prediction_no_delete
BEFORE DELETE ON coverage_entity_prediction
BEGIN
    SELECT RAISE(ABORT, 'coverage_entity_prediction is immutable');
END;
"""

_MIGRATION_006_SQL = """
CREATE TABLE coverage_address_parse_result (
    parse_result_id TEXT PRIMARY KEY,
    parse_result_fingerprint TEXT NOT NULL UNIQUE,
    observation_id TEXT NOT NULL REFERENCES source_observation(observation_id),
    address_id TEXT NOT NULL REFERENCES address_subject(address_id),
    parse_outcome TEXT NOT NULL CHECK (parse_outcome IN ('parsed_success', 'malformed_payload')),
    parsed_at TEXT NOT NULL
);

CREATE INDEX coverage_address_parse_result_lookup ON coverage_address_parse_result(
    address_id, parse_outcome, parsed_at DESC
);

CREATE TRIGGER coverage_address_parse_result_no_update
BEFORE UPDATE ON coverage_address_parse_result
BEGIN
    SELECT RAISE(ABORT, 'coverage_address_parse_result is immutable');
END;

CREATE TRIGGER coverage_address_parse_result_no_delete
BEFORE DELETE ON coverage_address_parse_result
BEGIN
    SELECT RAISE(ABORT, 'coverage_address_parse_result is immutable');
END;
"""

_MIGRATION_007_SQL = """
CREATE TABLE coverage_entity_prediction_parse_result (
    prediction_parse_result_id TEXT PRIMARY KEY,
    prediction_parse_result_fingerprint TEXT NOT NULL UNIQUE,
    observation_id TEXT NOT NULL REFERENCES source_observation(observation_id),
    provider_entity_id TEXT NOT NULL,
    parse_outcome TEXT NOT NULL CHECK (
        parse_outcome IN ('parsed_success', 'no_bitcoin_addresses', 'malformed_payload')
    ),
    parsed_at TEXT NOT NULL
);

CREATE INDEX coverage_entity_prediction_parse_result_lookup
ON coverage_entity_prediction_parse_result(
    provider_entity_id, parse_outcome, parsed_at DESC
);

CREATE TRIGGER coverage_entity_prediction_parse_result_no_update
BEFORE UPDATE ON coverage_entity_prediction_parse_result
BEGIN
    SELECT RAISE(ABORT, 'coverage_entity_prediction_parse_result is immutable');
END;

CREATE TRIGGER coverage_entity_prediction_parse_result_no_delete
BEFORE DELETE ON coverage_entity_prediction_parse_result
BEGIN
    SELECT RAISE(ABORT, 'coverage_entity_prediction_parse_result is immutable');
END;
"""

MIGRATIONS: tuple[Migration, ...] = (
    Migration("001_initial_ledger", _MIGRATION_001_SQL),
    Migration("002_claim_review", _MIGRATION_002_SQL),
    Migration("003_append_only_identity", _MIGRATION_003_SQL),
    Migration("004_provider_default_resolution", _MIGRATION_004_SQL),
    Migration("005_coverage_sync", _MIGRATION_005_SQL),
    Migration("006_coverage_address_parse_result", _MIGRATION_006_SQL),
    Migration("007_coverage_entity_prediction_parse_result", _MIGRATION_007_SQL),
)


class IdentityDatabase:
    """SQLite database wrapper with explicit read/write boundaries."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def migrate(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = self._connect(read_only=False)
        try:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migration (
                    migration_id TEXT PRIMARY KEY,
                    checksum TEXT NOT NULL,
                    applied_at TEXT NOT NULL
                )
                """
            )
            existing_rows = connection.execute(
                "SELECT migration_id, checksum FROM schema_migration"
            ).fetchall()
            expected = {migration.migration_id: migration for migration in MIGRATIONS}
            for row in existing_rows:
                migration = expected.get(row["migration_id"])
                if migration is None:
                    raise MigrationError(f"Unknown applied migration: {row['migration_id']}")
                if row["checksum"] != migration.checksum:
                    raise MigrationError(f"Migration checksum mismatch: {migration.migration_id}")

            for migration in MIGRATIONS:
                already_applied = any(
                    row["migration_id"] == migration.migration_id for row in existing_rows
                )
                if already_applied:
                    continue
                applied_at = _utc_now()
                statement = (
                    "BEGIN IMMEDIATE;\n"
                    f"{migration.sql}\n"
                    "INSERT INTO schema_migration (migration_id, checksum, applied_at) VALUES "
                    f"('{migration.migration_id}', '{migration.checksum}', '{applied_at}');\n"
                    "COMMIT;"
                )
                connection.executescript(statement)
        finally:
            connection.close()

    @contextmanager
    def write_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect(read_only=False)
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    @contextmanager
    def read_connection(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect(read_only=True)
        try:
            yield connection
        finally:
            connection.close()

    def _connect(self, *, read_only: bool) -> sqlite3.Connection:
        if read_only:
            uri = f"file:{quote(str(self.path.resolve()), safe='/')}?mode=ro"
            connection = sqlite3.connect(uri, uri=True, timeout=5.0)
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        if not read_only:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
        return connection


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")
