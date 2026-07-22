from __future__ import annotations

import sqlite3

import pytest

from crypto_address_identity.evidence import EvidenceInput, EvidenceService, VerifierRegistry
from crypto_address_identity.storage.sqlite import IdentityDatabase, MigrationError


def test_migrate_creates_all_contract_tables(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")

    database.migrate()

    with database.read_connection() as connection:
        tables = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        chains = connection.execute(
            "SELECT chain_key, enabled FROM chain_registry ORDER BY chain_key"
        ).fetchall()

    assert {
        "schema_migration",
        "chain_registry",
        "address_subject",
        "candidate_request",
        "candidate_attempt",
        "ingestion_run",
        "request_reservation",
        "source_observation",
        "raw_payload_object",
        "identity_evidence",
        "identity_claim",
        "conflict_set",
        "conflict_member",
        "identity_resolution",
        "resolver_snapshot",
    } <= tables
    assert dict((row["chain_key"], row["enabled"]) for row in chains)["bitcoin"] == 1


def test_migration_is_repeatable_and_detects_changed_historic_checksum(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    database.migrate()

    with database.write_transaction() as connection:
        connection.execute("UPDATE schema_migration SET checksum = 'tampered'")

    with pytest.raises(MigrationError, match="checksum"):
        database.migrate()


def test_foreign_keys_are_enforced(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()

    with pytest.raises(sqlite3.IntegrityError):
        with database.write_transaction() as connection:
            connection.execute(
                """
                INSERT INTO candidate_request (
                    candidate_request_id, address_id, reason, priority,
                    source_reference, requested_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("candidate-1", "absent", "manual_review", 50, "fixture", "2026-07-22T00:00:00Z", "2026-07-22T00:00:00Z"),
            )


def test_read_connection_is_read_only(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()

    with database.read_connection() as connection:
        with pytest.raises(sqlite3.OperationalError):
            connection.execute("INSERT INTO chain_registry VALUES ('x', 'x', 'x', 0, 'v1')")


def test_identity_evidence_is_append_only(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    record = EvidenceInput.model_validate(
        {
            "chain_key": "bitcoin",
            "address": "1BoatSLRHtKNngkdXEeobR76b53LETtpyT",
            "assertion_type": "entity_control",
            "candidate_entity_id": "arkham:fixture",
            "source_authority": "commercial_provider",
            "evidence_tier": "C",
            "verification_method": "api-observation",
            "source_url": "https://example.test/source",
            "artifact_sha256": "a" * 64,
            "license_ref": "fixture-license",
            "independence_group": "fixture",
            "observed_at": "2026-07-22T00:00:00Z",
            "imported_by": "fixture",
        }
    )
    EvidenceService(database, VerifierRegistry()).import_records([record])

    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        with database.write_transaction() as connection:
            connection.execute("UPDATE identity_evidence SET evidence_status = 'stale'")
