from __future__ import annotations

import json
from pathlib import Path

import pytest

from crypto_address_identity.evidence import EvidenceInput, EvidenceService, VerifierRegistry
from crypto_address_identity.exports import ExportCollisionError, ResolverExporter, ResolverSnapshot
from crypto_address_identity.resolver import ResolverService
from crypto_address_identity.storage.sqlite import IdentityDatabase


BTC_ADDRESS = "1BoatSLRHtKNngkdXEeobR76b53LETtpyT"


def _prepare_database(runtime_root: Path) -> IdentityDatabase:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    record = EvidenceInput.model_validate(
        {
            "chain_key": "bitcoin",
            "address": BTC_ADDRESS,
            "assertion_type": "entity_control",
            "candidate_entity_id": "arkham:one",
            "candidate_entity_name": "Example",
            "source_authority": "commercial_provider",
            "evidence_tier": "C",
            "verification_method": "api-observation",
            "source_url": "https://example.test/source",
            "artifact_sha256": "a" * 64,
            "license_ref": "fixture-license",
            "independence_group": "fixture-provider",
            "observed_at": "2026-07-22T00:00:00Z",
            "evidence_status": "valid",
            "imported_by": "fixture",
        }
    )
    EvidenceService(database, VerifierRegistry()).import_records([record])
    ResolverService(database).rebuild(as_of="2026-07-22T01:00:00Z")
    return database


def test_export_is_immutable_and_checksum_verifiable(runtime_root: Path) -> None:
    database = _prepare_database(runtime_root)
    exporter = ResolverExporter(database, runtime_root / "exports")

    result = exporter.export(chain_key="bitcoin", as_of="2026-07-22T01:00:00Z")
    snapshot = ResolverSnapshot.load(result.directory)

    assert result.resolution_count == 1
    assert len(result.manifest_sha256) == 64
    assert snapshot.manifest["chain_key"] == "bitcoin"
    assert snapshot.lookup("bitcoin", BTC_ADDRESS, "entity_control")["state"] == "resolved"


def test_corrupt_export_fails_checksum_validation(runtime_root: Path) -> None:
    database = _prepare_database(runtime_root)
    result = ResolverExporter(database, runtime_root / "exports").export(
        chain_key="bitcoin", as_of="2026-07-22T01:00:00Z"
    )
    (result.directory / "resolutions.ndjson").write_text("corrupt\n", encoding="utf-8")

    with pytest.raises(ValueError, match="checksum"):
        ResolverSnapshot.load(result.directory)


def test_same_as_of_export_is_idempotent_but_path_collision_fails(runtime_root: Path) -> None:
    database = _prepare_database(runtime_root)
    exporter = ResolverExporter(database, runtime_root / "exports")
    first = exporter.export(chain_key="bitcoin", as_of="2026-07-22T01:00:00Z")
    second = exporter.export(chain_key="bitcoin", as_of="2026-07-22T01:00:00Z")

    assert first.manifest_sha256 == second.manifest_sha256
    manifest = json.loads((first.directory / "manifest.json").read_text())
    manifest["as_of"] = "tampered"
    (first.directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ExportCollisionError):
        exporter.export(chain_key="bitcoin", as_of="2026-07-22T01:00:00Z")


def test_existing_snapshot_with_corrupt_data_is_not_silently_reused(runtime_root: Path) -> None:
    database = _prepare_database(runtime_root)
    exporter = ResolverExporter(database, runtime_root / "exports")
    first = exporter.export(chain_key="bitcoin", as_of="2026-07-22T01:00:00Z")
    (first.directory / "resolutions.ndjson").write_text("corrupt\n", encoding="utf-8")

    with pytest.raises(ExportCollisionError):
        exporter.export(chain_key="bitcoin", as_of="2026-07-22T01:00:00Z")


def test_dry_run_does_not_create_export_or_snapshot_record(runtime_root: Path) -> None:
    database = _prepare_database(runtime_root)
    exporter = ResolverExporter(database, runtime_root / "exports")

    result = exporter.export(chain_key="bitcoin", as_of="2026-07-22T01:00:00Z", dry_run=True)

    assert result.written is False
    assert not (runtime_root / "exports").exists()
    with database.read_connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM resolver_snapshot").fetchone()[0] == 0
