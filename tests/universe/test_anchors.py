from __future__ import annotations

import hashlib
import sqlite3
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest

from crypto_address_identity.chains.bitcoin import normalize_bitcoin_address
from crypto_address_identity.storage.sqlite import IdentityDatabase
from crypto_address_identity.universe.anchors import (
    AnchorIntegrityError,
    CalibrationAnchorReader,
)
from crypto_address_identity.universe.models import SourceManifest
from crypto_address_identity.universe.storage import UniverseStore
from tests.universe.conftest import (
    BTC_ADDRESSES,
    make_accounting,
    make_feature,
    make_script,
)


TIER_A_ADDRESS = BTC_ADDRESSES[0]
TIER_B_ADDRESS = BTC_ADDRESSES[1]
TIER_C_ONLY_ADDRESS = BTC_ADDRESSES[2]
ACTIVE_CONFLICT_ADDRESS = BTC_ADDRESSES[3]
WATCHLIST_ADDRESS = "bc1qs3njm2cnmj4s2nuk444vm9cfyxs8ktzqzsx2qh"
PREDICTED_ADDRESS = "bc1qu97pnw3arh9gslvt84r3h8rzv2q7ssaevaq5ay"
AS_OF = datetime(2026, 7, 24, tzinfo=UTC)


def populated_identity_database(tmp_path: Path) -> Path:
    path = tmp_path / "identity.sqlite3"
    database = IdentityDatabase(path)
    database.migrate()
    addresses = (
        TIER_A_ADDRESS,
        TIER_B_ADDRESS,
        TIER_C_ONLY_ADDRESS,
        ACTIVE_CONFLICT_ADDRESS,
        WATCHLIST_ADDRESS,
        PREDICTED_ADDRESS,
    )
    observed_at = "2026-07-20T00:00:00Z"
    with database.write_transaction() as connection:
        for address in addresses:
            subject = normalize_bitcoin_address(address)
            connection.execute(
                """
                INSERT INTO address_subject (
                    address_id, chain_key, normalized_address, display_address,
                    address_type, first_seen_at
                ) VALUES (?, 'bitcoin', ?, ?, ?, ?)
                """,
                (
                    subject.address_id,
                    subject.normalized_address,
                    subject.normalized_address,
                    subject.address_type,
                    observed_at,
                ),
            )
        for index, (address, tier, status, expires_at) in enumerate(
            (
                (TIER_A_ADDRESS, "A", "valid", None),
                (TIER_B_ADDRESS, "B", "valid", None),
                (TIER_C_ONLY_ADDRESS, "C", "valid", None),
                (TIER_C_ONLY_ADDRESS, "A", "stale", "2026-07-21T00:00:00Z"),
            )
        ):
            subject = normalize_bitcoin_address(address)
            connection.execute(
                """
                INSERT INTO identity_evidence (
                    evidence_id, evidence_fingerprint, address_id, observation_id,
                    assertion_type, candidate_entity_id, candidate_entity_name,
                    candidate_label, candidate_wallet_role, provider_entity_id,
                    provider_tag_id, source_authority, evidence_tier,
                    verification_method, verification_result, source_url,
                    artifact_sha256, license_ref, independence_group, asserted_at,
                    observed_at, effective_from, effective_to, expires_at,
                    evidence_status, imported_by
                ) VALUES (?, ?, ?, NULL, 'entity_control', ?, ?, NULL, NULL, NULL,
                          NULL, 'official', ?, 'fixture', ?, ?, NULL, 'fixture',
                          'fixture-panel', ?, ?, ?, NULL, ?, ?, 'fixture')
                """,
                (
                    str(uuid.uuid4()),
                    hashlib.sha256(f"evidence:{index}".encode()).hexdigest(),
                    subject.address_id,
                    f"entity-{index}",
                    f"Entity {index}",
                    tier,
                    "valid" if tier == "A" and status == "valid" else None,
                    f"https://example.test/evidence/{index}",
                    observed_at,
                    observed_at,
                    observed_at,
                    expires_at,
                    status,
                ),
            )

        conflict_subject = normalize_bitcoin_address(ACTIVE_CONFLICT_ADDRESS)
        connection.execute(
            """
            INSERT INTO conflict_set (
                conflict_set_id, address_id, assertion_type, created_at,
                resolved_at, status
            ) VALUES (?, ?, 'entity_control', ?, NULL, 'active')
            """,
            (str(uuid.uuid4()), conflict_subject.address_id, observed_at),
        )

        watchlist_subject = normalize_bitcoin_address(WATCHLIST_ADDRESS)
        connection.execute(
            """
            INSERT INTO candidate_request (
                candidate_request_id, address_id, reason, priority,
                source_reference, requested_at, created_at
            ) VALUES (?, ?, 'known_watchlist', 90, 'fixture:watchlist', ?, ?)
            """,
            (str(uuid.uuid4()), watchlist_subject.address_id, observed_at, observed_at),
        )

        predicted_subject = normalize_bitcoin_address(PREDICTED_ADDRESS)
        observation_id = str(uuid.uuid4())
        connection.execute(
            """
            INSERT INTO source_observation (
                observation_id, source_id, source_version, source_kind,
                endpoint_template, query_profile, requested_at, completed_at,
                http_status, outcome, response_bytes, payload_sha256,
                schema_fingerprint, chain_key, address_id, ingestion_run_id
            ) VALUES (?, 'fixture-provider', 'v1', 'provider', '/fixture',
                      'discovery', ?, ?, 200, 'success', 0, NULL, NULL,
                      'bitcoin', ?, NULL)
            """,
            (
                observation_id,
                observed_at,
                observed_at,
                predicted_subject.address_id,
            ),
        )
        connection.execute(
            """
            INSERT INTO coverage_entity_prediction (
                prediction_id, prediction_fingerprint, observation_id,
                provider_entity_id, address_id, prediction_rank, observed_at
            ) VALUES (?, ?, ?, 'fixture-entity', ?, 1, ?)
            """,
            (
                str(uuid.uuid4()),
                hashlib.sha256(b"prediction").hexdigest(),
                observation_id,
                predicted_subject.address_id,
                observed_at,
            ),
        )
    return path


def test_anchor_reader_exports_only_exact_strong_and_conflict_reasons(
    tmp_path: Path,
) -> None:
    database_path = populated_identity_database(tmp_path)
    snapshot = CalibrationAnchorReader(database_path).read(as_of=AS_OF)

    expected = sorted(
        (
            (
                normalize_bitcoin_address(ACTIVE_CONFLICT_ADDRESS).address_id,
                "existing_provider_conflict",
            ),
            (
                normalize_bitcoin_address(PREDICTED_ADDRESS).address_id,
                "provider_entity_prediction",
            ),
            (
                normalize_bitcoin_address(TIER_A_ADDRESS).address_id,
                "official_or_signed_evidence",
            ),
            (
                normalize_bitcoin_address(TIER_B_ADDRESS).address_id,
                "official_or_signed_evidence",
            ),
            (
                normalize_bitcoin_address(WATCHLIST_ADDRESS).address_id,
                "existing_system_watchlist",
            ),
        )
    )

    assert [(row.address_id, row.reason_code) for row in snapshot.rows] == expected
    assert snapshot.database_sha256
    assert snapshot.snapshot_sha256
    serialized = snapshot.model_dump_json()
    assert "source_url" not in serialized
    assert "Entity " not in serialized
    assert TIER_C_ONLY_ADDRESS not in serialized


def test_anchor_reader_uses_immutable_read_only_sqlite_uri(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = populated_identity_database(tmp_path)
    real_connect = sqlite3.connect
    captured: list[tuple[str, bool]] = []

    def tracked_connect(database: str, *args: object, **kwargs: object):
        captured.append((database, bool(kwargs.get("uri"))))
        return real_connect(database, *args, **kwargs)

    monkeypatch.setattr(
        "crypto_address_identity.universe.anchors.sqlite3.connect", tracked_connect
    )

    CalibrationAnchorReader(database_path).read(as_of=AS_OF)

    assert len(captured) == 1
    assert "mode=ro" in captured[0][0]
    assert "immutable=1" in captured[0][0]
    assert captured[0][1] is True


def test_anchor_reader_fails_when_database_changes_during_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = populated_identity_database(tmp_path)
    hashes = iter(("01" * 32, "02" * 32))
    monkeypatch.setattr(
        "crypto_address_identity.universe.anchors._sha256_file",
        lambda path: next(hashes),
    )

    with pytest.raises(AnchorIntegrityError):
        CalibrationAnchorReader(database_path).read(as_of=AS_OF)


def test_anchor_reader_rejects_nonempty_uncheckpointed_wal(
    tmp_path: Path,
) -> None:
    database_path = populated_identity_database(tmp_path)
    Path(f"{database_path}-wal").write_bytes(b"uncheckpointed")

    with pytest.raises(AnchorIntegrityError):
        CalibrationAnchorReader(database_path).read(as_of=AS_OF)


def test_anchor_snapshot_is_deterministic(tmp_path: Path) -> None:
    database_path = populated_identity_database(tmp_path)
    reader = CalibrationAnchorReader(database_path)

    first = reader.read(as_of=AS_OF)
    second = reader.read(as_of=AS_OF)

    assert first.snapshot_sha256 == second.snapshot_sha256
    assert first.rows == second.rows


def test_anchor_snapshot_is_checksum_pinned_inside_campaign(
    tmp_path: Path, universe_source_manifest: SourceManifest
) -> None:
    database_path = populated_identity_database(tmp_path)
    snapshot = CalibrationAnchorReader(database_path).read(as_of=AS_OF)
    store = UniverseStore(tmp_path / "universe")
    writer = store.begin_campaign(universe_source_manifest)
    writer.write_address_features([make_feature()])
    writer.write_script_subjects([make_script()])
    writer.write_source_accounting(make_accounting(distinct_script_subjects=1))
    writer.write_calibration_anchor_snapshot(snapshot)

    published = writer.publish()

    assert published.campaign_manifest.calibration_anchor_rows == 5
    assert (
        published.campaign_manifest.calibration_anchor_database_sha256
        == snapshot.database_sha256
    )
    assert (
        published.campaign_manifest.calibration_anchor_snapshot_sha256
        == snapshot.snapshot_sha256
    )
    assert published.campaign_manifest.calibration_anchor_as_of == AS_OF
    metadata = (
        published.root / "calibration_anchors" / "metadata.json"
    ).read_text(encoding="ascii")
    assert snapshot.snapshot_sha256 in metadata
    assert TIER_A_ADDRESS not in metadata
    with published.open_duckdb() as connection:
        assert connection.execute(
            "SELECT count(*) FROM universe_btc_calibration_anchor"
        ).fetchone()[0] == 5
    assert store.verify(published.campaign_id).status == "ok"
