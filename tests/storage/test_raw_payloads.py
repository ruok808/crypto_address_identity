from __future__ import annotations

from pathlib import Path

import pytest

from crypto_address_identity.observations import SecretBoundaryError, build_observation_metadata
from crypto_address_identity.storage.raw_payloads import RawPayloadStore
from crypto_address_identity.storage.sqlite import IdentityDatabase


def test_persists_content_addressed_payload_and_metadata(runtime_root: Path) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    store = RawPayloadStore(database, runtime_root / "raw")

    payload = b'{"bitcoin":{"address":"fixture"}}'
    stored = store.persist(payload)

    assert len(stored.payload_sha256) == 64
    assert not Path(stored.relative_path).is_absolute()
    assert (runtime_root / "raw" / stored.relative_path).is_file()
    assert store.verify(stored.payload_sha256).status == "active"

    with database.read_connection() as connection:
        row = connection.execute(
            "SELECT relative_path, compression, byte_count FROM raw_payload_object"
        ).fetchone()
    assert row["relative_path"] == stored.relative_path
    assert row["compression"] == "gzip"
    assert row["byte_count"] == len(payload)


def test_identical_payload_deduplicates_object_storage(runtime_root: Path) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    store = RawPayloadStore(database, runtime_root / "raw")

    first = store.persist(b"fixture")
    second = store.persist(b"fixture")

    assert first == second
    with database.read_connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM raw_payload_object").fetchone()[0] == 1


def test_missing_raw_object_is_detected_without_exposing_path_content(runtime_root: Path) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    store = RawPayloadStore(database, runtime_root / "raw")
    stored = store.persist(b"fixture")
    (runtime_root / "raw" / stored.relative_path).unlink()

    verification = store.verify(stored.payload_sha256)

    assert verification.status == "missing"
    assert verification.payload_sha256 == stored.payload_sha256


@pytest.mark.parametrize(
    "endpoint_template",
    [
        "https://0xrouter.test/path?token=secret",
        "https://user:password@0xrouter.test/path",
        "/path?apikey=secret",
    ],
)
def test_observation_metadata_rejects_secret_bearing_endpoint(endpoint_template: str) -> None:
    with pytest.raises(SecretBoundaryError):
        build_observation_metadata(endpoint_template=endpoint_template, query_profile="discovery")


def test_observation_metadata_keeps_only_route_class() -> None:
    metadata = build_observation_metadata(
        endpoint_template="/chaindata/intelligence/address_enriched/{address}/all",
        query_profile="discovery",
    )

    assert metadata.endpoint_template.endswith("/{address}/all")
    assert metadata.query_profile == "discovery"
