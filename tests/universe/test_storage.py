from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from crypto_address_identity.universe.models import SourceManifest
from crypto_address_identity.universe.storage import (
    ADDRESS_FEATURE_SCHEMA,
    SCRIPT_SUBJECT_SCHEMA,
    UniverseIntegrityError,
    UniverseStore,
)
from tests.universe.conftest import (
    BTC_ADDRESSES,
    make_accounting,
    make_feature,
    make_script,
)


def publish_fixture(
    tmp_path: Path, manifest: SourceManifest
):
    store = UniverseStore(tmp_path / "universe")
    writer = store.begin_campaign(manifest)
    writer.write_address_features(
        [make_feature(BTC_ADDRESSES[0]), make_feature(BTC_ADDRESSES[1])]
    )
    writer.write_script_subjects(
        [
            make_script(BTC_ADDRESSES[0]),
            make_script(
                None,
                script_hex="6a01ff",
                script_type="op_return",
            ),
        ]
    )
    writer.write_source_accounting(make_accounting())
    return store, writer.publish()


def test_campaign_publish_is_atomic_and_checksum_pinned(
    tmp_path: Path, universe_source_manifest: SourceManifest
) -> None:
    store, published = publish_fixture(tmp_path, universe_source_manifest)

    assert published.root.name == "btc-20260724"
    assert published.manifest_sha256 == universe_source_manifest.manifest_sha256
    assert published.address_feature_rows == 2
    assert published.script_subject_rows == 2
    assert not any((tmp_path / "universe" / ".staging").iterdir())
    assert store.verify("btc-20260724").status == "ok"
    assert (published.root / "checksums.sha256").is_file()


@pytest.mark.parametrize("row_kind", ["address", "script"])
def test_campaign_rejects_duplicate_subject_ids(
    tmp_path: Path,
    universe_source_manifest: SourceManifest,
    row_kind: str,
) -> None:
    store = UniverseStore(tmp_path / "universe")
    writer = store.begin_campaign(universe_source_manifest)

    with pytest.raises(UniverseIntegrityError):
        if row_kind == "address":
            row = make_feature()
            writer.write_address_features([row, row])
        else:
            row = make_script()
            writer.write_script_subjects([row, row])

    assert not (tmp_path / "universe" / "campaigns" / "btc-20260724").exists()
    assert not any((tmp_path / "universe" / ".staging").iterdir())


def test_duplicate_tracking_is_disk_backed_and_not_published(
    tmp_path: Path, universe_source_manifest: SourceManifest
) -> None:
    store = UniverseStore(tmp_path / "universe")
    writer = store.begin_campaign(universe_source_manifest)

    assert writer.duplicate_tracker_path.is_file()
    assert not hasattr(writer, "_address_ids")
    writer.write_address_features([make_feature()])
    writer.write_script_subjects([make_script()])
    writer.write_source_accounting(make_accounting())
    published = writer.publish()

    assert not (published.root / ".dedupe.duckdb").exists()


def test_non_address_scripts_remain_in_parquet_with_null_identity(
    tmp_path: Path, universe_source_manifest: SourceManifest
) -> None:
    _, published = publish_fixture(tmp_path, universe_source_manifest)
    table = pa.concat_tables(
        [
            pq.read_table(path)
            for path in sorted(
                published.root.glob("script_subjects/prefix=*/*.parquet")
            )
        ]
    )
    records = table.to_pylist()
    non_address = next(row for row in records if row["script_type"] == "op_return")

    assert non_address["normalized_address"] is None
    assert non_address["address_id"] is None
    assert non_address["provider_enrichable"] is False


def test_campaign_rejects_invalid_raw_address_row(
    tmp_path: Path, universe_source_manifest: SourceManifest
) -> None:
    store = UniverseStore(tmp_path / "universe")
    writer = store.begin_campaign(universe_source_manifest)
    invalid = make_feature().model_dump(mode="python")
    invalid["normalized_address"] = "not-a-bitcoin-address"

    with pytest.raises(UniverseIntegrityError):
        writer.write_address_features([invalid])

    assert not (tmp_path / "universe" / "campaigns" / "btc-20260724").exists()


def test_campaign_verification_detects_changed_parquet_or_manifest(
    tmp_path: Path, universe_source_manifest: SourceManifest
) -> None:
    store, published = publish_fixture(tmp_path, universe_source_manifest)
    parquet_path = next(published.root.glob("address_features/prefix=*/*.parquet"))
    original = parquet_path.read_bytes()
    parquet_path.write_bytes(original + b"tampered")

    verification = store.verify("btc-20260724")

    assert verification.status == "failed"
    assert "checksum_mismatch" in verification.errors


def test_campaign_verification_detects_changed_manifest(
    tmp_path: Path, universe_source_manifest: SourceManifest
) -> None:
    store, published = publish_fixture(tmp_path, universe_source_manifest)
    manifest_path = published.root / "manifest.json"
    manifest_path.write_bytes(manifest_path.read_bytes() + b" ")

    verification = store.verify("btc-20260724")

    assert verification.status == "failed"
    assert "checksum_mismatch" in verification.errors


def test_failed_writer_never_creates_final_campaign(
    tmp_path: Path, universe_source_manifest: SourceManifest
) -> None:
    store = UniverseStore(tmp_path / "universe")
    writer = store.begin_campaign(universe_source_manifest)
    writer.write_address_features([make_feature()])

    with pytest.raises(UniverseIntegrityError):
        writer.publish()

    assert not (tmp_path / "universe" / "campaigns" / "btc-20260724").exists()


def test_parquet_files_use_declared_arrow_schemas(
    tmp_path: Path, universe_source_manifest: SourceManifest
) -> None:
    _, published = publish_fixture(tmp_path, universe_source_manifest)

    for path in published.root.glob("address_features/prefix=*/*.parquet"):
        assert pq.read_schema(path).equals(ADDRESS_FEATURE_SCHEMA)
    for path in published.root.glob("script_subjects/prefix=*/*.parquet"):
        assert pq.read_schema(path).equals(SCRIPT_SUBJECT_SCHEMA)


def test_duckdb_reads_campaign_without_identity_sqlite(
    tmp_path: Path, universe_source_manifest: SourceManifest
) -> None:
    _, published = publish_fixture(tmp_path, universe_source_manifest)

    with published.open_duckdb() as connection:
        feature_count = connection.execute(
            "SELECT count(*) FROM universe_btc_address_feature"
        ).fetchone()[0]
        script_count = connection.execute(
            "SELECT count(*) FROM universe_btc_script_subject"
        ).fetchone()[0]

    assert feature_count == 2
    assert script_count == 2
    assert not (tmp_path / "identity.sqlite3").exists()
