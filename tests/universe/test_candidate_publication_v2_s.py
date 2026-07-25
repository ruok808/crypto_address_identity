from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from crypto_address_identity.universe.candidate_materialization_v2_s import (
    PINNED_STRICT_V2_S_QUERY_SHA256,
    STRICT_V2_S_CANDIDATE_SCHEMA_SHA256,
)
from crypto_address_identity.universe.candidate_publication_v2_s import (
    CandidateArtifactExpectedCounts,
    CandidatePublicationError,
    CandidateSubjectExclusionContract,
    StrictV2SCandidateArtifactPublisher,
    StrictV2SCandidatePublicationRequest,
    candidate_address_bucket,
    candidate_row_sha256,
)


CUTOFF = datetime(2026, 7, 24, 23, 59, 59, 999999, tzinfo=UTC)
TABLE_ID = (
    "cai-btc-universe-20260724.cai_private."
    "btc_strict_v2_s_candidates_959187"
)
NO_SUBJECT_EXCLUSIONS = CandidateSubjectExclusionContract(subjects=())


def _candidate(
    *,
    address: str,
    tier: str,
) -> dict[str, object]:
    values: dict[str, object] = {
        "normalized_address": address,
        "candidate_tier": tier,
        "tier_rank": {
            "p0": 0,
            "p1": 1,
            "edge": 2,
            "coarse_other": 3,
        }[tier],
        "address_bucket": candidate_address_bucket(address),
        "v2_chain_score": 0,
        "strict_p0_mask": 0,
        "receipt_support_mask": 0,
        "current_utxo_sats": Decimal(0),
        "lifetime_received_sats": Decimal(100_000_000),
        "residual_gross_90d_sats": Decimal(0),
        "max_same_tx_received_lifetime_sats": Decimal(0),
        "max_same_tx_received_365d_sats": Decimal(0),
        "max_same_tx_received_90d_sats": Decimal(0),
        "same_tx_receive_ge_500_btc_90d_count": 0,
        "same_tx_receive_ge_500_btc_365d_count": 0,
        "active_tx_90d_count": 0,
        "active_day_90d_count": 0,
        "active_tx_365d_count": 0,
        "active_day_365d_count": 0,
        "last_seen_time": CUTOFF,
    }
    if tier == "p0":
        values["current_utxo_sats"] = Decimal(10_000_000_000)
        values["lifetime_received_sats"] = Decimal(10_000_000_000)
        values["v2_chain_score"] = 30
        values["strict_p0_mask"] = 1
    elif tier == "p1":
        values["current_utxo_sats"] = Decimal(1_000_000_000)
        values["lifetime_received_sats"] = Decimal(1_000_000_000)
        values["residual_gross_90d_sats"] = Decimal(10_000_000_000)
        values["v2_chain_score"] = 30
        values["active_tx_90d_count"] = 3
        values["active_day_90d_count"] = 2
        values["active_tx_365d_count"] = 3
        values["active_day_365d_count"] = 2
    elif tier == "edge":
        values["current_utxo_sats"] = Decimal(100_000_000)
        values["lifetime_received_sats"] = Decimal(100_000_000)
        values["v2_chain_score"] = 15
    else:
        values["last_seen_time"] = datetime(2024, 1, 1, tzinfo=UTC)
        values["current_utxo_sats"] = Decimal(100_000_000)
        values["lifetime_received_sats"] = Decimal(100_000_000)
        values["v2_chain_score"] = 5
    values["candidate_row_sha256"] = candidate_row_sha256(values)
    return values


class FakeCandidateTableBackend:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.calls: list[str] = []

    def destination_metadata(self, table_id: str):
        self.calls.append("destination_metadata")
        return {
            "table_id": table_id,
            "result_schema_sha256": STRICT_V2_S_CANDIDATE_SCHEMA_SHA256,
            "row_count": len(self.rows),
        }

    def stream_destination_arrow_batches(
        self,
        *,
        table_id: str,
        page_size: int,
    ):
        self.calls.append("stream_destination_arrow_batches")
        midpoint = max(1, len(self.rows) // 2)
        for rows in (self.rows[:midpoint], self.rows[midpoint:]):
            if rows:
                yield pa.RecordBatch.from_pylist(rows)


class OversizedCandidateTableBackend(FakeCandidateTableBackend):
    def stream_destination_arrow_batches(
        self,
        *,
        table_id: str,
        page_size: int,
    ):
        self.calls.append("stream_destination_arrow_batches")
        yield pa.RecordBatch.from_pylist(self.rows)


class MutatingReceiptBackend(FakeCandidateTableBackend):
    def __init__(
        self,
        rows: list[dict[str, object]],
        receipt_path: Path,
    ) -> None:
        super().__init__(rows)
        self.receipt_path = receipt_path

    def destination_metadata(self, table_id: str):
        metadata = super().destination_metadata(table_id)
        self.receipt_path.write_text("{}\n", encoding="ascii")
        self.receipt_path.chmod(0o600)
        return metadata


def _execution_receipt(path: Path, *, candidate_rows: int = 4) -> str:
    payload = {
        "schema_version": (
            "btc_strict_v2_s_materialization_execution_receipt_v1"
        ),
        "status": "completed",
        "job_id": "cai_btc_v2s_123",
        "destination_table_id": TABLE_ID,
        "query_sha256": PINNED_STRICT_V2_S_QUERY_SHA256,
        "result_schema_sha256": STRICT_V2_S_CANDIDATE_SCHEMA_SHA256,
        "candidate_rows": candidate_rows,
        "candidate_materialized": True,
    }
    encoded = (
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("ascii")
    path.write_bytes(encoded)
    path.chmod(0o600)
    return hashlib.sha256(encoded).hexdigest()


def _request(
    tmp_path: Path,
    *,
    receipt_sha256: str,
) -> StrictV2SCandidatePublicationRequest:
    return StrictV2SCandidatePublicationRequest(
        campaign_id="btc-v2s-bootstrap-959187-test",
        destination_table_id=TABLE_ID,
        source_execution_receipt_path=tmp_path / "execution.json",
        expected_execution_receipt_sha256=receipt_sha256,
        artifact_root=tmp_path / "universe",
        expected_result_schema_sha256=(
            STRICT_V2_S_CANDIDATE_SCHEMA_SHA256
        ),
        page_size=2,
    )


def _rows() -> list[dict[str, object]]:
    return [
        _candidate(
            address="1BoatSLRHtKNngkdXEeobR76b53LETtpyT",
            tier="p0",
        ),
        _candidate(
            address="3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy",
            tier="p1",
        ),
        _candidate(
            address="1BitcoinEaterAddressDontSendf59kuE",
            tier="edge",
        ),
        _candidate(
            address="bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
            tier="coarse_other",
        ),
    ]


def _with_rehashed_update(
    row: dict[str, object],
    **changes: object,
) -> dict[str, object]:
    updated = {**row, **changes}
    updated["candidate_row_sha256"] = candidate_row_sha256(updated)
    return updated


def _non_address_subject(
    *,
    subject: str = "nonstandard-subject-fixture",
    tier: str = "coarse_other",
) -> dict[str, object]:
    row = _candidate(
        address="bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
        tier=tier,
    )
    return _with_rehashed_update(
        row,
        normalized_address=subject,
        address_bucket=candidate_address_bucket(subject),
    )


def _subject_sha256(subject: str) -> str:
    return hashlib.sha256(subject.encode("ascii")).hexdigest()


def test_candidate_hash_and_bucket_are_deterministic() -> None:
    row = _candidate(
        address="1BoatSLRHtKNngkdXEeobR76b53LETtpyT",
        tier="p0",
    )

    assert row["address_bucket"] == candidate_address_bucket(
        str(row["normalized_address"])
    )
    assert row["candidate_row_sha256"] == candidate_row_sha256(row)
    assert row["address_bucket"] == 13
    assert row["candidate_row_sha256"] == (
        "27b6453831202877970a225f5bf46c87cfe83459697f0a6524cbd3270dcde520"
    )


def test_production_exclusion_contract_reconciles_source_population() -> None:
    contract = CandidateSubjectExclusionContract()

    assert len(contract.subjects) == 13
    assert contract.tier_counts() == {
        "p0": 1,
        "p1": 0,
        "edge": 0,
        "coarse_other": 12,
    }
    published = contract.published_counts(
        CandidateArtifactExpectedCounts()
    )
    assert published.total == 1_090_398
    assert published.by_tier() == {
        "p0": 21_735,
        "p1": 2_143,
        "edge": 133_730,
        "coarse_other": 932_790,
    }


def test_publication_preview_writes_nothing(tmp_path: Path) -> None:
    receipt_sha256 = _execution_receipt(tmp_path / "execution.json")
    request = _request(tmp_path, receipt_sha256=receipt_sha256)

    outcome = StrictV2SCandidateArtifactPublisher.preview(request)

    assert outcome.status == "dry_run"
    assert outcome.candidate_rows == 0
    assert outcome.network_requests == 0
    assert outcome.written_paths == ()
    assert not (tmp_path / "universe").exists()


def test_publication_writes_real_addresses_and_checksum_manifest(
    tmp_path: Path,
) -> None:
    receipt_sha256 = _execution_receipt(tmp_path / "execution.json")
    request = _request(tmp_path, receipt_sha256=receipt_sha256)
    backend = FakeCandidateTableBackend(_rows())
    publisher = StrictV2SCandidateArtifactPublisher(
        backend=backend,
        expected_counts=CandidateArtifactExpectedCounts(
            total=4,
            p0=1,
            p1=1,
            edge=1,
            coarse_other=1,
        ),
        exclusion_contract=NO_SUBJECT_EXCLUSIONS,
    )

    outcome = publisher.run(request)

    assert outcome.status == "published"
    assert outcome.candidate_rows == 4
    assert outcome.source_candidate_subject_rows == 4
    assert outcome.published_address_rows == 4
    assert outcome.excluded_non_address_subject_rows == 0
    assert outcome.tier_counts == {
        "p0": 1,
        "p1": 1,
        "edge": 1,
        "coarse_other": 1,
    }
    assert outcome.provider_requests == 0
    assert outcome.provider_points == 0
    assert backend.calls == [
        "destination_metadata",
        "stream_destination_arrow_batches",
    ]

    root = Path(outcome.campaign_root)
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["candidate_rows"] == 4
    assert manifest["source_candidate_subject_rows"] == 4
    assert manifest["published_address_rows"] == 4
    assert manifest["excluded_non_address_subject_rows"] == 0
    assert manifest["excluded_non_address_subject_sha256"] == []
    assert manifest["source_execution_receipt_sha256"] == receipt_sha256
    assert manifest["files"]
    manifest_hash = manifest.pop("manifest_sha256")
    assert manifest_hash == hashlib.sha256(
        json.dumps(
            manifest,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    ).hexdigest()
    for file_record in manifest["files"]:
        artifact_path = root / file_record["path"]
        assert artifact_path.stat().st_size == file_record["size"]
        assert hashlib.sha256(artifact_path.read_bytes()).hexdigest() == (
            file_record["sha256"]
        )
    parquet_paths = sorted(root.glob("candidates/tier=*/bucket=*/*.parquet"))
    assert len(parquet_paths) == 4
    exported = []
    for path in parquet_paths:
        exported.extend(pq.read_table(path).to_pylist())
    assert {row["normalized_address"] for row in exported} == {
        row["normalized_address"] for row in _rows()
    }


def test_publication_reconciles_pinned_non_address_subject(
    tmp_path: Path,
) -> None:
    subject = "nonstandard-subject-fixture"
    subject_sha256 = _subject_sha256(subject)
    rows = [*_rows(), _non_address_subject(subject=subject)]
    receipt_sha256 = _execution_receipt(
        tmp_path / "execution.json",
        candidate_rows=5,
    )
    request = _request(tmp_path, receipt_sha256=receipt_sha256)
    publisher = StrictV2SCandidateArtifactPublisher(
        backend=FakeCandidateTableBackend(rows),
        expected_counts=CandidateArtifactExpectedCounts(
            total=5,
            p0=1,
            p1=1,
            edge=1,
            coarse_other=2,
        ),
        exclusion_contract=CandidateSubjectExclusionContract(
            subjects=((subject_sha256, "coarse_other"),),
        ),
    )

    outcome = publisher.run(request)

    assert outcome.candidate_rows == 4
    assert outcome.source_candidate_subject_rows == 5
    assert outcome.source_tier_counts["coarse_other"] == 2
    assert outcome.published_address_rows == 4
    assert outcome.published_tier_counts["coarse_other"] == 1
    assert outcome.excluded_non_address_subject_rows == 1
    assert outcome.excluded_non_address_tier_counts == {
        "p0": 0,
        "p1": 0,
        "edge": 0,
        "coarse_other": 1,
    }
    assert outcome.excluded_non_address_subject_sha256 == (
        subject_sha256,
    )

    root = Path(outcome.campaign_root)
    manifest = json.loads(
        (root / "manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["source_candidate_subject_rows"] == 5
    assert manifest["published_address_rows"] == 4
    assert manifest["excluded_non_address_subject_rows"] == 1
    assert manifest["excluded_non_address_subject_sha256"] == [
        subject_sha256
    ]
    assert manifest["non_address_exclusion_reason"] == (
        "bigquery_nonstandard_script_subject"
    )
    exported = []
    for path in root.glob("candidates/tier=*/bucket=*/*.parquet"):
        exported.extend(pq.read_table(path).to_pylist())
    assert subject not in {
        row["normalized_address"] for row in exported
    }


def test_publication_blocks_unknown_non_address_subject(
    tmp_path: Path,
) -> None:
    rows = [*_rows(), _non_address_subject()]
    receipt_sha256 = _execution_receipt(
        tmp_path / "execution.json",
        candidate_rows=5,
    )
    request = _request(tmp_path, receipt_sha256=receipt_sha256)
    publisher = StrictV2SCandidateArtifactPublisher(
        backend=FakeCandidateTableBackend(rows),
        expected_counts=CandidateArtifactExpectedCounts(
            total=5,
            p0=1,
            p1=1,
            edge=1,
            coarse_other=2,
        ),
        exclusion_contract=CandidateSubjectExclusionContract(subjects=()),
    )

    with pytest.raises(
        CandidatePublicationError,
        match="candidate row validation failed",
    ):
        publisher.run(request)


def test_publication_blocks_known_exclusion_in_wrong_tier(
    tmp_path: Path,
) -> None:
    subject = "nonstandard-subject-fixture"
    rows = [*_rows(), _non_address_subject(subject=subject)]
    receipt_sha256 = _execution_receipt(
        tmp_path / "execution.json",
        candidate_rows=5,
    )
    request = _request(tmp_path, receipt_sha256=receipt_sha256)
    publisher = StrictV2SCandidateArtifactPublisher(
        backend=FakeCandidateTableBackend(rows),
        expected_counts=CandidateArtifactExpectedCounts(
            total=5,
            p0=1,
            p1=1,
            edge=1,
            coarse_other=2,
        ),
        exclusion_contract=CandidateSubjectExclusionContract(
            subjects=((_subject_sha256(subject), "p0"),),
        ),
    )

    with pytest.raises(
        CandidatePublicationError,
        match="non-address subject tier mismatch",
    ):
        publisher.run(request)


def test_publication_blocks_when_pinned_exclusion_is_missing(
    tmp_path: Path,
) -> None:
    subject_sha256 = _subject_sha256("nonstandard-subject-fixture")
    receipt_sha256 = _execution_receipt(tmp_path / "execution.json")
    request = _request(tmp_path, receipt_sha256=receipt_sha256)
    publisher = StrictV2SCandidateArtifactPublisher(
        backend=FakeCandidateTableBackend(_rows()),
        expected_counts=CandidateArtifactExpectedCounts(
            total=4,
            p0=1,
            p1=1,
            edge=1,
            coarse_other=1,
        ),
        exclusion_contract=CandidateSubjectExclusionContract(
            subjects=((subject_sha256, "coarse_other"),),
        ),
    )

    with pytest.raises(
        CandidatePublicationError,
        match="non-address subject exclusion set mismatch",
    ):
        publisher.run(request)


def test_publication_locally_bounds_large_storage_api_batches(
    tmp_path: Path,
) -> None:
    receipt_sha256 = _execution_receipt(tmp_path / "execution.json")
    request = _request(tmp_path, receipt_sha256=receipt_sha256)
    publisher = StrictV2SCandidateArtifactPublisher(
        backend=OversizedCandidateTableBackend(_rows()),
        expected_counts=CandidateArtifactExpectedCounts(
            total=4,
            p0=1,
            p1=1,
            edge=1,
            coarse_other=1,
        ),
        exclusion_contract=NO_SUBJECT_EXCLUSIONS,
    )

    outcome = publisher.run(request)

    assert outcome.status == "published"
    assert outcome.candidate_rows == 4


def test_publication_canonicalizes_scaled_integral_satoshi_decimals(
    tmp_path: Path,
) -> None:
    receipt_sha256 = _execution_receipt(tmp_path / "execution.json")
    request = _request(tmp_path, receipt_sha256=receipt_sha256)
    rows = _rows()
    rows[0]["current_utxo_sats"] = Decimal(
        "10000000000.00000000000000000000000000000000000000"
    )
    publisher = StrictV2SCandidateArtifactPublisher(
        backend=FakeCandidateTableBackend(rows),
        expected_counts=CandidateArtifactExpectedCounts(
            total=4,
            p0=1,
            p1=1,
            edge=1,
            coarse_other=1,
        ),
        exclusion_contract=NO_SUBJECT_EXCLUSIONS,
    )

    outcome = publisher.run(request)

    parquet_path = next(
        Path(outcome.campaign_root).glob(
            "candidates/tier=p0/bucket=*/*.parquet"
        )
    )
    exported = pq.read_table(parquet_path).to_pylist()
    assert exported[0]["current_utxo_sats"] == Decimal(10_000_000_000)


def test_publication_blocks_receipt_change_after_initial_validation(
    tmp_path: Path,
) -> None:
    receipt_path = tmp_path / "execution.json"
    receipt_sha256 = _execution_receipt(receipt_path)
    request = _request(tmp_path, receipt_sha256=receipt_sha256)
    publisher = StrictV2SCandidateArtifactPublisher(
        backend=MutatingReceiptBackend(_rows(), receipt_path),
        expected_counts=CandidateArtifactExpectedCounts(
            total=4,
            p0=1,
            p1=1,
            edge=1,
            coarse_other=1,
        ),
        exclusion_contract=NO_SUBJECT_EXCLUSIONS,
    )

    with pytest.raises(
        CandidatePublicationError,
        match="execution receipt changed during publication",
    ):
        publisher.run(request)

    assert not (
        tmp_path
        / "universe"
        / "campaigns"
        / request.campaign_id
    ).exists()


@pytest.mark.parametrize(
    "mutator",
    [
        lambda rows: rows + [dict(rows[0])],
        lambda rows: [
            {
                **rows[0],
                "candidate_tier": "p1",
                "tier_rank": 1,
            },
            *rows[1:],
        ],
        lambda rows: [
            {
                **rows[0],
                "candidate_row_sha256": "00" * 32,
            },
            *rows[1:],
        ],
        lambda rows: [
            _with_rehashed_update(
                rows[0],
                lifetime_received_sats=Decimal(1),
            ),
            *rows[1:],
        ],
    ],
)
def test_publication_blocks_duplicate_wrong_tier_or_hash(
    tmp_path: Path,
    mutator,
) -> None:
    receipt_sha256 = _execution_receipt(tmp_path / "execution.json")
    request = _request(tmp_path, receipt_sha256=receipt_sha256)
    rows = mutator(_rows())
    backend = FakeCandidateTableBackend(rows)
    publisher = StrictV2SCandidateArtifactPublisher(
        backend=backend,
        expected_counts=CandidateArtifactExpectedCounts(
            total=len(rows),
            p0=sum(row["candidate_tier"] == "p0" for row in rows),
            p1=sum(row["candidate_tier"] == "p1" for row in rows),
            edge=sum(row["candidate_tier"] == "edge" for row in rows),
            coarse_other=sum(
                row["candidate_tier"] == "coarse_other" for row in rows
            ),
        ),
        exclusion_contract=NO_SUBJECT_EXCLUSIONS,
    )

    with pytest.raises(CandidatePublicationError):
        publisher.run(request)

    assert not (
        tmp_path
        / "universe"
        / "campaigns"
        / request.campaign_id
    ).exists()


def test_publication_blocks_duplicate_address_across_tiers(
    tmp_path: Path,
) -> None:
    receipt_sha256 = _execution_receipt(tmp_path / "execution.json")
    request = _request(tmp_path, receipt_sha256=receipt_sha256)
    address = "1BoatSLRHtKNngkdXEeobR76b53LETtpyT"
    rows = [
        _candidate(address=address, tier="p0"),
        _candidate(address=address, tier="p1"),
        *_rows()[2:],
    ]
    publisher = StrictV2SCandidateArtifactPublisher(
        backend=FakeCandidateTableBackend(rows),
        expected_counts=CandidateArtifactExpectedCounts(
            total=4,
            p0=1,
            p1=1,
            edge=1,
            coarse_other=1,
        ),
        exclusion_contract=NO_SUBJECT_EXCLUSIONS,
    )

    with pytest.raises(
        CandidatePublicationError,
        match="duplicate candidate address",
    ):
        publisher.run(request)

    assert not (
        tmp_path
        / "universe"
        / "campaigns"
        / request.campaign_id
    ).exists()
