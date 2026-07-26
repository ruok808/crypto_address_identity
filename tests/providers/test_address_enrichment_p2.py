from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from crypto_address_identity.address_enrichment import (
    AddressEnrichmentArtifactError,
    BtcV2SP2AddressQueueBuilder,
)
from crypto_address_identity.cli import main


BTC_ADDRESSES = (
    "1BoatSLRHtKNngkdXEeobR76b53LETtpyT",
    "bc1qcr8te4kr609gcawutmrza0j4xv80jy8z306fyu",
    "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy",
    "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
    "1BitcoinEaterAddressDontSendf59kuE",
)


def _semantic_hash(value: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _write_inputs(root: Path) -> tuple[Path, Path]:
    candidate_root = root / "candidate"
    row_hashes = [
        hashlib.sha256(address.encode()).hexdigest()
        for address in BTC_ADDRESSES
    ]
    tiers = ("edge", "coarse_other", "edge", "coarse_other", "edge")
    rows = [
        {
            "normalized_address": address,
            "candidate_tier": tier,
            "v2_chain_score": score,
            "current_utxo_sats": current,
            "lifetime_received_sats": lifetime,
            "residual_gross_90d_sats": recent,
            "max_same_tx_received_lifetime_sats": max_lifetime,
            "max_same_tx_received_365d_sats": max_365d,
            "max_same_tx_received_90d_sats": max_90d,
            "candidate_row_sha256": row_hash,
        }
        for (
            address,
            tier,
            score,
            current,
            lifetime,
            recent,
            max_lifetime,
            max_365d,
            max_90d,
            row_hash,
        ) in zip(
            BTC_ADDRESSES,
            tiers,
            (20, 10, 18, 5, 16),
            (100, 200, 300, 400, 500),
            (1_000, 2_000, 3_000, 4_000, 5_000),
            (10_000, 9_000, 8_000, 7_000, 6_000),
            (1_000, 2_000, 30_000, 4_000, 5_000),
            (1_000, 2_000, 3_000, 40_000, 5_000),
            (1_000, 20_000, 3_000, 4_000, 5_000),
            row_hashes,
            strict=True,
        )
    ]
    file_records: list[dict[str, object]] = []
    for tier in ("edge", "coarse_other"):
        tier_rows = [row for row in rows if row["candidate_tier"] == tier]
        path = (
            candidate_root
            / "candidates"
            / f"tier={tier}"
            / "bucket=00"
            / "part-00000.parquet"
        )
        path.parent.mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist(tier_rows), path)
        file_records.append(
            {
                "path": str(path.relative_to(candidate_root)),
                "row_count": len(tier_rows),
                "size": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    candidate_manifest: dict[str, object] = {
        "campaign_id": "fixture-v2s",
        "candidate_rows": len(rows),
        "files": file_records,
    }
    candidate_manifest["manifest_sha256"] = _semantic_hash(candidate_manifest)
    candidate_manifest_path = candidate_root / "manifest.json"
    candidate_manifest_path.write_text(
        json.dumps(candidate_manifest, sort_keys=True),
        encoding="utf-8",
    )

    coverage_root = root / "coverage"
    coverage_root.mkdir()
    coverage_path = coverage_root / "btc_v2s_coverage_state.parquet"
    pq.write_table(
        pa.table(
            {
                "normalized_address": list(BTC_ADDRESSES),
                "candidate_tier": list(tiers),
                "candidate_row_sha256": row_hashes,
                "coverage_state": [
                    "needs_direct_enrichment",
                    "needs_direct_enrichment",
                    "needs_direct_enrichment",
                    "needs_direct_enrichment",
                    "direct_enriched",
                ],
                "active_conflict": [False] * 5,
                "explicit_direct_requirement": [False] * 5,
            }
        ),
        coverage_path,
    )
    coverage_manifest: dict[str, object] = {
        "schema_version": "btc_v2s_coverage_state_v1",
        "snapshot_id": "fixture-coverage",
        "source_campaign_id": "fixture-v2s",
        "source_manifest_file_sha256": hashlib.sha256(
            candidate_manifest_path.read_bytes()
        ).hexdigest(),
        "files": [
            {
                "path": coverage_path.name,
                "row_count": len(rows),
                "size": coverage_path.stat().st_size,
                "sha256": hashlib.sha256(
                    coverage_path.read_bytes()
                ).hexdigest(),
            }
        ],
    }
    coverage_manifest["manifest_sha256"] = _semantic_hash(coverage_manifest)
    (coverage_root / "manifest.json").write_text(
        json.dumps(coverage_manifest, sort_keys=True),
        encoding="utf-8",
    )
    return candidate_root, coverage_root


def test_p2_queue_dry_run_is_budget_bounded_and_writes_nothing(
    runtime_root: Path,
) -> None:
    candidate_root, coverage_root = _write_inputs(runtime_root)
    output_root = runtime_root / "p2-queues"

    result = BtcV2SP2AddressQueueBuilder().build(
        candidate_campaign_root=candidate_root,
        coverage_snapshot_root=coverage_root,
        output_root=output_root,
        actual_points=8,
        account_reserve_points=2,
        fanout_recovery_reserve_points=2,
        observed_p95_points_per_address=1,
        built_at=datetime(2026, 7, 26, tzinfo=UTC),
        dry_run=True,
    )

    assert result.status == "dry_run"
    assert result.written is False
    assert result.queue_rows == 4
    assert result.eligible_rows == 4
    assert result.mandatory_direct_rows == 0
    assert result.direct_point_limit == 4
    assert result.campaign_point_limit == 6
    assert result.tier_counts == {"edge": 2, "coarse_other": 2}
    assert result.selection_threshold_economic_value_sats == 10_000
    assert not output_root.exists()


def test_p2_queue_publishes_economic_leaders_with_checksum_contract(
    runtime_root: Path,
) -> None:
    candidate_root, coverage_root = _write_inputs(runtime_root)

    result = BtcV2SP2AddressQueueBuilder().build(
        candidate_campaign_root=candidate_root,
        coverage_snapshot_root=coverage_root,
        output_root=runtime_root / "p2-queues",
        actual_points=8,
        account_reserve_points=2,
        fanout_recovery_reserve_points=3,
        observed_p95_points_per_address=1,
        built_at=datetime(2026, 7, 26, tzinfo=UTC),
    )

    assert result.status == "published"
    assert result.written is True
    assert result.queue_rows == 3
    rows = pq.read_table(result.parquet_path).to_pylist()
    assert [row["economic_value_sats"] for row in rows] == [
        40_000,
        30_000,
        20_000,
    ]
    assert {row["cohort"] for row in rows} == {"p2"}
    assert all(len(row["selection_key_sha256"]) == 64 for row in rows)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    declared = manifest.pop("manifest_sha256")
    assert declared == _semantic_hash(manifest)
    assert manifest["budget"]["actual_points"] == 8
    assert manifest["budget"]["account_reserve_points"] == 2
    assert manifest["budget"]["fanout_recovery_reserve_points"] == 3
    assert manifest["budget"]["direct_point_limit"] == 3


@pytest.mark.parametrize(
    ("actual", "account_reserve", "fanout_reserve", "p95"),
    (
        (0, 0, 0, 1),
        (8, 8, 0, 1),
        (8, 2, 6, 1),
        (8, 2, 2, 0),
    ),
)
def test_p2_queue_fails_closed_on_invalid_budget(
    runtime_root: Path,
    actual: int,
    account_reserve: int,
    fanout_reserve: int,
    p95: int,
) -> None:
    candidate_root, coverage_root = _write_inputs(runtime_root)

    with pytest.raises(AddressEnrichmentArtifactError):
        BtcV2SP2AddressQueueBuilder().build(
            candidate_campaign_root=candidate_root,
            coverage_snapshot_root=coverage_root,
            output_root=runtime_root / "p2-queues",
            actual_points=actual,
            account_reserve_points=account_reserve,
            fanout_recovery_reserve_points=fanout_reserve,
            observed_p95_points_per_address=p95,
        )


def test_p2_queue_cli_dry_run_reports_counts_without_writes(
    runtime_root: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    candidate_root, coverage_root = _write_inputs(runtime_root)
    output_root = runtime_root / "p2-queues"

    exit_code = main(
        [
            "coverage-sync",
            "build-v2s-p2-address-queue",
            "--candidate-campaign-root",
            str(candidate_root),
            "--coverage-snapshot-root",
            str(coverage_root),
            "--output-root",
            str(output_root),
            "--actual-points",
            "8",
            "--account-reserve-points",
            "2",
            "--fanout-recovery-reserve-points",
            "2",
            "--dry-run",
        ]
    )

    result = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert result["status"] == "dry_run"
    assert result["queue_rows"] == 4
    assert result["eligible_rows"] == 4
    assert result["written"] is False
    assert not output_root.exists()
