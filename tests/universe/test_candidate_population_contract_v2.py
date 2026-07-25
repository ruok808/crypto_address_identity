from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path

import pytest

from crypto_address_identity.universe.candidate_population_contract_v2 import (
    OUTPUT_DEFINED_RECEIPT_FILENAME,
    POSITIVE_VALUE_RECEIPT_FILENAME,
    preview_candidate_population_contract_v2,
    validate_candidate_population_contract_v2,
)


def _v1_receipt(*, output_defined_count: int = 1_000) -> dict[str, object]:
    return {
        "schema_version": "btc_candidate_statistics_execution_receipt_v1",
        "status": "quality_blocked",
        "authorization_id": "btc-candidate-statistics-20260724-v1",
        "automatic_retries": 0,
        "blocking_reasons": [
            "candidate_statistics_input_only_addresses_present",
            "candidate_statistics_source_baseline_mismatch",
        ],
        "candidate_materialized": False,
        "cutoff_height": 959_187,
        "cutoff_time": "2026-07-24T23:59:59.999999Z",
        "execution_calls": 1,
        "expected_schema_sha256": (
            "7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7"
        ),
        "network_requests": 4,
        "provider_points": 0,
        "provider_requests": 0,
        "quality": {
            "allow_interpretation": False,
            "blocking_reasons": [
                "candidate_statistics_input_only_addresses_present",
                "candidate_statistics_source_baseline_mismatch",
            ],
            "status": "blocked",
            "warnings": [],
        },
        "query_sha256": (
            "5dbb2c914448837ac43b20e4943abb33130cf2ce9c1b7c2a72eb5ce4d285012c"
        ),
        "receipt_created": True,
        "row_count": 1,
        "statistics": {
            "contract_version": "btc_candidate_statistics_v1",
            "source_standard_address_count": output_defined_count,
            "source_input_only_address_count": 3,
            "negative_current_utxo_count": 0,
            "null_value_count": 0,
            "value_cast_failure_count": 0,
            "source_cutoff_height": 959_187,
            "source_cutoff_time": "2026-07-24T23:59:59.999999Z",
            "query_sha256": (
                "5dbb2c914448837ac43b20e4943abb33130cf2ce9c1b7c2a72eb5ce4d285012c"
            ),
            "schema_sha256": (
                "7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7"
            ),
        },
        "total_bytes_billed": 637_999_775_744,
        "total_bytes_processed": 637_999_682_243,
    }


def _v2_receipt(
    *,
    positive_value_count: int = 900,
    p0_count: int = 100,
    p1_count: int = 50,
    edge_count: int = 100,
    coarse_count: int = 500,
) -> dict[str, object]:
    return {
        "schema_version": (
            "btc_candidate_statistics_v2_execution_receipt_v1"
        ),
        "status": "quality_blocked",
        "authorization_id": (
            "btc-importance-v2-20260724-quota-recovery-one-shot"
        ),
        "automatic_retries": 0,
        "billing_acknowledged": True,
        "blocking_reasons": [
            "candidate_statistics_v2_source_baseline_mismatch",
        ],
        "candidate_materialized": False,
        "cutoff_height": 959_187,
        "cutoff_time": "2026-07-24T23:59:59.999999Z",
        "execution_calls": 1,
        "expected_schema_sha256": (
            "7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7"
        ),
        "job_id": (
            "cai_btc_importance_v2_"
            "b2bf4b71772b68d2d2e7f7ec2303745a48d69d74"
        ),
        "network_requests": 4,
        "provider_points": 0,
        "provider_requests": 0,
        "quality": {
            "allow_interpretation": False,
            "blocking_reasons": [
                "candidate_statistics_v2_source_baseline_mismatch",
            ],
            "status": "blocked",
            "warnings": [],
        },
        "query_sha256": (
            "47b0b8977cc1443578bc3daf3f90a2cf5e0e48ae758a4b7a133d3caa7d301e74"
        ),
        "receipt_created": True,
        "recovery_evidence_validated": True,
        "row_count": 1,
        "statistics": {
            "contract_version": "btc_candidate_statistics_v2",
            "policy_version": "btc_importance_v2",
            "source_standard_address_count": positive_value_count,
            "source_input_only_address_count": 3,
            "negative_current_utxo_count": 0,
            "null_value_count": 0,
            "value_cast_failure_count": 0,
            "source_cutoff_height": 959_187,
            "source_cutoff_time": "2026-07-24T23:59:59.999999Z",
            "query_sha256": (
                "47b0b8977cc1443578bc3daf3f90a2cf5e0e48ae758a4b7a133d3caa7d301e74"
            ),
            "schema_sha256": (
                "7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7"
            ),
            "strict_variant": {
                "variant": "V2-S",
                "chain_p0_union_count": p0_count,
                "chain_p1_count": p1_count,
                "edge_upgrade_frontier_count": edge_count,
                "coarse_candidate_union_count": coarse_count,
                "excluded_source_address_count": (
                    positive_value_count - coarse_count
                ),
                "p0_p1_overlap_count": 0,
            },
        },
        "total_bytes_billed": 637_999_775_744,
        "total_bytes_processed": 637_999_682_243,
    }


def _write_receipt(path: Path, payload: dict[str, object]) -> str:
    encoded = (
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("ascii")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(encoded)
    path.chmod(0o600)
    return hashlib.sha256(encoded).hexdigest()


def _write_pair(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    v1: dict[str, object] | None = None,
    v2: dict[str, object] | None = None,
) -> Path:
    receipt_root = tmp_path / "executions"
    v1_payload = deepcopy(v1 or _v1_receipt())
    v2_payload = deepcopy(v2 or _v2_receipt())
    v1_statistics = v1_payload["statistics"]
    assert isinstance(v1_statistics, dict)
    v1_payload.update(
        {
            "expected_source_standard_address_count": 1_557_951_354,
            "maximum_bytes_billed": 650_000_000_000,
        }
    )
    v2_payload.update(
        {
            "expected_dry_run_bytes": 637_999_682_243,
            "expected_source_input_only_address_count": 3,
            "expected_source_standard_address_count": (
                v1_statistics["source_standard_address_count"]
            ),
            "maximum_bytes_billed": 650_000_000_000,
        }
    )
    v1_sha = _write_receipt(
        receipt_root / OUTPUT_DEFINED_RECEIPT_FILENAME,
        v1_payload,
    )
    v2_sha = _write_receipt(
        receipt_root / POSITIVE_VALUE_RECEIPT_FILENAME,
        v2_payload,
    )
    monkeypatch.setattr(
        "crypto_address_identity.universe."
        "candidate_population_contract_v2."
        "PINNED_OUTPUT_DEFINED_RECEIPT_SHA256",
        v1_sha,
    )
    monkeypatch.setattr(
        "crypto_address_identity.universe."
        "candidate_population_contract_v2."
        "PINNED_POSITIVE_VALUE_RECEIPT_SHA256",
        v2_sha,
    )
    return receipt_root


def test_dual_population_preview_is_offline_and_writes_nothing(
    tmp_path: Path,
) -> None:
    outcome = preview_candidate_population_contract_v2(
        receipt_root=tmp_path / "missing",
    )

    assert outcome.status == "dry_run"
    assert outcome.receipt_reads == 0
    assert outcome.network_requests == 0
    assert outcome.provider_requests == 0
    assert outcome.provider_points == 0
    assert outcome.written_paths == ()
    assert outcome.candidate_materialization_allowed is False
    assert not (tmp_path / "missing").exists()


def test_dual_population_accepts_exact_immutable_receipts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt_root = _write_pair(tmp_path, monkeypatch)

    outcome = validate_candidate_population_contract_v2(
        receipt_root=receipt_root,
    )

    assert outcome.status == "accepted"
    assert outcome.allow_population_interpretation is True
    assert outcome.allow_materialization_design is True
    assert outcome.candidate_materialization_allowed is False
    assert outcome.output_defined_standard_address_count == 1_000
    assert outcome.positive_value_standard_address_count == 900
    assert outcome.zero_value_only_standard_address_count == 100
    assert outcome.policy_denominator == "positive_value"
    assert outcome.population_relation == (
        "positive_value_subset_of_output_defined"
    )
    assert outcome.strict_capacity is not None
    assert outcome.strict_capacity.status == "eligible_for_design"
    assert outcome.strict_capacity.chain_p0_union_count == 100
    assert outcome.strict_capacity.coarse_candidate_union_count == 500
    assert outcome.receipt_reads == 2
    assert outcome.network_requests == 0
    assert outcome.written_paths == ()


def test_dual_population_checksum_drift_blocks_without_interpretation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt_root = _write_pair(tmp_path, monkeypatch)
    v2_path = receipt_root / POSITIVE_VALUE_RECEIPT_FILENAME
    v2_path.write_bytes(v2_path.read_bytes() + b" ")

    outcome = validate_candidate_population_contract_v2(
        receipt_root=receipt_root,
    )

    assert outcome.status == "blocked"
    assert outcome.allow_population_interpretation is False
    assert outcome.allow_materialization_design is False
    assert outcome.candidate_materialization_allowed is False
    assert outcome.blocking_reasons == (
        "positive_value_receipt_checksum_mismatch",
    )
    assert outcome.written_paths == ()


def test_dual_population_mode_drift_blocks_before_parsing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt_root = _write_pair(tmp_path, monkeypatch)
    (receipt_root / OUTPUT_DEFINED_RECEIPT_FILENAME).chmod(0o644)

    outcome = validate_candidate_population_contract_v2(
        receipt_root=receipt_root,
    )

    assert outcome.status == "blocked"
    assert outcome.blocking_reasons == (
        "output_defined_receipt_mode_invalid",
    )


def test_dual_population_rejects_receipt_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt_root = _write_pair(tmp_path, monkeypatch)
    path = receipt_root / OUTPUT_DEFINED_RECEIPT_FILENAME
    target = receipt_root / "output-defined-target.json"
    path.rename(target)
    path.symlink_to(target.name)

    outcome = validate_candidate_population_contract_v2(
        receipt_root=receipt_root,
    )

    assert outcome.status == "blocked"
    assert outcome.blocking_reasons == (
        "output_defined_receipt_not_regular",
    )


def test_dual_population_rejects_boolean_for_integer_contract_field(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    v1 = _v1_receipt()
    v1["automatic_retries"] = False
    receipt_root = _write_pair(tmp_path, monkeypatch, v1=v1)

    outcome = validate_candidate_population_contract_v2(
        receipt_root=receipt_root,
    )

    assert outcome.status == "blocked"
    assert outcome.blocking_reasons == (
        "output_defined_receipt_contract_invalid",
    )


def test_dual_population_rejects_boolean_for_statistics_integer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    v1 = _v1_receipt()
    statistics = v1["statistics"]
    assert isinstance(statistics, dict)
    statistics["negative_current_utxo_count"] = False
    receipt_root = _write_pair(tmp_path, monkeypatch, v1=v1)

    outcome = validate_candidate_population_contract_v2(
        receipt_root=receipt_root,
    )

    assert outcome.status == "blocked"
    assert outcome.blocking_reasons == (
        "output_defined_receipt_contract_invalid",
    )


def test_dual_population_rejects_unexpected_source_blocker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    v2 = _v2_receipt()
    quality = deepcopy(v2["quality"])
    assert isinstance(quality, dict)
    quality["blocking_reasons"] = [
        "candidate_statistics_v2_source_baseline_mismatch",
        "candidate_statistics_v2_null_value",
    ]
    v2["quality"] = quality
    v2["blocking_reasons"] = list(quality["blocking_reasons"])
    receipt_root = _write_pair(tmp_path, monkeypatch, v2=v2)

    outcome = validate_candidate_population_contract_v2(
        receipt_root=receipt_root,
    )

    assert outcome.status == "blocked"
    assert "positive_value_receipt_contract_invalid" in (
        outcome.blocking_reasons
    )


def test_dual_population_requires_positive_value_subset(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt_root = _write_pair(
        tmp_path,
        monkeypatch,
        v2=_v2_receipt(positive_value_count=1_001),
    )

    outcome = validate_candidate_population_contract_v2(
        receipt_root=receipt_root,
    )

    assert outcome.status == "blocked"
    assert outcome.blocking_reasons == (
        "positive_value_population_exceeds_output_defined_population",
    )


def test_dual_population_requires_v2_reference_to_output_defined_count(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt_root = _write_pair(tmp_path, monkeypatch)
    path = receipt_root / POSITIVE_VALUE_RECEIPT_FILENAME
    payload = json.loads(path.read_text(encoding="ascii"))
    payload["expected_source_standard_address_count"] = 999
    checksum = _write_receipt(path, payload)
    monkeypatch.setattr(
        "crypto_address_identity.universe."
        "candidate_population_contract_v2."
        "PINNED_POSITIVE_VALUE_RECEIPT_SHA256",
        checksum,
    )

    outcome = validate_candidate_population_contract_v2(
        receipt_root=receipt_root,
    )

    assert outcome.status == "blocked"
    assert outcome.blocking_reasons == (
        "positive_value_expected_population_not_output_defined",
    )


def test_dual_population_rejects_invalid_strict_partition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    v2 = _v2_receipt()
    statistics = v2["statistics"]
    assert isinstance(statistics, dict)
    strict = statistics["strict_variant"]
    assert isinstance(strict, dict)
    strict["excluded_source_address_count"] = 399
    receipt_root = _write_pair(tmp_path, monkeypatch, v2=v2)

    outcome = validate_candidate_population_contract_v2(
        receipt_root=receipt_root,
    )

    assert outcome.status == "blocked"
    assert outcome.blocking_reasons == (
        "strict_v2_s_partition_invalid",
    )


def test_dual_population_requires_disjoint_tiers_inside_coarse_union(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt_root = _write_pair(
        tmp_path,
        monkeypatch,
        v2=_v2_receipt(
            p0_count=100,
            p1_count=50,
            edge_count=400,
            coarse_count=500,
        ),
    )

    outcome = validate_candidate_population_contract_v2(
        receipt_root=receipt_root,
    )

    assert outcome.status == "blocked"
    assert outcome.blocking_reasons == (
        "strict_v2_s_partition_invalid",
    )


def test_dual_population_accepts_semantics_but_blocks_oversized_design(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt_root = _write_pair(
        tmp_path,
        monkeypatch,
        v2=_v2_receipt(
            positive_value_count=6_000_000,
            p0_count=1_000_001,
            p1_count=50,
            edge_count=1_000_001,
            coarse_count=5_000_001,
        ),
        v1=_v1_receipt(output_defined_count=6_100_000),
    )

    outcome = validate_candidate_population_contract_v2(
        receipt_root=receipt_root,
    )

    assert outcome.status == "accepted"
    assert outcome.allow_population_interpretation is True
    assert outcome.allow_materialization_design is False
    assert outcome.strict_capacity is not None
    assert outcome.strict_capacity.status == "capacity_blocked"
    assert outcome.strict_capacity.p0_within_limit is False
    assert outcome.strict_capacity.coarse_within_limit is False
    assert outcome.strict_capacity.edge_within_limit is False
    assert "strict_v2_s_capacity_limits_exceeded" in outcome.warnings
    assert outcome.candidate_materialization_allowed is False
