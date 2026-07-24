from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path

from crypto_address_identity.chains.bitcoin import normalize_bitcoin_address
from crypto_address_identity.universe.anchors import (
    CalibrationAnchorRow,
    CalibrationAnchorSnapshot,
)
from crypto_address_identity.universe.models import (
    AddressFeatureRow,
    SourceManifest,
    SourceProbeResult,
)
from crypto_address_identity.universe.policy import (
    BTC,
    is_deterministic_control_sample,
)
from crypto_address_identity.universe.statistics import CandidateStatisticsService
from crypto_address_identity.universe.storage import PublishedCampaign, UniverseStore
from tests.universe.conftest import make_accounting, make_feature, make_script


CUTOFF = datetime(2026, 7, 24, tzinfo=UTC)
CAMPAIGN_ID = "btc-stats-20260724"
_BASE58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _base58check_address(index: int) -> str:
    body = b"\x00" + hashlib.sha256(f"stats-address:{index}".encode()).digest()[:20]
    checksum = hashlib.sha256(hashlib.sha256(body).digest()).digest()[:4]
    raw = body + checksum
    number = int.from_bytes(raw, "big")
    encoded = ""
    while number:
        number, remainder = divmod(number, 58)
        encoded = _BASE58[remainder] + encoded
    return "1" * (len(raw) - len(raw.lstrip(b"\x00"))) + encoded


def _feature(index: int, **updates: object) -> AddressFeatureRow:
    address = _base58check_address(index)
    current = int(updates.pop("current_utxo_sats", 0))
    received = int(updates.pop("lifetime_received_sats", current))
    values: dict[str, object] = {
        "current_utxo_sats": current,
        "lifetime_received_sats": received,
        "lifetime_spent_sats": received - current,
        "max_same_tx_received_sats": 0,
        "inflow_90d_sats": 0,
        "outflow_90d_sats": 0,
        "gross_flow_90d_sats": 0,
        "direct_large_counterparty_count": 0,
        "last_seen_time": CUTOFF - timedelta(days=500),
    }
    values.update(updates)
    return make_feature(address, **values)


def _manifest(*, script_completeness: bool = True) -> SourceManifest:
    return SourceManifest(
        campaign_id=CAMPAIGN_ID,
        source_kind="fixture",
        source_revision="statistics-fixture-v1",
        cutoff_height=900_000,
        cutoff_hash="01" * 32,
        cutoff_time=CUTOFF,
        schema_sha256="02" * 32,
        query_sha256="03" * 32,
        source_capabilities=("address_rows", "script_hex", "source_accounting"),
        script_completeness=script_completeness,
    )


def _publish(
    tmp_path: Path,
    *,
    features: list[AddressFeatureRow],
    anchors: list[CalibrationAnchorRow] | None = None,
    script_completeness: bool = True,
    probe_status: str = "accepted",
) -> PublishedCampaign:
    store = UniverseStore(tmp_path / "universe")
    manifest = _manifest(script_completeness=script_completeness)
    writer = store.begin_campaign(manifest)
    writer.write_address_features(features)
    writer.write_script_subjects(
        [make_script(features[0].normalized_address, script_hex="51")]
    )
    writer.write_source_accounting(
        make_accounting(
            distinct_script_subjects=len(features) + 3,
            standard_single_address_rows=len(features),
            unmatched_input_rows=2,
        )
    )
    writer.write_source_probe(
        SourceProbeResult(
            source_kind="bigquery",
            status=probe_status,
            schema_sha256=manifest.schema_sha256,
            latest_height=manifest.cutoff_height,
            latest_hash=manifest.cutoff_hash,
            latest_time=manifest.cutoff_time,
            finalized_height=manifest.cutoff_height,
            finalized_hash=manifest.cutoff_hash,
            dry_run_bytes=1234,
            script_completeness=script_completeness,
            capabilities=("address_rows", "script_hex"),
            blocking_reasons=(
                ("fixture_source_blocked",) if probe_status == "blocked" else ()
            ),
            warnings=(
                ("historical_scripts_incomplete",)
                if not script_completeness
                else ()
            ),
        )
    )
    if anchors is not None:
        writer.write_calibration_anchor_snapshot(
            CalibrationAnchorSnapshot(
                as_of=CUTOFF,
                database_sha256="ab" * 32,
                rows=tuple(anchors),
            )
        )
    return writer.publish()


def _overlapping_campaign(tmp_path: Path) -> PublishedCampaign:
    features = [
        _feature(
            1,
            current_utxo_sats=120 * BTC,
            lifetime_received_sats=1_000 * BTC,
            max_same_tx_received_sats=700 * BTC,
        ),
        _feature(
            2,
            inflow_90d_sats=1_200 * BTC,
            gross_flow_90d_sats=1_200 * BTC,
        ),
        _feature(
            3,
            current_utxo_sats=10 * BTC,
            lifetime_received_sats=10 * BTC,
            inflow_90d_sats=10 * BTC,
            gross_flow_90d_sats=10 * BTC,
            last_seen_time=CUTOFF - timedelta(days=20),
        ),
        _feature(
            4,
            current_utxo_sats=1 * BTC,
            lifetime_received_sats=1 * BTC,
            max_same_tx_received_sats=100 * BTC,
            last_seen_time=CUTOFF - timedelta(days=20),
        ),
        _feature(
            5,
            current_utxo_sats=10 * BTC,
            lifetime_received_sats=10 * BTC,
            direct_large_counterparty_count=1,
            last_seen_time=CUTOFF - timedelta(days=200),
        ),
    ]
    anchored = normalize_bitcoin_address(features[4].normalized_address)
    anchor_only = normalize_bitcoin_address(_base58check_address(99))
    anchors = [
        CalibrationAnchorRow(
            address_id=anchored.address_id,
            normalized_address=anchored.normalized_address,
            reason_code="existing_system_watchlist",
        ),
        CalibrationAnchorRow(
            address_id=anchor_only.address_id,
            normalized_address=anchor_only.normalized_address,
            reason_code="provider_entity_prediction",
        ),
    ]
    return _publish(tmp_path, features=features, anchors=anchors)


def test_statistics_report_exact_dedupe_and_first_wave_capacity(
    tmp_path: Path,
) -> None:
    campaign = _overlapping_campaign(tmp_path)

    result = CandidateStatisticsService(campaign).dry_run(
        runtime_minutes=480,
        requests_per_minute=25,
        estimated_points_per_address=1,
        discovery_point_budget=100_000,
    )

    assert result.status == "dry_run"
    assert result.rate_limited_capacity == 12_000
    assert result.point_limited_capacity == 100_000
    assert result.provider_requests == 0
    assert result.provider_points == 0
    assert result.written_paths == ()
    assert result.reason_memberships > result.unique_selected_addresses
    assert result.duplicate_slots_prevented == (
        result.reason_memberships - result.unique_selected_addresses
    )
    assert result.anchor_only_count == 1
    assert result.first_wave_unique_addresses == result.unique_selected_addresses
    assert result.projected_minimum_minutes == math.ceil(
        result.unique_selected_addresses / 25
    )


def test_point_capacity_is_unknown_until_canary_estimate_exists(
    tmp_path: Path,
) -> None:
    result = CandidateStatisticsService(_overlapping_campaign(tmp_path)).dry_run(
        runtime_minutes=60,
        requests_per_minute=25,
        estimated_points_per_address=None,
        discovery_point_budget=100_000,
    )

    assert result.point_limited_capacity is None
    assert result.first_wave_unique_addresses <= 1_500


def test_all_p0_work_precedes_p1_and_remaining_p0_is_reported(
    tmp_path: Path,
) -> None:
    features = [
        _feature(
            index,
            current_utxo_sats=(100 + index) * BTC,
            lifetime_received_sats=(100 + index) * BTC,
        )
        for index in range(1, 8)
    ]
    campaign = _publish(tmp_path, features=features)

    result = CandidateStatisticsService(campaign).dry_run(
        runtime_minutes=1,
        requests_per_minute=3,
        estimated_points_per_address=None,
        discovery_point_budget=1,
    )

    assert result.p0_unique_addresses == 7
    assert result.first_wave_unique_addresses == 3
    assert result.remaining_p0_addresses == 4
    assert result.first_wave_cohort_counts == {"P0": 3}


def test_p1_cohort_cap_is_40_percent_when_other_cohorts_have_supply(
    tmp_path: Path,
) -> None:
    features: list[AddressFeatureRow] = []
    for index in range(1, 16):
        features.append(
            _feature(
                index,
                current_utxo_sats=10 * BTC,
                lifetime_received_sats=10 * BTC,
                inflow_90d_sats=10 * BTC,
                gross_flow_90d_sats=10 * BTC,
                last_seen_time=CUTOFF - timedelta(days=20),
            )
        )
    for index in range(16, 26):
        features.append(
            _feature(
                index,
                max_same_tx_received_sats=100 * BTC,
                current_utxo_sats=1 * BTC,
                lifetime_received_sats=1 * BTC,
                last_seen_time=CUTOFF - timedelta(days=20),
            )
        )
    campaign = _publish(tmp_path, features=features)

    result = CandidateStatisticsService(campaign).dry_run(
        runtime_minutes=1,
        requests_per_minute=10,
        estimated_points_per_address=None,
        discovery_point_budget=1,
    )

    assert result.first_wave_unique_addresses == 10
    assert max(result.first_wave_cohort_counts.values()) <= 4
    assert len(result.first_wave_cohort_counts) >= 2


def test_unused_quota_is_reassigned_when_other_cohorts_are_exhausted(
    tmp_path: Path,
) -> None:
    features = [
        _feature(
            index,
            current_utxo_sats=10 * BTC,
            lifetime_received_sats=10 * BTC,
            inflow_90d_sats=10 * BTC,
            gross_flow_90d_sats=10 * BTC,
            last_seen_time=CUTOFF - timedelta(days=20),
        )
        for index in range(1, 11)
    ]
    campaign = _publish(tmp_path, features=features)

    result = CandidateStatisticsService(campaign).dry_run(
        runtime_minutes=1,
        requests_per_minute=10,
        estimated_points_per_address=None,
        discovery_point_budget=1,
    )

    assert result.first_wave_unique_addresses == 10
    assert sum(result.first_wave_cohort_counts.values()) == 10


def test_incomplete_scripts_propagate_warning_without_inventing_coverage(
    tmp_path: Path,
) -> None:
    campaign = _publish(
        tmp_path,
        features=[_feature(1, current_utxo_sats=100 * BTC, lifetime_received_sats=100 * BTC)],
        script_completeness=False,
        probe_status="partial",
    )

    result = CandidateStatisticsService(campaign).dry_run(
        runtime_minutes=10,
        requests_per_minute=25,
        estimated_points_per_address=None,
        discovery_point_budget=1,
    )

    assert result.status == "dry_run"
    assert result.quality_status == "warning"
    assert result.script_completeness is False
    assert "historical_scripts_incomplete" in result.warning_reasons


def test_blocked_source_returns_aggregate_only_blocked_result(
    tmp_path: Path,
) -> None:
    campaign = _publish(
        tmp_path,
        features=[_feature(1, current_utxo_sats=100 * BTC, lifetime_received_sats=100 * BTC)],
        probe_status="blocked",
    )

    result = CandidateStatisticsService(campaign).dry_run(
        runtime_minutes=10,
        requests_per_minute=25,
        estimated_points_per_address=None,
        discovery_point_budget=1,
    )

    assert result.status == "blocked"
    assert result.quality_status == "blocked"
    assert result.blocking_reasons == ("fixture_source_blocked",)
    assert result.first_wave_unique_addresses == 0


def test_result_order_is_stable_and_json_contains_no_address_values(
    tmp_path: Path,
) -> None:
    campaign = _overlapping_campaign(tmp_path)
    service = CandidateStatisticsService(campaign)
    arguments = {
        "runtime_minutes": 480,
        "requests_per_minute": 25,
        "estimated_points_per_address": None,
        "discovery_point_budget": 100_000,
    }

    first = service.dry_run(**arguments)
    second = service.dry_run(**arguments)
    payload = json.dumps(first.model_dump(mode="json"), sort_keys=True)

    assert first == second
    assert tuple(first.cohort_counts) == tuple(sorted(first.cohort_counts))
    assert tuple(first.cohort_overlap_counts) == tuple(
        sorted(first.cohort_overlap_counts)
    )
    for index in (1, 2, 3, 4, 5, 99):
        address = _base58check_address(index)
        subject = normalize_bitcoin_address(address)
        assert address not in payload
        assert subject.address_id not in payload


def test_control_sample_count_is_reported_without_exposing_subject(
    tmp_path: Path,
) -> None:
    index = next(
        index
        for index in range(1, 100_000)
        if is_deterministic_control_sample(
            campaign_id=CAMPAIGN_ID,
            address_id=normalize_bitcoin_address(
                _base58check_address(index)
            ).address_id,
        )
    )
    campaign = _publish(tmp_path, features=[_feature(index)])

    result = CandidateStatisticsService(campaign).dry_run(
        runtime_minutes=1,
        requests_per_minute=25,
        estimated_points_per_address=None,
        discovery_point_budget=1,
    )

    assert result.control_unique_addresses == 1
    assert result.first_wave_unique_addresses == 1
