from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from crypto_address_identity.universe.policy import (
    BTC,
    BtcImportancePolicyV1,
    is_deterministic_control_sample,
)
from tests.universe.conftest import BTC_ADDRESSES, make_feature


CUTOFF = datetime(2026, 7, 24, tzinfo=UTC)


def policy(*, campaign_id: str = "btc-20260724") -> BtcImportancePolicyV1:
    return BtcImportancePolicyV1(
        campaign_id=campaign_id,
        cutoff_time=CUTOFF,
    )


def feature(**updates: object):
    current = int(updates.pop("current_utxo_sats", 0))
    received = int(updates.pop("lifetime_received_sats", current))
    spent = received - current
    values: dict[str, object] = {
        "current_utxo_sats": current,
        "lifetime_received_sats": received,
        "lifetime_spent_sats": spent,
        "direct_large_counterparty_count": 0,
        "last_seen_time": CUTOFF - timedelta(days=500),
        "max_same_tx_received_sats": 0,
        "inflow_90d_sats": 0,
        "outflow_90d_sats": 0,
        "gross_flow_90d_sats": 0,
    }
    values.update(updates)
    return make_feature(BTC_ADDRESSES[0], **values)


def test_p0_reasons_are_a_deduplicated_union() -> None:
    row = feature(
        current_utxo_sats=120 * BTC,
        lifetime_received_sats=20_000 * BTC,
        max_same_tx_received_sats=700 * BTC,
        inflow_90d_sats=1_000 * BTC,
        outflow_90d_sats=500 * BTC,
        gross_flow_90d_sats=1_500 * BTC,
        last_seen_time=CUTOFF - timedelta(days=20),
    )
    decision = policy().classify(row)

    assert decision.priority_class == "P0"
    assert decision.reason_codes == (
        "gross_90d_ge_1000_btc",
        "lifetime_ge_10000_active_365d",
        "same_tx_receive_ge_500_btc",
        "utxo_ge_100_btc",
    )
    assert decision.unique_address_slots == 1


@pytest.mark.parametrize(
    ("field", "value_btc", "expected_points"),
    [
        ("current_utxo_sats", 1, 5),
        ("current_utxo_sats", 10, 12),
        ("current_utxo_sats", 100, 20),
        ("current_utxo_sats", 1_000, 25),
        ("max_same_tx_received_sats", 100, 10),
        ("max_same_tx_received_sats", 500, 18),
        ("max_same_tx_received_sats", 1_000, 20),
        ("max_same_tx_received_sats", 5_000, 25),
        ("gross_flow_90d_sats", 10, 3),
        ("gross_flow_90d_sats", 100, 8),
        ("gross_flow_90d_sats", 1_000, 15),
        ("gross_flow_90d_sats", 10_000, 20),
    ],
)
def test_each_amount_bucket_uses_only_the_highest_match(
    field: str,
    value_btc: int,
    expected_points: int,
) -> None:
    updates: dict[str, object] = {field: value_btc * BTC}
    if field == "current_utxo_sats":
        updates["lifetime_received_sats"] = value_btc * BTC
    if field == "gross_flow_90d_sats":
        updates["inflow_90d_sats"] = value_btc * BTC
        updates["outflow_90d_sats"] = 0

    decision = policy().classify(feature(**updates))

    assert decision.importance_score == expected_points
    assert sum(component.points for component in decision.score_components) == (
        expected_points
    )
    assert len(decision.score_components) == 1


@pytest.mark.parametrize(
    ("days", "expected_points"),
    [(30, 10), (90, 7), (365, 3), (366, 0)],
)
def test_recency_uses_campaign_cutoff_not_wall_clock(
    days: int,
    expected_points: int,
) -> None:
    decision = policy().classify(
        feature(last_seen_time=CUTOFF - timedelta(days=days))
    )

    assert decision.importance_score == expected_points


def test_policy_rejects_activity_after_campaign_cutoff() -> None:
    with pytest.raises(ValueError, match="after campaign cutoff"):
        policy().classify(
            feature(last_seen_time=CUTOFF + timedelta(seconds=1))
        )


def test_score_adds_each_non_bucket_signal_once() -> None:
    decision = policy().classify(
        feature(
            current_utxo_sats=10 * BTC,
            lifetime_received_sats=10 * BTC,
            max_same_tx_received_sats=100 * BTC,
            inflow_90d_sats=10 * BTC,
            outflow_90d_sats=0,
            gross_flow_90d_sats=10 * BTC,
            direct_large_counterparty_count=2,
            last_seen_time=CUTOFF - timedelta(days=30),
        ),
        calibration_reasons=(
            "provider_entity_prediction",
            "provider_entity_prediction",
            "existing_system_watchlist",
        ),
    )

    assert decision.importance_score == 70
    assert decision.priority_class == "P1"
    assert tuple(component.code for component in decision.score_components) == (
        "balance_ge_10_btc",
        "direct_large_selected_edge",
        "existing_system_watchlist",
        "gross_90d_ge_10_btc_score",
        "max_same_tx_receipt_ge_100_btc",
        "provider_entity_prediction",
        "recency_le_30d",
    )


def test_p1_threshold_is_inclusive() -> None:
    exact_p1 = policy().classify(
        feature(),
        calibration_reasons=(
            "provider_entity_prediction",
            "existing_system_watchlist",
        ),
    )

    assert exact_p1.importance_score == 25
    assert exact_p1.priority_class == "P1"


def test_forced_anchor_reasons_are_supplied_separately_from_chain_features() -> None:
    decision = policy().classify(
        feature(),
        calibration_reasons=(
            "official_or_signed_evidence",
            "existing_provider_conflict",
            "existing_provider_conflict",
        ),
    )

    assert decision.priority_class == "P0"
    assert decision.reason_codes == (
        "existing_provider_conflict",
        "official_or_signed_evidence",
    )
    assert decision.importance_score == 0


def test_low_score_control_sample_is_deterministic_and_campaign_scoped() -> None:
    address_id = feature().address_id
    selected_campaign = next(
        f"btc-control-{index}"
        for index in range(10_000)
        if is_deterministic_control_sample(
            campaign_id=f"btc-control-{index}",
            address_id=address_id,
        )
    )

    first = policy(campaign_id=selected_campaign).classify(feature())
    second = policy(campaign_id=selected_campaign).classify(feature())

    assert first == second
    assert first.priority_class == "CONTROL"
    assert first.reason_codes == ("deterministic_low_score_control",)
    assert is_deterministic_control_sample(
        campaign_id=selected_campaign,
        address_id=address_id,
    )
    assert not all(
        is_deterministic_control_sample(
            campaign_id=f"{selected_campaign}-other-{index}",
            address_id=address_id,
        )
        for index in range(50)
    )


def test_policy_output_is_stably_sorted_and_contains_no_identity_claims() -> None:
    decision = policy().classify(
        feature(
            current_utxo_sats=10 * BTC,
            lifetime_received_sats=10 * BTC,
            max_same_tx_received_sats=100 * BTC,
            direct_large_counterparty_count=1,
        ),
        calibration_reasons=("existing_system_watchlist",),
    )
    payload = decision.model_dump(mode="json")

    assert decision.cohort_names == tuple(sorted(decision.cohort_names))
    assert decision.reason_codes == tuple(sorted(decision.reason_codes))
    assert decision.address_id == feature().address_id
    forbidden = {
        "entity",
        "entity_id",
        "owner",
        "ownership",
        "wallet_role",
        "label",
    }
    assert forbidden.isdisjoint(payload)


def test_policy_rejects_unknown_calibration_reason() -> None:
    with pytest.raises(ValueError, match="unsupported calibration reason"):
        policy().classify(
            feature(),
            calibration_reasons=("invented_owner_hint",),
        )
