"""Deterministic BTC universe importance policy."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Literal, Sequence

from pydantic import Field, field_validator

from crypto_address_identity.universe.anchors import AnchorReason
from crypto_address_identity.universe.models import AddressFeatureRow, UniverseModel


BTC = 100_000_000
POLICY_VERSION = "btc_importance_v1"
P1_MIN_SCORE = 25
CONTROL_SAMPLE_NUMERATOR = 2
CONTROL_SAMPLE_DENOMINATOR = 100

P0_THRESHOLDS = {
    "utxo_ge_100_btc": 100 * BTC,
    "same_tx_receive_ge_500_btc": 500 * BTC,
    "gross_90d_ge_1000_btc": 1_000 * BTC,
    "lifetime_ge_10000_active_365d": 10_000 * BTC,
}

BALANCE_BUCKETS = (
    (1_000 * BTC, 25, "balance_ge_1000_btc"),
    (100 * BTC, 20, "balance_ge_100_btc"),
    (10 * BTC, 12, "balance_ge_10_btc"),
    (1 * BTC, 5, "balance_ge_1_btc"),
)
MAX_SAME_TX_RECEIPT_BUCKETS = (
    (5_000 * BTC, 25, "max_same_tx_receipt_ge_5000_btc"),
    (1_000 * BTC, 20, "max_same_tx_receipt_ge_1000_btc"),
    (500 * BTC, 18, "max_same_tx_receipt_ge_500_btc"),
    (100 * BTC, 10, "max_same_tx_receipt_ge_100_btc"),
)
GROSS_90D_BUCKETS = (
    (10_000 * BTC, 20, "gross_90d_ge_10000_btc_score"),
    (1_000 * BTC, 15, "gross_90d_ge_1000_btc_score"),
    (100 * BTC, 8, "gross_90d_ge_100_btc_score"),
    (10 * BTC, 3, "gross_90d_ge_10_btc_score"),
)
RECENCY_BUCKETS = (
    (30, 10, "recency_le_30d"),
    (90, 7, "recency_le_90d"),
    (365, 3, "recency_le_365d"),
)
DIRECT_LARGE_SELECTED_EDGE_POINTS = 10
PROVIDER_ENTITY_PREDICTION_POINTS = 15
EXISTING_SYSTEM_WATCHLIST_POINTS = 10

_CALIBRATION_REASONS = frozenset(AnchorReason.__args__)
_FORCED_P0_REASONS = frozenset(
    {"official_or_signed_evidence", "existing_provider_conflict"}
)


class ImportanceScoreComponent(UniverseModel):
    code: str
    points: int = Field(gt=0)


class ImportanceDecision(UniverseModel):
    policy_version: Literal["btc_importance_v1"] = POLICY_VERSION
    address_id: str
    priority_class: Literal["P0", "P1", "CONTROL", "NONE"]
    importance_score: int = Field(ge=0)
    reason_codes: tuple[str, ...]
    cohort_names: tuple[str, ...]
    score_components: tuple[ImportanceScoreComponent, ...]
    unique_address_slots: Literal[1] = 1

    @field_validator("reason_codes", "cohort_names")
    @classmethod
    def canonicalize_names(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted(set(value)))

    @field_validator("score_components")
    @classmethod
    def canonicalize_components(
        cls, value: tuple[ImportanceScoreComponent, ...]
    ) -> tuple[ImportanceScoreComponent, ...]:
        by_code = {component.code: component for component in value}
        if len(by_code) != len(value):
            raise ValueError("score component codes must be unique")
        return tuple(by_code[code] for code in sorted(by_code))


class BtcImportancePolicyV1:
    """Classify one chain feature row using a fixed campaign cutoff."""

    def __init__(self, *, campaign_id: str, cutoff_time: datetime) -> None:
        if not campaign_id.strip():
            raise ValueError("campaign_id must be non-empty")
        if cutoff_time.tzinfo is None or cutoff_time.utcoffset() is None:
            raise ValueError("cutoff_time must be timezone-aware")
        self.campaign_id = campaign_id
        self.cutoff_time = cutoff_time.astimezone(UTC)

    def classify(
        self,
        row: AddressFeatureRow,
        *,
        calibration_reasons: Sequence[str] = (),
    ) -> ImportanceDecision:
        reasons = tuple(sorted(set(calibration_reasons)))
        unsupported = set(reasons) - _CALIBRATION_REASONS
        if unsupported:
            raise ValueError("unsupported calibration reason")
        if row.last_seen_time > self.cutoff_time:
            raise ValueError("feature activity is after campaign cutoff")

        recency_days = (self.cutoff_time - row.last_seen_time).days
        p0_reasons = self._p0_reasons(
            row=row,
            recency_days=recency_days,
            calibration_reasons=reasons,
        )
        score_components = self._score_components(
            row=row,
            recency_days=recency_days,
            calibration_reasons=reasons,
        )
        importance_score = sum(component.points for component in score_components)
        cohorts = self._cohorts(
            row=row,
            recency_days=recency_days,
            calibration_reasons=reasons,
        )

        if p0_reasons:
            priority_class: Literal["P0", "P1", "CONTROL", "NONE"] = "P0"
            decision_reasons = p0_reasons
        elif importance_score >= P1_MIN_SCORE:
            priority_class = "P1"
            decision_reasons = tuple(
                component.code for component in score_components
            )
        elif is_deterministic_control_sample(
            campaign_id=self.campaign_id,
            address_id=row.address_id,
        ):
            priority_class = "CONTROL"
            decision_reasons = ("deterministic_low_score_control",)
            cohorts = tuple(sorted(set(cohorts) | {"control"}))
        else:
            priority_class = "NONE"
            decision_reasons = ()

        return ImportanceDecision(
            address_id=row.address_id,
            priority_class=priority_class,
            importance_score=importance_score,
            reason_codes=decision_reasons,
            cohort_names=cohorts,
            score_components=score_components,
        )

    @staticmethod
    def _p0_reasons(
        *,
        row: AddressFeatureRow,
        recency_days: int,
        calibration_reasons: tuple[str, ...],
    ) -> tuple[str, ...]:
        reasons = set(calibration_reasons) & _FORCED_P0_REASONS
        if row.current_utxo_sats >= P0_THRESHOLDS["utxo_ge_100_btc"]:
            reasons.add("utxo_ge_100_btc")
        if (
            row.max_same_tx_received_sats
            >= P0_THRESHOLDS["same_tx_receive_ge_500_btc"]
        ):
            reasons.add("same_tx_receive_ge_500_btc")
        if row.gross_flow_90d_sats >= P0_THRESHOLDS["gross_90d_ge_1000_btc"]:
            reasons.add("gross_90d_ge_1000_btc")
        if (
            row.lifetime_received_sats
            >= P0_THRESHOLDS["lifetime_ge_10000_active_365d"]
            and recency_days <= 365
        ):
            reasons.add("lifetime_ge_10000_active_365d")
        return tuple(sorted(reasons))

    @staticmethod
    def _score_components(
        *,
        row: AddressFeatureRow,
        recency_days: int,
        calibration_reasons: tuple[str, ...],
    ) -> tuple[ImportanceScoreComponent, ...]:
        components: list[ImportanceScoreComponent] = []
        _append_first_bucket(components, row.current_utxo_sats, BALANCE_BUCKETS)
        _append_first_bucket(
            components,
            row.max_same_tx_received_sats,
            MAX_SAME_TX_RECEIPT_BUCKETS,
        )
        _append_first_bucket(
            components,
            row.gross_flow_90d_sats,
            GROSS_90D_BUCKETS,
        )
        _append_first_bucket(components, recency_days, RECENCY_BUCKETS, at_most=True)

        if row.direct_large_counterparty_count > 0:
            components.append(
                ImportanceScoreComponent(
                    code="direct_large_selected_edge",
                    points=DIRECT_LARGE_SELECTED_EDGE_POINTS,
                )
            )
        if "provider_entity_prediction" in calibration_reasons:
            components.append(
                ImportanceScoreComponent(
                    code="provider_entity_prediction",
                    points=PROVIDER_ENTITY_PREDICTION_POINTS,
                )
            )
        if "existing_system_watchlist" in calibration_reasons:
            components.append(
                ImportanceScoreComponent(
                    code="existing_system_watchlist",
                    points=EXISTING_SYSTEM_WATCHLIST_POINTS,
                )
            )
        return tuple(sorted(components, key=lambda component: component.code))

    @staticmethod
    def _cohorts(
        *,
        row: AddressFeatureRow,
        recency_days: int,
        calibration_reasons: tuple[str, ...],
    ) -> tuple[str, ...]:
        cohorts: set[str] = set()
        if row.current_utxo_sats >= BTC:
            cohorts.add("current_capital")
        if row.max_same_tx_received_sats >= 100 * BTC:
            cohorts.add("historical_large_receipt")
        if row.gross_flow_90d_sats >= 10 * BTC:
            cohorts.add("high_turnover")
        if row.current_utxo_sats >= BTC and recency_days > 365:
            cohorts.add("dormant_holder")
        if row.direct_large_counterparty_count > 0:
            cohorts.add("high_value_connector")
        if calibration_reasons:
            cohorts.add("calibration")
        return tuple(sorted(cohorts))


def is_deterministic_control_sample(*, campaign_id: str, address_id: str) -> bool:
    digest = hashlib.sha256(f"{campaign_id}:{address_id}".encode("ascii")).digest()
    value = int.from_bytes(digest[:8], byteorder="big", signed=False)
    threshold = (
        (1 << 64) * CONTROL_SAMPLE_NUMERATOR // CONTROL_SAMPLE_DENOMINATOR
    )
    return value < threshold


def _append_first_bucket(
    components: list[ImportanceScoreComponent],
    value: int,
    buckets: tuple[tuple[int, int, str], ...],
    *,
    at_most: bool = False,
) -> None:
    for threshold, points, code in buckets:
        matched = value <= threshold if at_most else value >= threshold
        if matched:
            components.append(ImportanceScoreComponent(code=code, points=points))
            return
