"""Aggregate-only first-wave statistics for immutable BTC campaigns."""

from __future__ import annotations

import heapq
import itertools
import json
import math
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from pydantic import ValidationError

from crypto_address_identity.universe.models import (
    AddressFeatureRow,
    CandidateReasonCount,
    CandidateStatistics,
    SourceProbeResult,
)
from crypto_address_identity.universe.policy import BtcImportancePolicyV1
from crypto_address_identity.universe.storage import (
    PublishedCampaign,
    UniverseStore,
)


P0_REASON_PRECEDENCE = (
    "existing_provider_conflict",
    "official_or_signed_evidence",
    "utxo_ge_100_btc",
    "same_tx_receive_ge_500_btc",
    "gross_90d_ge_1000_btc",
    "lifetime_ge_10000_active_365d",
)
P1_QUOTAS = (
    ("current_capital", 30),
    ("historical_large_receipt", 25),
    ("high_turnover", 20),
    ("dormant_holder", 10),
    ("high_value_connector", 10),
    ("calibration_control", 5),
)
P1_COHORT_CAP_PERCENT = 40
_FEATURE_COLUMNS = tuple(AddressFeatureRow.model_fields)


@dataclass(frozen=True)
class _Candidate:
    address_id: str
    priority_class: str
    rank: tuple[int, int, int]
    cohort_names: tuple[str, ...]


class _BoundedBest:
    def __init__(self, limit: int) -> None:
        self._limit = max(0, limit)
        self._heap: list[tuple[tuple[int, int, int], str, _Candidate]] = []

    def add(self, candidate: _Candidate) -> None:
        if self._limit == 0:
            return
        item = (candidate.rank, candidate.address_id, candidate)
        if len(self._heap) < self._limit:
            heapq.heappush(self._heap, item)
        elif item[:2] > self._heap[0][:2]:
            heapq.heapreplace(self._heap, item)

    def best(self) -> list[_Candidate]:
        return [
            item[2]
            for item in sorted(
                self._heap,
                key=lambda item: (item[0], item[1]),
                reverse=True,
            )
        ]


class CandidateStatisticsService:
    """Classify a campaign without writing or constructing a provider client."""

    def __init__(self, campaign: PublishedCampaign) -> None:
        self._campaign = campaign

    def dry_run(
        self,
        *,
        runtime_minutes: int,
        requests_per_minute: int,
        estimated_points_per_address: int | None,
        discovery_point_budget: int,
    ) -> CandidateStatistics:
        _validate_capacity_inputs(
            runtime_minutes=runtime_minutes,
            requests_per_minute=requests_per_minute,
            estimated_points_per_address=estimated_points_per_address,
            discovery_point_budget=discovery_point_budget,
        )
        rate_capacity = runtime_minutes * requests_per_minute
        point_capacity = (
            None
            if estimated_points_per_address is None
            else discovery_point_budget // estimated_points_per_address
        )
        bounded_capacity = (
            rate_capacity
            if point_capacity is None
            else min(rate_capacity, point_capacity)
        )

        quality_status, blocking, warnings = self._quality()
        with self._campaign.open_duckdb() as connection:
            source_accounting = _source_accounting(connection)
            unique_script_subjects = int(
                connection.execute(
                    "SELECT count(DISTINCT script_id) "
                    "FROM universe_btc_script_subject"
                ).fetchone()[0]
            )
            unique_standard_addresses = int(
                connection.execute(
                    "SELECT count(*) FROM universe_btc_address_feature"
                ).fetchone()[0]
            )
            calibration_anchor_count = int(
                connection.execute(
                    "SELECT count(DISTINCT address_id) "
                    "FROM universe_btc_calibration_anchor"
                ).fetchone()[0]
            )

            if blocking:
                return self._blocked_result(
                    source_accounting=source_accounting,
                    unique_script_subjects=unique_script_subjects,
                    unique_standard_addresses=unique_standard_addresses,
                    calibration_anchor_count=calibration_anchor_count,
                    rate_capacity=rate_capacity,
                    point_capacity=point_capacity,
                    blocking=blocking,
                    warnings=warnings,
                )

            aggregates = _AggregateScan(capacity=bounded_capacity)
            policy = BtcImportancePolicyV1(
                campaign_id=self._campaign.campaign_id,
                cutoff_time=self._campaign.source_manifest.cutoff_time,
            )
            for row, calibration_reasons in _feature_rows(connection):
                decision = policy.classify(
                    row,
                    calibration_reasons=calibration_reasons,
                )
                aggregates.observe_feature(row=row, decision=decision)
            for address_id, reasons in _anchor_only_rows(connection):
                aggregates.observe_anchor_only(
                    address_id=address_id,
                    reasons=reasons,
                )

        unique_selected = (
            aggregates.p0_count
            + aggregates.p1_count
            + aggregates.control_count
        )
        first_wave_capacity = min(unique_selected, bounded_capacity)
        first_wave_counts, first_wave_size = aggregates.select_first_wave(
            first_wave_capacity
        )
        reason_counts = tuple(
            CandidateReasonCount(reason_code=reason, count=count)
            for reason, count in sorted(aggregates.reason_counts.items())
        )
        projected_minutes = (
            math.ceil(unique_selected / requests_per_minute)
            if unique_selected
            else 0
        )
        return CandidateStatistics(
            status="dry_run",
            campaign_id=self._campaign.campaign_id,
            source_coverage=self._source_coverage(),
            quality_status=quality_status,
            script_completeness=self._campaign.source_manifest.script_completeness,
            unique_script_subjects=unique_script_subjects,
            unique_standard_addresses=unique_standard_addresses,
            source_accounting_counts=source_accounting,
            calibration_anchor_count=calibration_anchor_count,
            anchor_only_count=aggregates.anchor_only_count,
            p0_unique_addresses=aggregates.p0_count,
            p1_unique_addresses=aggregates.p1_count,
            control_unique_addresses=aggregates.control_count,
            unique_selected_addresses=unique_selected,
            reason_memberships=aggregates.reason_memberships,
            reason_counts=reason_counts,
            cohort_counts=dict(sorted(aggregates.cohort_counts.items())),
            cohort_overlap_counts=dict(
                sorted(aggregates.cohort_overlap_counts.items())
            ),
            duplicate_slots_prevented=(
                aggregates.reason_memberships - unique_selected
            ),
            rate_limited_capacity=rate_capacity,
            point_limited_capacity=point_capacity,
            first_wave_unique_addresses=first_wave_size,
            first_wave_cohort_counts=dict(sorted(first_wave_counts.items())),
            remaining_p0_addresses=max(
                0, aggregates.p0_count - min(aggregates.p0_count, first_wave_capacity)
            ),
            projected_minimum_minutes=projected_minutes,
            blocking_reasons=(),
            warning_reasons=warnings,
        )

    def _quality(self) -> tuple[str, tuple[str, ...], tuple[str, ...]]:
        store = UniverseStore(self._campaign.root.parent.parent)
        verification = store.verify(self._campaign.campaign_id)
        blocking: set[str] = set()
        warnings: set[str] = set()
        if verification.status != "ok":
            blocking.add("universe_integrity_error")

        for path in sorted((self._campaign.root / "source_probes").glob("*.json")):
            try:
                probe = SourceProbeResult.model_validate(
                    json.loads(path.read_text(encoding="ascii"))
                )
            except (OSError, ValueError, ValidationError):
                blocking.add("source_probe_invalid")
                continue
            blocking.update(probe.blocking_reasons)
            warnings.update(probe.warnings)
            if probe.status == "blocked" and not probe.blocking_reasons:
                blocking.add(f"{probe.source_kind}_source_blocked")
            elif probe.status == "partial":
                warnings.add(f"{probe.source_kind}_source_partial")

        if not self._campaign.source_manifest.script_completeness:
            warnings.add("historical_scripts_incomplete")
        if blocking:
            return "blocked", tuple(sorted(blocking)), tuple(sorted(warnings))
        if warnings:
            return "warning", (), tuple(sorted(warnings))
        return "ok", (), ()

    def _source_coverage(self) -> dict[str, int | str | bool | None]:
        manifest = self._campaign.source_manifest
        return {
            "source_kind": manifest.source_kind,
            "source_revision": manifest.source_revision,
            "cutoff_height": manifest.cutoff_height,
            "cutoff_hash": manifest.cutoff_hash,
            "cutoff_time": manifest.cutoff_time.isoformat().replace("+00:00", "Z"),
            "script_completeness": manifest.script_completeness,
            "address_feature_rows": self._campaign.address_feature_rows,
        }

    def _blocked_result(
        self,
        *,
        source_accounting: dict[str, int],
        unique_script_subjects: int,
        unique_standard_addresses: int,
        calibration_anchor_count: int,
        rate_capacity: int,
        point_capacity: int | None,
        blocking: tuple[str, ...],
        warnings: tuple[str, ...],
    ) -> CandidateStatistics:
        return CandidateStatistics(
            status="blocked",
            campaign_id=self._campaign.campaign_id,
            source_coverage=self._source_coverage(),
            quality_status="blocked",
            script_completeness=self._campaign.source_manifest.script_completeness,
            unique_script_subjects=unique_script_subjects,
            unique_standard_addresses=unique_standard_addresses,
            source_accounting_counts=source_accounting,
            calibration_anchor_count=calibration_anchor_count,
            anchor_only_count=0,
            p0_unique_addresses=0,
            p1_unique_addresses=0,
            control_unique_addresses=0,
            unique_selected_addresses=0,
            reason_memberships=0,
            reason_counts=(),
            cohort_counts={},
            cohort_overlap_counts={},
            duplicate_slots_prevented=0,
            rate_limited_capacity=rate_capacity,
            point_limited_capacity=point_capacity,
            first_wave_unique_addresses=0,
            first_wave_cohort_counts={},
            remaining_p0_addresses=0,
            projected_minimum_minutes=0,
            blocking_reasons=blocking,
            warning_reasons=warnings,
        )


class _AggregateScan:
    def __init__(self, *, capacity: int) -> None:
        self.p0_count = 0
        self.p1_count = 0
        self.control_count = 0
        self.anchor_only_count = 0
        self.reason_memberships = 0
        self.reason_counts: Counter[str] = Counter()
        self.cohort_counts: Counter[str] = Counter()
        self.cohort_overlap_counts: Counter[str] = Counter()
        self._p0 = _BoundedBest(capacity)
        self._p1_by_cohort = {
            cohort: _BoundedBest(capacity)
            for cohort, _ in P1_QUOTAS
        }

    def observe_feature(self, *, row: AddressFeatureRow, decision: Any) -> None:
        if decision.priority_class == "NONE":
            return
        self._observe_memberships(
            reasons=decision.reason_codes,
            cohorts=decision.cohort_names,
        )
        if decision.priority_class == "P0":
            self.p0_count += 1
            candidate = _Candidate(
                address_id=row.address_id,
                priority_class="P0",
                rank=_p0_rank(
                    address_id=row.address_id,
                    reasons=decision.reason_codes,
                    row=row,
                ),
                cohort_names=decision.cohort_names,
            )
            self._p0.add(candidate)
            return

        if decision.priority_class == "P1":
            self.p1_count += 1
        else:
            self.control_count += 1
        candidate = _Candidate(
            address_id=row.address_id,
            priority_class=decision.priority_class,
            rank=(
                decision.importance_score,
                max(
                    row.current_utxo_sats,
                    row.max_same_tx_received_sats,
                    row.gross_flow_90d_sats,
                    row.lifetime_received_sats,
                ),
                -int(row.address_id, 16),
            ),
            cohort_names=decision.cohort_names,
        )
        self._add_p1_candidate(candidate)

    def observe_anchor_only(
        self,
        *,
        address_id: str,
        reasons: tuple[str, ...],
    ) -> None:
        canonical_reasons = tuple(sorted(set(reasons)))
        if not canonical_reasons:
            canonical_reasons = ("anchor_only_calibration",)
        self.anchor_only_count += 1
        self.p0_count += 1
        self._observe_memberships(
            reasons=canonical_reasons,
            cohorts=("calibration",),
        )
        self._p0.add(
            _Candidate(
                address_id=address_id,
                priority_class="P0",
                rank=_anchor_only_rank(
                    address_id=address_id,
                    reasons=canonical_reasons,
                ),
                cohort_names=("calibration",),
            )
        )

    def _observe_memberships(
        self,
        *,
        reasons: Iterable[str],
        cohorts: Iterable[str],
    ) -> None:
        reason_tuple = tuple(sorted(set(reasons)))
        cohort_tuple = tuple(sorted(set(cohorts)))
        self.reason_memberships += len(reason_tuple)
        self.reason_counts.update(reason_tuple)
        self.cohort_counts.update(cohort_tuple)
        self.cohort_overlap_counts.update(
            "&".join(pair) for pair in itertools.combinations(cohort_tuple, 2)
        )

    def _add_p1_candidate(self, candidate: _Candidate) -> None:
        groups = _quota_groups(candidate.cohort_names)
        if not groups:
            groups = ("calibration_control",)
        for group in groups:
            self._p1_by_cohort[group].add(candidate)

    def select_first_wave(self, capacity: int) -> tuple[Counter[str], int]:
        if capacity <= 0:
            return Counter(), 0
        p0 = self._p0.best()[:capacity]
        counts: Counter[str] = Counter({"P0": len(p0)}) if p0 else Counter()
        selected_ids = {candidate.address_id for candidate in p0}
        remaining_slots = capacity - len(p0)
        if remaining_slots <= 0:
            return counts, len(selected_ids)

        quota_counts = _allocate_quotas(remaining_slots)
        candidates_by_group = {
            group: heap.best()
            for group, heap in self._p1_by_cohort.items()
        }
        for group, _ in P1_QUOTAS:
            for candidate in candidates_by_group[group]:
                if counts[group] >= quota_counts[group]:
                    break
                if candidate.address_id in selected_ids:
                    continue
                selected_ids.add(candidate.address_id)
                counts[group] += 1

        all_candidates = {
            candidate.address_id: candidate
            for values in candidates_by_group.values()
            for candidate in values
        }
        ordered = sorted(
            all_candidates.values(),
            key=lambda candidate: (candidate.rank, candidate.address_id),
            reverse=True,
        )
        nonempty_groups = {
            group
            for group, values in candidates_by_group.items()
            if any(candidate.address_id not in selected_ids for candidate in values)
        }
        cap = math.ceil(remaining_slots * P1_COHORT_CAP_PERCENT / 100)
        for candidate in ordered:
            if len(selected_ids) >= capacity:
                break
            if candidate.address_id in selected_ids:
                continue
            groups = _quota_groups(candidate.cohort_names) or (
                "calibration_control",
            )
            eligible = [
                group
                for group in groups
                if counts[group] < cap or len(nonempty_groups) < 3
            ]
            if not eligible:
                continue
            assigned = min(eligible, key=lambda group: (counts[group], group))
            selected_ids.add(candidate.address_id)
            counts[assigned] += 1
            nonempty_groups = {
                group
                for group, values in candidates_by_group.items()
                if any(item.address_id not in selected_ids for item in values)
            }

        return counts, len(selected_ids)


def _feature_rows(connection: Any) -> Iterable[tuple[AddressFeatureRow, tuple[str, ...]]]:
    columns = ", ".join(f"feature.{name}" for name in _FEATURE_COLUMNS)
    cursor = connection.execute(
        f"""
        WITH anchors AS (
            SELECT address_id, list(reason_code ORDER BY reason_code) AS reasons
            FROM universe_btc_calibration_anchor
            GROUP BY address_id
        )
        SELECT {columns}, coalesce(anchors.reasons, []::VARCHAR[]) AS reasons
        FROM universe_btc_address_feature AS feature
        LEFT JOIN anchors USING (address_id)
        ORDER BY feature.address_id
        """
    )
    while rows := cursor.fetchmany(2_048):
        for values in rows:
            yield (
                AddressFeatureRow.model_validate(
                    dict(zip(_FEATURE_COLUMNS, values[:-1], strict=True))
                ),
                tuple(values[-1]),
            )


def _anchor_only_rows(connection: Any) -> Iterable[tuple[str, tuple[str, ...]]]:
    cursor = connection.execute(
        """
        WITH anchors AS (
            SELECT address_id, list(reason_code ORDER BY reason_code) AS reasons
            FROM universe_btc_calibration_anchor
            GROUP BY address_id
        )
        SELECT anchors.address_id, anchors.reasons
        FROM anchors
        LEFT JOIN universe_btc_address_feature AS feature USING (address_id)
        WHERE feature.address_id IS NULL
        ORDER BY anchors.address_id
        """
    )
    while rows := cursor.fetchmany(2_048):
        for address_id, reasons in rows:
            yield address_id, tuple(reasons)


def _source_accounting(connection: Any) -> dict[str, int]:
    cursor = connection.execute("SELECT * FROM universe_btc_source_accounting")
    row = cursor.fetchone()
    if row is None:
        return {}
    names = [item[0] for item in cursor.description]
    return dict(sorted(zip(names, (int(value) for value in row), strict=True)))


def _p0_rank(
    *,
    address_id: str,
    reasons: tuple[str, ...],
    row: AddressFeatureRow,
) -> tuple[int, int, int]:
    reason = min(
        reasons,
        key=lambda item: (
            P0_REASON_PRECEDENCE.index(item)
            if item in P0_REASON_PRECEDENCE
            else len(P0_REASON_PRECEDENCE)
        ),
    )
    precedence = (
        P0_REASON_PRECEDENCE.index(reason)
        if reason in P0_REASON_PRECEDENCE
        else len(P0_REASON_PRECEDENCE)
    )
    metric = {
        "utxo_ge_100_btc": row.current_utxo_sats,
        "same_tx_receive_ge_500_btc": row.max_same_tx_received_sats,
        "gross_90d_ge_1000_btc": row.gross_flow_90d_sats,
        "lifetime_ge_10000_active_365d": row.lifetime_received_sats,
    }.get(reason, 0)
    return (
        len(P0_REASON_PRECEDENCE) - precedence,
        metric,
        -int(address_id, 16),
    )


def _anchor_only_rank(
    *,
    address_id: str,
    reasons: tuple[str, ...],
) -> tuple[int, int, int]:
    precedence = min(
        (
            P0_REASON_PRECEDENCE.index(reason)
            for reason in reasons
            if reason in P0_REASON_PRECEDENCE
        ),
        default=len(P0_REASON_PRECEDENCE),
    )
    return (
        len(P0_REASON_PRECEDENCE) - precedence,
        0,
        -int(address_id, 16),
    )


def _quota_groups(cohort_names: tuple[str, ...]) -> tuple[str, ...]:
    groups = {
        name
        for name in cohort_names
        if name
        in {
            "current_capital",
            "historical_large_receipt",
            "high_turnover",
            "dormant_holder",
            "high_value_connector",
        }
    }
    if "calibration" in cohort_names or "control" in cohort_names:
        groups.add("calibration_control")
    return tuple(sorted(groups))


def _allocate_quotas(slots: int) -> dict[str, int]:
    quotas = {
        group: slots * percent // 100
        for group, percent in P1_QUOTAS
    }
    remaining = slots - sum(quotas.values())
    remainders = sorted(
        (
            (slots * percent % 100, -index, group)
            for index, (group, percent) in enumerate(P1_QUOTAS)
        ),
        reverse=True,
    )
    for _, _, group in remainders[:remaining]:
        quotas[group] += 1
    return quotas


def _validate_capacity_inputs(
    *,
    runtime_minutes: int,
    requests_per_minute: int,
    estimated_points_per_address: int | None,
    discovery_point_budget: int,
) -> None:
    if runtime_minutes <= 0:
        raise ValueError("runtime_minutes must be positive")
    if requests_per_minute <= 0:
        raise ValueError("requests_per_minute must be positive")
    if estimated_points_per_address is not None and estimated_points_per_address <= 0:
        raise ValueError("estimated_points_per_address must be positive")
    if discovery_point_budget < 0:
        raise ValueError("discovery_point_budget must be non-negative")
