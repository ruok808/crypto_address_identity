"""Cost-pinned BTC entity fanout and V2-S coverage state snapshots."""

from __future__ import annotations

import gzip
import hashlib
import json
import math
import os
import shutil
import time
import uuid
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from crypto_address_identity.candidates import (
    ByteBudgetExceeded,
    QuotaManager,
    RateLimitExceeded,
)
from crypto_address_identity.chains.bitcoin import (
    BitcoinAddressError,
    normalize_bitcoin_address,
)
from crypto_address_identity.core.config import Settings
from crypto_address_identity.coverage import (
    CoveragePayloadError,
    parse_prediction_addresses,
)
from crypto_address_identity.providers.zero_x_router import (
    ProviderTokenMissing,
    ZeroXRouterClient,
)
from crypto_address_identity.storage.raw_payloads import RawPayloadStore
from crypto_address_identity.storage.sqlite import IdentityDatabase


_PREDICTION_ESTIMATED_BYTES = 512_000
_POINT_UNIT_BYTES = 100_000
_MAX_ENTITY_REQUESTS_PER_RUN = 500
_MAX_CONSECUTIVE_FAILURES = 3
_TERMINAL_PARSE_OUTCOMES = ("parsed_success", "no_bitcoin_addresses")
_COVERAGE_STATES = (
    "direct_enriched",
    "entity_membership_covered",
    "local_evidence_covered",
    "needs_direct_enrichment",
)


class EntityFanoutArtifactError(RuntimeError):
    """Raised when a checksum-pinned bootstrap artifact is incomplete."""


@dataclass(frozen=True)
class CanaryEntitySeedResult:
    canary_id: str
    entity_ids: tuple[str, ...]
    entity_frequencies: tuple[tuple[str, int], ...]
    direct_enriched_addresses: tuple[str, ...]
    entity_labeled_addresses: int
    duplicate_entity_mentions: int
    verified_payloads: int
    source_reference: str
    requested_at: datetime


@dataclass(frozen=True)
class EntityFanoutResult:
    status: str
    dry_run: bool
    campaign_id: str
    run_id: str | None
    input_entities: int
    local_entities: int
    merged_unique_entities: int
    terminal_cached_entities: int
    campaign_attempted_entities: int
    planned_entities: int
    requests: int
    successful_entities: int
    parsed_entities: int
    no_bitcoin_entities: int
    failed_entities: int
    unique_prediction_addresses: int
    prediction_memberships_inserted: int
    duplicate_memberships: int
    response_bytes: int
    response_bytes_max: int
    response_bytes_p50: int
    response_bytes_p95: int
    estimated_points: int
    outcome_counts: dict[str, int]
    written_paths: tuple[str, ...]


@dataclass(frozen=True)
class CoverageSnapshotResult:
    status: str
    snapshot_id: str
    candidate_rows: int
    state_counts: dict[str, int]
    intersected_prediction_addresses: int
    prediction_addresses_outside_candidates: int
    direct_enriched_intersection: int
    local_evidence_intersection: int
    active_conflict_intersection: int
    explicit_direct_requirement_intersection: int
    parquet_path: Path
    parquet_sha256: str
    manifest_path: Path
    manifest_sha256: str


class CanaryEntitySeedReader:
    """Read verified Arkham entity IDs from a completed address canary."""

    def __init__(self, canary_root: Path) -> None:
        self.canary_root = Path(canary_root)

    def read(self) -> CanaryEntitySeedResult:
        ledger_path = self.canary_root / "request_ledger.jsonl"
        if not ledger_path.is_file():
            raise EntityFanoutArtifactError("canary request ledger is missing")
        ledger_sha256 = _file_sha256(ledger_path)
        entity_counts: Counter[str] = Counter()
        direct_addresses: set[str] = set()
        verified_payloads = 0
        completed_at: list[datetime] = []

        for line_number, line in enumerate(
            ledger_path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise EntityFanoutArtifactError(
                    f"canary ledger row {line_number} is not JSON"
                ) from exc
            if not isinstance(row, dict):
                raise EntityFanoutArtifactError(
                    f"canary ledger row {line_number} is not an object"
                )
            if row.get("outcome") != "success" or row.get(
                "parse_outcome"
            ) != "parsed_success":
                continue
            payload_sha256 = row.get("payload_sha256")
            relative_path = row.get("raw_relative_path")
            if not _is_sha256(payload_sha256) or not isinstance(
                relative_path, str
            ):
                raise EntityFanoutArtifactError(
                    f"canary ledger row {line_number} has invalid payload metadata"
                )
            raw_path = _safe_child(
                self.canary_root / "raw", relative_path
            )
            if not raw_path.is_file():
                raise EntityFanoutArtifactError(
                    f"canary payload for row {line_number} is missing"
                )
            try:
                payload = gzip.decompress(raw_path.read_bytes())
            except (OSError, EOFError) as exc:
                raise EntityFanoutArtifactError(
                    f"canary payload for row {line_number} is corrupt"
                ) from exc
            if hashlib.sha256(payload).hexdigest() != payload_sha256:
                raise EntityFanoutArtifactError(
                    f"canary payload for row {line_number} failed checksum"
                )
            verified_payloads += 1
            root = _json_object(payload, context="canary payload")
            bitcoin = root.get("bitcoin")
            if not isinstance(bitcoin, dict):
                raise EntityFanoutArtifactError(
                    f"canary payload for row {line_number} has no Bitcoin root"
                )
            raw_address = bitcoin.get("address")
            if not isinstance(raw_address, str):
                raise EntityFanoutArtifactError(
                    f"canary payload for row {line_number} has no address"
                )
            try:
                direct_addresses.add(
                    normalize_bitcoin_address(raw_address).normalized_address
                )
            except BitcoinAddressError as exc:
                raise EntityFanoutArtifactError(
                    f"canary payload for row {line_number} has invalid address"
                ) from exc
            entity = bitcoin.get("arkhamEntity")
            if isinstance(entity, dict):
                entity_id = entity.get("id") or entity.get("entityId")
                if isinstance(entity_id, str) and entity_id.strip():
                    entity_counts[_normalize_entity_id(entity_id)] += 1
            raw_completed_at = row.get("completed_at")
            if isinstance(raw_completed_at, str):
                try:
                    completed_at.append(_parse_utc(raw_completed_at))
                except ValueError:
                    pass

        frequencies = tuple(
            sorted(entity_counts.items(), key=lambda item: (-item[1], item[0]))
        )
        if not frequencies:
            raise EntityFanoutArtifactError(
                "canary contains no valid provider entity IDs"
            )
        requested_at = min(completed_at) if completed_at else datetime.now(UTC)
        return CanaryEntitySeedResult(
            canary_id=self.canary_root.name,
            entity_ids=tuple(entity_id for entity_id, _ in frequencies),
            entity_frequencies=frequencies,
            direct_enriched_addresses=tuple(sorted(direct_addresses)),
            entity_labeled_addresses=sum(entity_counts.values()),
            duplicate_entity_mentions=sum(entity_counts.values())
            - len(entity_counts),
            verified_payloads=verified_payloads,
            source_reference=(
                f"canary:{self.canary_root.name}:ledger-sha256:{ledger_sha256}"
            ),
            requested_at=requested_at,
        )


class BtcEntityFanoutService:
    """Request each unique entity prediction list once per bootstrap campaign."""

    def __init__(
        self,
        *,
        database: IdentityDatabase,
        settings: Settings,
        provider: ZeroXRouterClient,
        raw_payloads: RawPayloadStore,
        sleeper: Callable[[float], None] = time.sleep,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.database = database
        self.settings = settings
        self.provider = provider
        self.raw_payloads = raw_payloads
        self.quota = QuotaManager(database)
        self.sleeper = sleeper
        self.clock = clock or (lambda: datetime.now(UTC))

    def run(
        self,
        *,
        entity_ids: Iterable[str],
        dry_run: bool,
        request_limit: int,
        campaign_id: str = "btc-v2s-bootstrap-959187",
        include_local_entities: bool = True,
        now: datetime | None = None,
    ) -> EntityFanoutResult:
        if request_limit < 1 or request_limit > _MAX_ENTITY_REQUESTS_PER_RUN:
            raise ValueError("request_limit must be between 1 and 500")
        campaign_id = _normalize_campaign_id(campaign_id)
        input_ids = _ordered_entity_ids(entity_ids)
        local_ids = self._local_entity_ids() if include_local_entities else ()
        merged_ids = _ordered_entity_ids((*input_ids, *local_ids))
        terminal = self._terminal_entity_ids()
        attempted = self._campaign_attempted_entity_ids(campaign_id)
        due = tuple(
            entity_id
            for entity_id in merged_ids
            if entity_id not in terminal and entity_id not in attempted
        )
        planned = due[:request_limit]

        if dry_run:
            return EntityFanoutResult(
                status="dry_run",
                dry_run=True,
                campaign_id=campaign_id,
                run_id=None,
                input_entities=len(input_ids),
                local_entities=len(local_ids),
                merged_unique_entities=len(merged_ids),
                terminal_cached_entities=len(set(merged_ids) & terminal),
                campaign_attempted_entities=len(set(merged_ids) & attempted),
                planned_entities=len(planned),
                requests=0,
                successful_entities=0,
                parsed_entities=0,
                no_bitcoin_entities=0,
                failed_entities=0,
                unique_prediction_addresses=0,
                prediction_memberships_inserted=0,
                duplicate_memberships=0,
                response_bytes=0,
                response_bytes_max=0,
                response_bytes_p50=0,
                response_bytes_p95=0,
                estimated_points=0,
                outcome_counts={},
                written_paths=(),
            )
        if self.settings.provider_token_value() is None:
            raise ProviderTokenMissing()

        started_at = _as_utc(now or self.clock())
        run_id = self.quota.create_run(
            mode="execute",
            request_limit=self.settings.coverage_requests_per_minute,
            response_bytes_budget=self.settings.coverage_response_bytes_budget,
            started_at=started_at,
        )
        interval_seconds = 60.0 / self.settings.coverage_requests_per_minute
        response_sizes: list[int] = []
        points = 0
        successful = 0
        parsed_entities = 0
        no_bitcoin_entities = 0
        failed = 0
        inserted_memberships = 0
        duplicate_memberships = 0
        prediction_addresses: set[str] = set()
        outcomes: Counter[str] = Counter()
        written_paths: list[str] = []
        blocked = False
        request_count = 0
        consecutive_failures = 0

        for index, entity_id in enumerate(planned):
            if index:
                self.sleeper(interval_seconds)
            requested_at = (
                started_at + timedelta(seconds=interval_seconds * index)
                if now is not None
                else _as_utc(self.clock())
            )
            try:
                reservation = self.quota.reserve(
                    run_id=run_id,
                    now=requested_at,
                    estimated_response_bytes=_PREDICTION_ESTIMATED_BYTES,
                )
            except RateLimitExceeded:
                outcomes["rate_limited"] += 1
                blocked = True
                break
            except ByteBudgetExceeded:
                outcomes["budget_exhausted"] += 1
                blocked = True
                break

            response = self.provider.fetch_request_once(
                self.provider.build_entity_predictions_request(entity_id)
            )
            request_count += 1
            stored = (
                self.raw_payloads.persist(response.body)
                if response.body
                else None
            )
            response_size = len(response.body)
            response_sizes.append(response_size)
            budget_exceeded = (
                sum(response_sizes)
                > self.settings.coverage_response_bytes_budget
            )
            points += _request_points(response_size)
            if stored is not None:
                written_paths.append(stored.relative_path)
            observation_id = str(uuid.uuid4())
            self._record_observation(
                observation_id=observation_id,
                run_id=run_id,
                campaign_id=campaign_id,
                requested_at=requested_at,
                http_status=response.http_status,
                outcome=response.outcome,
                response_bytes=response_size,
                payload_sha256=(
                    stored.payload_sha256 if stored is not None else None
                ),
            )
            self.quota.complete(
                reservation.reservation_id,
                actual_response_bytes=response_size,
                outcome=(
                    "completed"
                    if response.outcome == "success"
                    else "rate_limited"
                    if response.outcome == "rate_limited"
                    else "failed"
                ),
            )
            self._record_prediction_attempt(
                observation_id=observation_id,
                entity_id=entity_id,
                outcome=response.outcome,
                attempted_at=requested_at,
            )
            outcomes[response.outcome] += 1
            if budget_exceeded:
                outcomes["budget_exhausted"] += 1
            if response.outcome != "success":
                failed += 1
                consecutive_failures += 1
                if (
                    budget_exceeded
                    or response.outcome == "rate_limited"
                    or response.http_status in {401, 402, 403}
                    or consecutive_failures >= _MAX_CONSECUTIVE_FAILURES
                ):
                    blocked = True
                    break
                continue
            consecutive_failures = 0
            successful += 1
            try:
                addresses = parse_prediction_addresses(response.body)
            except CoveragePayloadError:
                self._record_prediction_parse_result(
                    observation_id=observation_id,
                    entity_id=entity_id,
                    parse_outcome="malformed_payload",
                    parsed_at=requested_at,
                )
                outcomes["malformed_payload"] += 1
                failed += 1
                if budget_exceeded:
                    blocked = True
                    break
                continue
            parse_outcome = (
                "parsed_success" if addresses else "no_bitcoin_addresses"
            )
            self._record_prediction_parse_result(
                observation_id=observation_id,
                entity_id=entity_id,
                parse_outcome=parse_outcome,
                parsed_at=requested_at,
            )
            if addresses:
                parsed_entities += 1
            else:
                no_bitcoin_entities += 1
            prediction_addresses.update(addresses)
            inserted, duplicates = self._record_predictions(
                observation_id=observation_id,
                entity_id=entity_id,
                addresses=addresses,
                observed_at=requested_at,
            )
            inserted_memberships += inserted
            duplicate_memberships += duplicates
            if budget_exceeded:
                blocked = True
                break

        status = "blocked" if blocked else "completed"
        if not blocked and (failed or outcomes.get("malformed_payload", 0)):
            status = "partial"
        self._complete_run(
            run_id,
            status=status,
            campaign_id=campaign_id,
            outcomes=outcomes,
            points=points,
        )
        return EntityFanoutResult(
            status=status,
            dry_run=False,
            campaign_id=campaign_id,
            run_id=run_id,
            input_entities=len(input_ids),
            local_entities=len(local_ids),
            merged_unique_entities=len(merged_ids),
            terminal_cached_entities=len(set(merged_ids) & terminal),
            campaign_attempted_entities=len(set(merged_ids) & attempted),
            planned_entities=len(planned),
            requests=request_count,
            successful_entities=successful,
            parsed_entities=parsed_entities,
            no_bitcoin_entities=no_bitcoin_entities,
            failed_entities=failed,
            unique_prediction_addresses=len(prediction_addresses),
            prediction_memberships_inserted=inserted_memberships,
            duplicate_memberships=duplicate_memberships,
            response_bytes=sum(response_sizes),
            response_bytes_max=max(response_sizes, default=0),
            response_bytes_p50=_nearest_rank(response_sizes, 0.50),
            response_bytes_p95=_nearest_rank(response_sizes, 0.95),
            estimated_points=points,
            outcome_counts=dict(outcomes),
            written_paths=tuple(written_paths),
        )

    def _local_entity_ids(self) -> tuple[str, ...]:
        with self.database.read_connection() as connection:
            rows = connection.execute(
                """
                SELECT provider_entity_id FROM coverage_entity_seed
                UNION
                SELECT provider_entity_id FROM coverage_entity_observation
                UNION
                SELECT provider_entity_id FROM coverage_entity_prediction
                ORDER BY provider_entity_id
                """
            ).fetchall()
        return tuple(row["provider_entity_id"] for row in rows)

    def _terminal_entity_ids(self) -> set[str]:
        placeholders = ",".join("?" for _ in _TERMINAL_PARSE_OUTCOMES)
        with self.database.read_connection() as connection:
            rows = connection.execute(
                f"""
                SELECT DISTINCT provider_entity_id
                FROM coverage_entity_prediction_parse_result
                WHERE parse_outcome IN ({placeholders})
                """,
                _TERMINAL_PARSE_OUTCOMES,
            ).fetchall()
        return {row["provider_entity_id"] for row in rows}

    def _campaign_attempted_entity_ids(self, campaign_id: str) -> set[str]:
        query_profile = _campaign_query_profile(campaign_id)
        with self.database.read_connection() as connection:
            rows = connection.execute(
                """
                SELECT DISTINCT attempt.provider_entity_id
                FROM coverage_entity_prediction_attempt AS attempt
                JOIN source_observation AS observation
                  ON observation.observation_id = attempt.observation_id
                WHERE observation.query_profile = ?
                """,
                (query_profile,),
            ).fetchall()
        return {row["provider_entity_id"] for row in rows}

    def _record_observation(
        self,
        *,
        observation_id: str,
        run_id: str,
        campaign_id: str,
        requested_at: datetime,
        http_status: int | None,
        outcome: str,
        response_bytes: int,
        payload_sha256: str | None,
    ) -> None:
        timestamp = _utc_string(requested_at)
        with self.database.write_transaction() as connection:
            connection.execute(
                """
                INSERT INTO source_observation (
                    observation_id, source_id, source_version, source_kind,
                    endpoint_template, query_profile, requested_at,
                    completed_at, http_status, outcome, response_bytes,
                    payload_sha256, chain_key, address_id, ingestion_run_id
                ) VALUES (
                    ?, '0xrouter', 'chaindata_entity_fanout_v1', 'provider',
                    '/chaindata/intelligence/entity_predictions/{entity}',
                    ?, ?, ?, ?, ?, ?, ?, 'bitcoin', NULL, ?
                )
                """,
                (
                    observation_id,
                    _campaign_query_profile(campaign_id),
                    timestamp,
                    timestamp,
                    http_status,
                    outcome,
                    response_bytes,
                    payload_sha256,
                    run_id,
                ),
            )

    def _record_prediction_attempt(
        self,
        *,
        observation_id: str,
        entity_id: str,
        outcome: str,
        attempted_at: datetime,
    ) -> None:
        fingerprint = _json_sha256(
            {
                "observation_id": observation_id,
                "provider_entity_id": entity_id,
                "outcome": outcome,
            }
        )
        with self.database.write_transaction() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO coverage_entity_prediction_attempt (
                    prediction_attempt_id, prediction_attempt_fingerprint,
                    observation_id, provider_entity_id, outcome, attempted_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    fingerprint,
                    observation_id,
                    entity_id,
                    outcome,
                    _utc_string(attempted_at),
                ),
            )

    def _record_prediction_parse_result(
        self,
        *,
        observation_id: str,
        entity_id: str,
        parse_outcome: str,
        parsed_at: datetime,
    ) -> None:
        fingerprint = _json_sha256(
            {
                "observation_id": observation_id,
                "provider_entity_id": entity_id,
                "parse_outcome": parse_outcome,
            }
        )
        with self.database.write_transaction() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO coverage_entity_prediction_parse_result (
                    prediction_parse_result_id,
                    prediction_parse_result_fingerprint, observation_id,
                    provider_entity_id, parse_outcome, parsed_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    fingerprint,
                    observation_id,
                    entity_id,
                    parse_outcome,
                    _utc_string(parsed_at),
                ),
            )

    def _record_predictions(
        self,
        *,
        observation_id: str,
        entity_id: str,
        addresses: tuple[str, ...],
        observed_at: datetime,
    ) -> tuple[int, int]:
        inserted = 0
        duplicates = 0
        timestamp = _utc_string(observed_at)
        with self.database.write_transaction() as connection:
            for rank, address in enumerate(addresses, start=1):
                subject = normalize_bitcoin_address(address)
                connection.execute(
                    """
                    INSERT OR IGNORE INTO address_subject (
                        address_id, chain_key, normalized_address,
                        display_address, address_type, first_seen_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        subject.address_id,
                        subject.chain_key,
                        subject.normalized_address,
                        subject.display_address,
                        subject.address_type,
                        timestamp,
                    ),
                )
                fingerprint = _json_sha256(
                    {
                        "observation_id": observation_id,
                        "provider_entity_id": entity_id,
                        "address_id": subject.address_id,
                        "prediction_rank": rank,
                    }
                )
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO coverage_entity_prediction (
                        prediction_id, prediction_fingerprint, observation_id,
                        provider_entity_id, address_id, prediction_rank,
                        observed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        fingerprint,
                        observation_id,
                        entity_id,
                        subject.address_id,
                        rank,
                        timestamp,
                    ),
                )
                if cursor.rowcount == 1:
                    inserted += 1
                else:
                    duplicates += 1
        return inserted, duplicates

    def _complete_run(
        self,
        run_id: str,
        *,
        status: str,
        campaign_id: str,
        outcomes: Counter[str],
        points: int,
    ) -> None:
        summary = {
            "coverage_mode": "btc_v2s_entity_fanout",
            "campaign_id": campaign_id,
            "estimated_points": points,
            "outcomes": dict(outcomes),
        }
        with self.database.write_transaction() as connection:
            connection.execute(
                """
                UPDATE ingestion_run
                SET status = ?, completed_at = ?, summary_json = ?
                WHERE ingestion_run_id = ?
                """,
                (
                    status,
                    _utc_string(_as_utc(self.clock())),
                    json.dumps(summary, sort_keys=True),
                    run_id,
                ),
            )


class BtcV2SCoverageSnapshotBuilder:
    """Intersect local identity coverage with the immutable V2-S candidates."""

    def __init__(self, database: IdentityDatabase) -> None:
        self.database = database

    def build(
        self,
        *,
        campaign_root: Path,
        canary_root: Path,
        output_root: Path,
        built_at: datetime | None = None,
    ) -> CoverageSnapshotResult:
        built_at = _as_utc(built_at or datetime.now(UTC))
        campaign_root = Path(campaign_root)
        output_root = Path(output_root)
        manifest_path = campaign_root / "manifest.json"
        manifest = _read_json_file(
            manifest_path, context="candidate manifest"
        )
        files = _verified_candidate_files(
            campaign_root=campaign_root, manifest=manifest
        )
        expected_rows = manifest.get("candidate_rows")
        if not isinstance(expected_rows, int) or expected_rows < 1:
            raise EntityFanoutArtifactError(
                "candidate manifest row count is invalid"
            )
        source_manifest_file_sha256 = _file_sha256(manifest_path)
        canary = CanaryEntitySeedReader(canary_root).read()
        coverage = self._coverage_inputs()
        direct_addresses = set(canary.direct_enriched_addresses)
        direct_addresses.update(coverage["direct"])
        memberships: dict[str, tuple[str, ...]] = coverage["memberships"]
        membership_addresses = set(memberships)
        local_evidence_addresses: set[str] = coverage["local_evidence"]
        active_conflicts: set[str] = coverage["active_conflicts"]
        explicit_direct: set[str] = coverage["explicit_direct"]

        snapshot_id = (
            built_at.strftime("%Y%m%dT%H%M%SZ")
            + "-"
            + source_manifest_file_sha256[:12]
        )
        final_root = output_root / snapshot_id
        if final_root.exists():
            raise EntityFanoutArtifactError(
                "coverage snapshot already exists"
            )
        staging_root = output_root / f".staging-{snapshot_id}-{uuid.uuid4().hex}"
        staging_root.mkdir(parents=True, mode=0o700)
        parquet_path = staging_root / "btc_v2s_coverage_state.parquet"
        writer: pq.ParquetWriter | None = None
        state_counts: Counter[str] = Counter()
        candidate_addresses: set[str] = set()
        direct_intersection = 0
        evidence_intersection = 0
        conflict_intersection = 0
        explicit_intersection = 0
        row_count = 0
        try:
            for candidate_file in files:
                parquet = pq.ParquetFile(candidate_file)
                for batch in parquet.iter_batches(
                    batch_size=65_536,
                    columns=(
                        "normalized_address",
                        "candidate_tier",
                        "candidate_row_sha256",
                    ),
                ):
                    addresses = batch.column(0).to_pylist()
                    tiers = batch.column(1).to_pylist()
                    row_hashes = batch.column(2).to_pylist()
                    states: list[str] = []
                    entity_ids: list[list[str]] = []
                    direct_flags: list[bool] = []
                    membership_flags: list[bool] = []
                    evidence_flags: list[bool] = []
                    conflict_flags: list[bool] = []
                    explicit_flags: list[bool] = []
                    reason_codes: list[list[str]] = []
                    for address in addresses:
                        if not isinstance(address, str) or not address:
                            raise EntityFanoutArtifactError(
                                "candidate artifact contains an invalid address"
                            )
                        if address in candidate_addresses:
                            raise EntityFanoutArtifactError(
                                "candidate artifact contains a duplicate address"
                            )
                        candidate_addresses.add(address)
                        is_direct = address in direct_addresses
                        has_membership = address in membership_addresses
                        has_evidence = address in local_evidence_addresses
                        has_conflict = address in active_conflicts
                        has_explicit = address in explicit_direct
                        reasons: list[str] = []
                        if is_direct:
                            state = "direct_enriched"
                            reasons.append("direct_provider_response")
                            direct_intersection += 1
                        elif has_conflict or has_explicit:
                            state = "needs_direct_enrichment"
                            if has_conflict:
                                reasons.append("active_identity_conflict")
                            if has_explicit:
                                reasons.append(
                                    "explicit_address_level_requirement"
                                )
                        elif has_membership:
                            state = "entity_membership_covered"
                            reasons.append("provider_entity_prediction")
                        elif has_evidence:
                            state = "local_evidence_covered"
                            reasons.append("valid_local_entity_evidence")
                        else:
                            state = "needs_direct_enrichment"
                            reasons.append("no_explicit_entity_membership")
                        if has_evidence:
                            evidence_intersection += 1
                        if has_conflict:
                            conflict_intersection += 1
                        if has_explicit:
                            explicit_intersection += 1
                        state_counts[state] += 1
                        states.append(state)
                        entity_ids.append(list(memberships.get(address, ())))
                        direct_flags.append(is_direct)
                        membership_flags.append(has_membership)
                        evidence_flags.append(has_evidence)
                        conflict_flags.append(has_conflict)
                        explicit_flags.append(has_explicit)
                        reason_codes.append(reasons)
                    output_batch = pa.record_batch(
                        [
                            pa.array(addresses, type=pa.string()),
                            pa.array(tiers, type=pa.string()),
                            pa.array(row_hashes, type=pa.string()),
                            pa.array(states, type=pa.string()),
                            pa.array(
                                entity_ids, type=pa.list_(pa.string())
                            ),
                            pa.array(direct_flags, type=pa.bool_()),
                            pa.array(membership_flags, type=pa.bool_()),
                            pa.array(evidence_flags, type=pa.bool_()),
                            pa.array(conflict_flags, type=pa.bool_()),
                            pa.array(explicit_flags, type=pa.bool_()),
                            pa.array(
                                reason_codes, type=pa.list_(pa.string())
                            ),
                        ],
                        names=(
                            "normalized_address",
                            "candidate_tier",
                            "candidate_row_sha256",
                            "coverage_state",
                            "provider_entity_ids",
                            "direct_enriched",
                            "entity_membership_covered",
                            "local_evidence_covered",
                            "active_conflict",
                            "explicit_direct_requirement",
                            "coverage_reason_codes",
                        ),
                    )
                    if writer is None:
                        writer = pq.ParquetWriter(
                            parquet_path,
                            output_batch.schema,
                            compression="zstd",
                            version="2.6",
                            write_statistics=True,
                        )
                    writer.write_batch(output_batch)
                    row_count += output_batch.num_rows
            if writer is not None:
                writer.close()
                writer = None
            if row_count != expected_rows:
                raise EntityFanoutArtifactError(
                    "coverage snapshot row count does not match source manifest"
                )
            if set(state_counts) - set(_COVERAGE_STATES):
                raise EntityFanoutArtifactError(
                    "coverage snapshot produced an unknown state"
                )
            parquet_sha256 = _file_sha256(parquet_path)
            prediction_intersection = len(
                membership_addresses & candidate_addresses
            )
            snapshot_manifest = {
                "schema_version": "btc_v2s_coverage_state_v1",
                "snapshot_id": snapshot_id,
                "built_at": _utc_string(built_at),
                "source_campaign_id": manifest.get("campaign_id"),
                "source_manifest_file_sha256": source_manifest_file_sha256,
                "source_manifest_declared_sha256": manifest.get(
                    "manifest_sha256"
                ),
                "source_candidate_rows": expected_rows,
                "canary_id": canary.canary_id,
                "canary_source_reference": canary.source_reference,
                "state_counts": {
                    state: state_counts.get(state, 0)
                    for state in _COVERAGE_STATES
                },
                "intersected_prediction_addresses": prediction_intersection,
                "prediction_addresses_outside_candidates": len(
                    membership_addresses - candidate_addresses
                ),
                "direct_enriched_intersection": direct_intersection,
                "local_evidence_intersection": evidence_intersection,
                "active_conflict_intersection": conflict_intersection,
                "explicit_direct_requirement_intersection": (
                    explicit_intersection
                ),
                "files": [
                    {
                        "path": parquet_path.name,
                        "row_count": row_count,
                        "size": parquet_path.stat().st_size,
                        "sha256": parquet_sha256,
                    }
                ],
            }
            manifest_sha256 = _json_sha256(snapshot_manifest)
            snapshot_manifest["manifest_sha256"] = manifest_sha256
            generated_manifest_path = staging_root / "manifest.json"
            generated_manifest_path.write_text(
                json.dumps(
                    snapshot_manifest,
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            parquet_path.chmod(0o600)
            generated_manifest_path.chmod(0o600)
            output_root.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.replace(staging_root, final_root)
        except Exception:
            if writer is not None:
                writer.close()
            shutil.rmtree(staging_root, ignore_errors=True)
            raise

        final_parquet = final_root / parquet_path.name
        final_manifest = final_root / "manifest.json"
        return CoverageSnapshotResult(
            status="published",
            snapshot_id=snapshot_id,
            candidate_rows=row_count,
            state_counts={
                state: state_counts.get(state, 0)
                for state in _COVERAGE_STATES
            },
            intersected_prediction_addresses=prediction_intersection,
            prediction_addresses_outside_candidates=len(
                membership_addresses - candidate_addresses
            ),
            direct_enriched_intersection=direct_intersection,
            local_evidence_intersection=evidence_intersection,
            active_conflict_intersection=conflict_intersection,
            explicit_direct_requirement_intersection=explicit_intersection,
            parquet_path=final_parquet,
            parquet_sha256=parquet_sha256,
            manifest_path=final_manifest,
            manifest_sha256=manifest_sha256,
        )

    def _coverage_inputs(self) -> dict[str, Any]:
        memberships: dict[str, set[str]] = defaultdict(set)
        with self.database.read_connection() as connection:
            for row in connection.execute(
                """
                SELECT address.normalized_address,
                       prediction.provider_entity_id
                FROM coverage_entity_prediction AS prediction
                JOIN address_subject AS address
                  ON address.address_id = prediction.address_id
                """
            ).fetchall():
                memberships[row["normalized_address"]].add(
                    row["provider_entity_id"]
                )
            direct = {
                row["normalized_address"]
                for row in connection.execute(
                    """
                    SELECT DISTINCT address.normalized_address
                    FROM coverage_address_parse_result AS parse_result
                    JOIN address_subject AS address
                      ON address.address_id = parse_result.address_id
                    WHERE parse_result.parse_outcome = 'parsed_success'
                    """
                ).fetchall()
            }
            local_evidence = {
                row["normalized_address"]
                for row in connection.execute(
                    """
                    SELECT DISTINCT address.normalized_address
                    FROM identity_evidence AS evidence
                    JOIN address_subject AS address
                      ON address.address_id = evidence.address_id
                    WHERE evidence.assertion_type = 'entity_control'
                      AND evidence.evidence_status = 'valid'
                    """
                ).fetchall()
            }
            active_conflicts = {
                row["normalized_address"]
                for row in connection.execute(
                    """
                    SELECT DISTINCT address.normalized_address
                    FROM conflict_set AS conflict
                    JOIN address_subject AS address
                      ON address.address_id = conflict.address_id
                    WHERE conflict.status = 'active'
                    """
                ).fetchall()
            }
            explicit_direct = {
                row["normalized_address"]
                for row in connection.execute(
                    """
                    SELECT DISTINCT address.normalized_address
                    FROM candidate_request AS request
                    JOIN address_subject AS address
                      ON address.address_id = request.address_id
                    """
                ).fetchall()
            }
        return {
            "memberships": {
                address: tuple(sorted(entity_ids))
                for address, entity_ids in memberships.items()
            },
            "direct": direct,
            "local_evidence": local_evidence,
            "active_conflicts": active_conflicts,
            "explicit_direct": explicit_direct,
        }


def _verified_candidate_files(
    *, campaign_root: Path, manifest: dict[str, Any]
) -> tuple[Path, ...]:
    records = manifest.get("files")
    if not isinstance(records, list) or not records:
        raise EntityFanoutArtifactError(
            "candidate manifest has no files"
        )
    files: list[Path] = []
    declared_rows = 0
    for record in records:
        if not isinstance(record, dict):
            raise EntityFanoutArtifactError(
                "candidate manifest file record is invalid"
            )
        relative_path = record.get("path")
        expected_sha256 = record.get("sha256")
        expected_size = record.get("size")
        expected_rows = record.get("row_count")
        if (
            not isinstance(relative_path, str)
            or not _is_sha256(expected_sha256)
            or not isinstance(expected_size, int)
            or expected_size < 1
        ):
            raise EntityFanoutArtifactError(
                "candidate manifest file metadata is incomplete"
            )
        is_candidate = (
            relative_path.startswith("candidates/")
            and relative_path.endswith(".parquet")
        )
        is_execution_receipt = (
            relative_path == "execution_receipt.json"
            and expected_rows is None
        )
        if is_candidate and (
            not isinstance(expected_rows, int) or expected_rows < 0
        ):
            raise EntityFanoutArtifactError(
                "candidate manifest row metadata is incomplete"
            )
        if not is_candidate and not is_execution_receipt:
            raise EntityFanoutArtifactError(
                "candidate manifest contains an unsupported artifact"
            )
        path = _safe_child(campaign_root, relative_path)
        if not path.is_file():
            raise EntityFanoutArtifactError(
                "candidate manifest file is missing"
            )
        if path.stat().st_size != expected_size:
            raise EntityFanoutArtifactError(
                "candidate manifest file size mismatch"
            )
        if _file_sha256(path) != expected_sha256:
            raise EntityFanoutArtifactError(
                "candidate manifest file checksum mismatch"
            )
        if is_candidate:
            files.append(path)
            declared_rows += expected_rows
    if not files:
        raise EntityFanoutArtifactError(
            "candidate manifest has no candidate parquet files"
        )
    if declared_rows != manifest.get("candidate_rows"):
        raise EntityFanoutArtifactError(
            "candidate manifest file row counts do not reconcile"
        )
    return tuple(files)


def _ordered_entity_ids(entity_ids: Iterable[str]) -> tuple[str, ...]:
    selected: dict[str, None] = {}
    for entity_id in entity_ids:
        selected.setdefault(_normalize_entity_id(entity_id), None)
    return tuple(selected)


def _normalize_entity_id(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("provider entity ID must be text")
    normalized = value.strip()
    if (
        not normalized
        or len(normalized) > 256
        or any(character.isspace() for character in normalized)
    ):
        raise ValueError("provider entity ID is invalid")
    return normalized


def _normalize_campaign_id(value: str) -> str:
    normalized = value.strip()
    if (
        not normalized
        or len(normalized) > 128
        or any(
            character not in "abcdefghijklmnopqrstuvwxyz0123456789._-"
            for character in normalized
        )
    ):
        raise ValueError("campaign ID is invalid")
    return normalized


def _campaign_query_profile(campaign_id: str) -> str:
    return f"btc_v2s_entity_fanout:{campaign_id}"


def _request_points(response_bytes: int) -> int:
    return max(1, math.ceil(response_bytes / _POINT_UNIT_BYTES))


def _nearest_rank(values: list[int], quantile: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    index = max(0, math.ceil(quantile * len(ordered)) - 1)
    return ordered[index]


def _safe_child(root: Path, relative_path: str) -> Path:
    relative = Path(relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise EntityFanoutArtifactError("artifact path is unsafe")
    resolved_root = root.resolve()
    candidate = (resolved_root / relative).resolve()
    if not candidate.is_relative_to(resolved_root):
        raise EntityFanoutArtifactError("artifact path escapes root")
    return candidate


def _read_json_file(path: Path, *, context: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EntityFanoutArtifactError(f"{context} is unreadable") from exc
    if not isinstance(value, dict):
        raise EntityFanoutArtifactError(f"{context} is not an object")
    return value


def _json_object(payload: bytes, *, context: str) -> dict[str, Any]:
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EntityFanoutArtifactError(f"{context} is not JSON") from exc
    if not isinstance(value, dict):
        raise EntityFanoutArtifactError(f"{context} is not an object")
    return value


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_sha256(value: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _utc_string(value: datetime) -> str:
    return _as_utc(value).isoformat().replace("+00:00", "Z")
