"""Checksum-pinned BTC V2-S address queues and one-shot enrichment campaigns."""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import time
import uuid
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import pyarrow as pa
import pyarrow.parquet as pq

from crypto_address_identity.candidates import (
    ByteBudgetExceeded,
    QuotaManager,
    RateLimitExceeded,
)
from crypto_address_identity.chains.bitcoin import normalize_bitcoin_address
from crypto_address_identity.core.config import Settings
from crypto_address_identity.coverage import (
    CoverageEntitySeedInput,
    CoverageEntitySeedService,
)
from crypto_address_identity.evidence import EvidenceService
from crypto_address_identity.providers.zero_x_router import (
    ProviderPayloadError,
    ProviderTokenMissing,
    ZeroXRouterClient,
    parse_bitcoin_response,
)
from crypto_address_identity.storage.raw_payloads import RawPayloadStore
from crypto_address_identity.storage.sqlite import IdentityDatabase


AddressCohort = Literal["urgent", "p0", "p1"]

_ADDRESS_ESTIMATED_BYTES = 65_536
_POINT_UNIT_BYTES = 100_000
_MAX_REQUESTS_PER_RUN = 500
_MAX_CONSECUTIVE_FAILURES = 3
_COHORT_ORDER = {"urgent": 0, "p0": 1, "p1": 2}
_QUEUE_COLUMNS = (
    "queue_rank",
    "address_id",
    "normalized_address",
    "candidate_tier",
    "cohort",
    "v2_chain_score",
    "current_utxo_sats",
    "lifetime_received_sats",
    "active_conflict",
    "explicit_direct_requirement",
    "candidate_row_sha256",
)


class AddressEnrichmentArtifactError(RuntimeError):
    """Raised when a queue input or output fails a checksum contract."""


@dataclass(frozen=True)
class AddressQueueBuildResult:
    status: str
    queue_id: str
    queue_rows: int
    cohort_counts: dict[str, int]
    source_coverage_snapshot_id: str
    parquet_path: Path
    parquet_sha256: str
    manifest_path: Path
    manifest_sha256: str


@dataclass(frozen=True)
class AddressEnrichmentResult:
    status: str
    dry_run: bool
    campaign_id: str
    queue_manifest_sha256: str
    cohort: str
    eligible_addresses: int
    campaign_attempted_addresses: int
    skipped_terminal_addresses: int
    skipped_newly_covered: int
    planned_addresses: int
    requests: int
    successful_addresses: int
    parsed_addresses: int
    unattributed_addresses: int
    malformed_addresses: int
    failed_addresses: int
    evidence_inserted: int
    duplicate_evidence: int
    new_entities: int
    entity_seeds_inserted: int
    response_bytes: int
    estimated_points: int
    campaign_points_before: int
    campaign_points_after: int
    outcome_counts: dict[str, int]
    raw_payloads_written: int
    run_id: str | None


class BtcV2SAddressQueueBuilder:
    """Build a deterministic P0/P1 queue from two checksum-pinned artifacts."""

    def build(
        self,
        *,
        candidate_campaign_root: Path,
        coverage_snapshot_root: Path,
        output_root: Path,
        built_at: datetime | None = None,
    ) -> AddressQueueBuildResult:
        candidate_root = Path(candidate_campaign_root)
        coverage_root = Path(coverage_snapshot_root)
        output_root = Path(output_root)
        built_at = _as_utc(built_at or datetime.now(UTC))

        candidate_manifest_path = candidate_root / "manifest.json"
        candidate_manifest = _verified_manifest(candidate_manifest_path)
        coverage_manifest_path = coverage_root / "manifest.json"
        coverage_manifest = _verified_manifest(coverage_manifest_path)
        candidate_manifest_file_sha = _file_sha256(candidate_manifest_path)
        if (
            coverage_manifest.get("source_manifest_file_sha256")
            != candidate_manifest_file_sha
        ):
            raise AddressEnrichmentArtifactError(
                "coverage snapshot is not bound to the candidate manifest"
            )
        if (
            coverage_manifest.get("source_campaign_id")
            != candidate_manifest.get("campaign_id")
        ):
            raise AddressEnrichmentArtifactError(
                "coverage and candidate campaign IDs differ"
            )

        coverage_path = _verified_single_parquet(
            coverage_root, coverage_manifest
        )
        selected = self._selected_coverage_rows(coverage_path)
        candidates = self._candidate_rows(
            candidate_root=candidate_root,
            manifest=candidate_manifest,
            selected=selected,
        )
        if len(candidates) != len(selected):
            raise AddressEnrichmentArtifactError(
                "candidate and coverage queue rows do not reconcile"
            )
        ordered = sorted(
            candidates,
            key=lambda row: (
                _COHORT_ORDER[row["cohort"]],
                -row["v2_chain_score"],
                -row["current_utxo_sats"],
                -row["lifetime_received_sats"],
                row["normalized_address"],
            ),
        )
        for rank, row in enumerate(ordered, start=1):
            row["queue_rank"] = rank

        coverage_manifest_file_sha = _file_sha256(coverage_manifest_path)
        queue_id = (
            built_at.strftime("%Y%m%dT%H%M%SZ")
            + "-"
            + coverage_manifest_file_sha[:12]
        )
        final_root = output_root / queue_id
        if final_root.exists():
            raise AddressEnrichmentArtifactError(
                "address queue snapshot already exists"
            )
        staging_root = (
            output_root / f".staging-{queue_id}-{uuid.uuid4().hex}"
        )
        staging_root.mkdir(parents=True, mode=0o700)
        parquet_path = staging_root / "btc_v2s_address_queue.parquet"
        try:
            table = pa.table(
                {
                    "queue_rank": pa.array(
                        [row["queue_rank"] for row in ordered],
                        type=pa.int64(),
                    ),
                    "address_id": pa.array(
                        [row["address_id"] for row in ordered],
                        type=pa.string(),
                    ),
                    "normalized_address": pa.array(
                        [row["normalized_address"] for row in ordered],
                        type=pa.string(),
                    ),
                    "candidate_tier": pa.array(
                        [row["candidate_tier"] for row in ordered],
                        type=pa.string(),
                    ),
                    "cohort": pa.array(
                        [row["cohort"] for row in ordered],
                        type=pa.string(),
                    ),
                    "v2_chain_score": pa.array(
                        [row["v2_chain_score"] for row in ordered],
                        type=pa.int64(),
                    ),
                    "current_utxo_sats": pa.array(
                        [row["current_utxo_sats"] for row in ordered],
                        type=pa.decimal128(38, 0),
                    ),
                    "lifetime_received_sats": pa.array(
                        [row["lifetime_received_sats"] for row in ordered],
                        type=pa.decimal128(38, 0),
                    ),
                    "active_conflict": pa.array(
                        [row["active_conflict"] for row in ordered],
                        type=pa.bool_(),
                    ),
                    "explicit_direct_requirement": pa.array(
                        [
                            row["explicit_direct_requirement"]
                            for row in ordered
                        ],
                        type=pa.bool_(),
                    ),
                    "candidate_row_sha256": pa.array(
                        [row["candidate_row_sha256"] for row in ordered],
                        type=pa.string(),
                    ),
                }
            )
            pq.write_table(
                table,
                parquet_path,
                compression="zstd",
                version="2.6",
                write_statistics=True,
            )
            parquet_sha = _file_sha256(parquet_path)
            counts = Counter(row["cohort"] for row in ordered)
            manifest: dict[str, Any] = {
                "schema_version": "btc_v2s_address_enrichment_queue_v1",
                "queue_id": queue_id,
                "built_at": _utc_string(built_at),
                "source_candidate_campaign_id": candidate_manifest.get(
                    "campaign_id"
                ),
                "source_candidate_manifest_file_sha256": (
                    candidate_manifest_file_sha
                ),
                "source_candidate_manifest_sha256": candidate_manifest.get(
                    "manifest_sha256"
                ),
                "source_coverage_snapshot_id": coverage_manifest.get(
                    "snapshot_id"
                ),
                "source_coverage_manifest_file_sha256": (
                    coverage_manifest_file_sha
                ),
                "source_coverage_manifest_sha256": coverage_manifest.get(
                    "manifest_sha256"
                ),
                "queue_rows": len(ordered),
                "cohort_counts": {
                    cohort: counts.get(cohort, 0)
                    for cohort in _COHORT_ORDER
                },
                "ordering": [
                    "cohort",
                    "v2_chain_score_desc",
                    "current_utxo_sats_desc",
                    "lifetime_received_sats_desc",
                    "normalized_address",
                ],
                "files": [
                    {
                        "path": parquet_path.name,
                        "row_count": len(ordered),
                        "size": parquet_path.stat().st_size,
                        "sha256": parquet_sha,
                    }
                ],
            }
            manifest_sha = _json_sha256(manifest)
            manifest["manifest_sha256"] = manifest_sha
            manifest_path = staging_root / "manifest.json"
            manifest_path.write_text(
                json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            parquet_path.chmod(0o600)
            manifest_path.chmod(0o600)
            output_root.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.replace(staging_root, final_root)
        except Exception:
            shutil.rmtree(staging_root, ignore_errors=True)
            raise

        return AddressQueueBuildResult(
            status="published",
            queue_id=queue_id,
            queue_rows=len(ordered),
            cohort_counts={
                cohort: counts.get(cohort, 0)
                for cohort in _COHORT_ORDER
            },
            source_coverage_snapshot_id=str(
                coverage_manifest.get("snapshot_id")
            ),
            parquet_path=final_root / parquet_path.name,
            parquet_sha256=parquet_sha,
            manifest_path=final_root / "manifest.json",
            manifest_sha256=manifest_sha,
        )

    @staticmethod
    def _selected_coverage_rows(path: Path) -> dict[str, dict[str, Any]]:
        selected: dict[str, dict[str, Any]] = {}
        parquet = pq.ParquetFile(path)
        columns = (
            "normalized_address",
            "candidate_tier",
            "candidate_row_sha256",
            "coverage_state",
            "active_conflict",
            "explicit_direct_requirement",
        )
        for batch in parquet.iter_batches(batch_size=65_536, columns=columns):
            for row in batch.to_pylist():
                if (
                    row["coverage_state"] != "needs_direct_enrichment"
                    or row["candidate_tier"] not in {"p0", "p1"}
                ):
                    continue
                subject = normalize_bitcoin_address(
                    row["normalized_address"]
                )
                if subject.normalized_address in selected:
                    raise AddressEnrichmentArtifactError(
                        "coverage snapshot has duplicate selected addresses"
                    )
                row_hash = row["candidate_row_sha256"]
                if not _is_sha256(row_hash):
                    raise AddressEnrichmentArtifactError(
                        "coverage candidate row hash is invalid"
                    )
                selected[subject.normalized_address] = {
                    "address_id": subject.address_id,
                    "candidate_tier": row["candidate_tier"],
                    "candidate_row_sha256": row_hash,
                    "active_conflict": bool(row["active_conflict"]),
                    "explicit_direct_requirement": bool(
                        row["explicit_direct_requirement"]
                    ),
                }
        return selected

    @staticmethod
    def _candidate_rows(
        *,
        candidate_root: Path,
        manifest: dict[str, Any],
        selected: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        seen: set[str] = set()
        for path in _verified_candidate_parquets(
            candidate_root, manifest, tiers={"p0", "p1"}
        ):
            columns = (
                "normalized_address",
                "candidate_tier",
                "v2_chain_score",
                "current_utxo_sats",
                "lifetime_received_sats",
                "candidate_row_sha256",
            )
            parquet = pq.ParquetFile(path)
            for batch in parquet.iter_batches(
                batch_size=65_536, columns=columns
            ):
                for row in batch.to_pylist():
                    address = row["normalized_address"]
                    coverage = selected.get(address)
                    if coverage is None:
                        continue
                    if address in seen:
                        raise AddressEnrichmentArtifactError(
                            "candidate artifact has duplicate selected address"
                        )
                    if (
                        row["candidate_tier"]
                        != coverage["candidate_tier"]
                        or row["candidate_row_sha256"]
                        != coverage["candidate_row_sha256"]
                    ):
                        raise AddressEnrichmentArtifactError(
                            "candidate and coverage row metadata differ"
                        )
                    seen.add(address)
                    urgent = (
                        coverage["active_conflict"]
                        or coverage["explicit_direct_requirement"]
                    )
                    result.append(
                        {
                            "address_id": coverage["address_id"],
                            "normalized_address": address,
                            "candidate_tier": row["candidate_tier"],
                            "cohort": (
                                "urgent"
                                if urgent
                                else row["candidate_tier"]
                            ),
                            "v2_chain_score": int(row["v2_chain_score"]),
                            "current_utxo_sats": int(
                                row["current_utxo_sats"]
                            ),
                            "lifetime_received_sats": int(
                                row["lifetime_received_sats"]
                            ),
                            "active_conflict": coverage[
                                "active_conflict"
                            ],
                            "explicit_direct_requirement": coverage[
                                "explicit_direct_requirement"
                            ],
                            "candidate_row_sha256": row[
                                "candidate_row_sha256"
                            ],
                        }
                    )
        return result


class BtcV2SAddressEnrichmentService:
    """Execute each checksum-pinned address at most once per campaign."""

    def __init__(
        self,
        *,
        database: IdentityDatabase,
        settings: Settings,
        provider: ZeroXRouterClient,
        raw_payloads: RawPayloadStore,
        evidence: EvidenceService,
        sleeper: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.database = database
        self.settings = settings
        self.provider = provider
        self.raw_payloads = raw_payloads
        self.evidence = evidence
        self.sleeper = sleeper
        self.monotonic = monotonic
        self.clock = clock or (lambda: datetime.now(UTC))
        self.quota = QuotaManager(database)

    def run(
        self,
        *,
        queue_root: Path,
        campaign_id: str,
        cohort: AddressCohort,
        request_limit: int,
        campaign_point_limit: int,
        dry_run: bool,
        now: datetime | None = None,
    ) -> AddressEnrichmentResult:
        campaign_id = _normalize_campaign_id(campaign_id)
        if cohort not in _COHORT_ORDER:
            raise ValueError("unsupported address enrichment cohort")
        if request_limit < 1 or request_limit > _MAX_REQUESTS_PER_RUN:
            raise ValueError("request_limit must be between 1 and 500")
        if campaign_point_limit < 1:
            raise ValueError("campaign_point_limit must be positive")
        queue_manifest, rows = _read_queue(Path(queue_root))
        queue_manifest_sha = str(queue_manifest["manifest_sha256"])
        self._validate_campaign_binding(
            campaign_id=campaign_id,
            queue_manifest_sha256=queue_manifest_sha,
            cohort=cohort,
            point_limit=campaign_point_limit,
        )

        eligible = [row for row in rows if row["cohort"] == cohort]
        attempted = self._attempted_address_ids(campaign_id)
        terminal, memberships, local_evidence = self._current_coverage()
        due: list[dict[str, Any]] = []
        skipped_terminal = 0
        skipped_newly_covered = 0
        for row in eligible:
            address_id = row["address_id"]
            if address_id in attempted:
                continue
            if address_id in terminal:
                skipped_terminal += 1
                continue
            requires_direct = bool(
                row["active_conflict"]
                or row["explicit_direct_requirement"]
            )
            if not requires_direct and (
                address_id in memberships or address_id in local_evidence
            ):
                skipped_newly_covered += 1
                continue
            due.append(row)

        points_before = self._campaign_points(campaign_id)
        remaining_points = max(0, campaign_point_limit - points_before)
        planned = due[: min(request_limit, remaining_points)]
        common = {
            "campaign_id": campaign_id,
            "queue_manifest_sha256": queue_manifest_sha,
            "cohort": cohort,
            "eligible_addresses": len(eligible),
            "campaign_attempted_addresses": len(
                {row["address_id"] for row in eligible} & attempted
            ),
            "skipped_terminal_addresses": skipped_terminal,
            "skipped_newly_covered": skipped_newly_covered,
            "planned_addresses": len(planned),
            "campaign_points_before": points_before,
        }
        if dry_run:
            return AddressEnrichmentResult(
                status="dry_run",
                dry_run=True,
                requests=0,
                successful_addresses=0,
                parsed_addresses=0,
                unattributed_addresses=0,
                malformed_addresses=0,
                failed_addresses=0,
                evidence_inserted=0,
                duplicate_evidence=0,
                new_entities=0,
                entity_seeds_inserted=0,
                response_bytes=0,
                estimated_points=0,
                campaign_points_after=points_before,
                outcome_counts={},
                raw_payloads_written=0,
                run_id=None,
                **common,
            )
        if self.settings.provider_token_value() is None:
            raise ProviderTokenMissing()
        if not planned:
            return AddressEnrichmentResult(
                status="noop",
                dry_run=False,
                requests=0,
                successful_addresses=0,
                parsed_addresses=0,
                unattributed_addresses=0,
                malformed_addresses=0,
                failed_addresses=0,
                evidence_inserted=0,
                duplicate_evidence=0,
                new_entities=0,
                entity_seeds_inserted=0,
                response_bytes=0,
                estimated_points=0,
                campaign_points_after=points_before,
                outcome_counts={},
                raw_payloads_written=0,
                run_id=None,
                **common,
            )

        started_at = _as_utc(now or self.clock())
        self._bind_campaign(
            campaign_id=campaign_id,
            queue_manifest_sha256=queue_manifest_sha,
            cohort=cohort,
            point_limit=campaign_point_limit,
            created_at=started_at,
        )
        run_id = self.quota.create_run(
            mode="execute",
            request_limit=self.settings.coverage_requests_per_minute,
            response_bytes_budget=self.settings.coverage_response_bytes_budget,
            started_at=started_at,
        )
        interval = 60.0 / self.settings.coverage_requests_per_minute
        outcomes: Counter[str] = Counter()
        response_bytes = 0
        points = 0
        requests = 0
        successful = 0
        parsed_count = 0
        unattributed = 0
        malformed = 0
        failed = 0
        evidence_inserted = 0
        duplicate_evidence = 0
        raw_written = 0
        consecutive_failures = 0
        blocked = False
        discovered_entities: set[str] = set()
        existing_entities = self._known_entity_ids()
        previous_request_started: float | None = None

        for index, row in enumerate(planned):
            if previous_request_started is not None:
                elapsed = self.monotonic() - previous_request_started
                remaining = interval - elapsed
                if remaining > 0:
                    self.sleeper(remaining)
            previous_request_started = self.monotonic()
            observed_at = (
                started_at + timedelta(seconds=interval * index)
                if now is not None
                else _as_utc(self.clock())
            )
            try:
                reservation = self.quota.reserve(
                    run_id=run_id,
                    now=observed_at,
                    estimated_response_bytes=_ADDRESS_ESTIMATED_BYTES,
                )
            except RateLimitExceeded:
                outcomes["rate_limited"] += 1
                blocked = True
                break
            except ByteBudgetExceeded:
                outcomes["budget_exhausted"] += 1
                blocked = True
                break
            if not self._reserve_address_attempt(
                campaign_id=campaign_id,
                queue_manifest_sha256=queue_manifest_sha,
                cohort=cohort,
                row=row,
                reserved_at=observed_at,
            ):
                self.quota.complete(
                    reservation.reservation_id,
                    actual_response_bytes=0,
                    outcome="failed",
                )
                outcomes["duplicate_attempt"] += 1
                continue

            response = self.provider.fetch_request_once(
                self.provider.build_btc_coverage_enrichment_request(
                    row["normalized_address"]
                )
            )
            requests += 1
            size = len(response.body)
            response_bytes += size
            response_points = _request_points(size)
            points += response_points
            stored = (
                self.raw_payloads.persist(response.body)
                if response.body
                else None
            )
            raw_written += int(stored is not None)
            observation_id = str(uuid.uuid4())
            self._record_observation(
                observation_id=observation_id,
                run_id=run_id,
                campaign_id=campaign_id,
                observed_at=observed_at,
                http_status=response.http_status,
                outcome=response.outcome,
                response_bytes=size,
                payload_sha256=(
                    stored.payload_sha256 if stored is not None else None
                ),
                address_id=row["address_id"],
            )
            self.quota.complete(
                reservation.reservation_id,
                actual_response_bytes=size,
                outcome=(
                    "completed"
                    if response.outcome == "success"
                    else "rate_limited"
                    if response.outcome == "rate_limited"
                    else "failed"
                ),
            )
            outcomes[response.outcome] += 1
            if response.outcome != "success":
                failed += 1
                consecutive_failures += 1
                if (
                    response.outcome == "rate_limited"
                    or response.http_status in {401, 402, 403}
                    or consecutive_failures >= _MAX_CONSECUTIVE_FAILURES
                ):
                    blocked = True
                    break
                continue
            successful += 1
            consecutive_failures = 0
            try:
                parsed = parse_bitcoin_response(
                    response.body,
                    normalize_bitcoin_address(row["normalized_address"]),
                )
            except ProviderPayloadError:
                malformed += 1
                outcomes["malformed_payload"] += 1
                self._record_parse_result(
                    observation_id=observation_id,
                    address_id=row["address_id"],
                    parse_outcome="malformed_payload",
                    parsed_at=observed_at,
                )
            else:
                parsed_count += 1
                self._record_parse_result(
                    observation_id=observation_id,
                    address_id=row["address_id"],
                    parse_outcome="parsed_success",
                    parsed_at=observed_at,
                )
                evidence_result = self.evidence.append_provider_candidates(
                    address=row["normalized_address"],
                    observation_id=observation_id,
                    candidates=parsed.evidence_candidates,
                    source_url=self.settings.provider_base_url,
                    artifact_sha256=(
                        stored.payload_sha256
                        if stored is not None
                        else None
                    ),
                    observed_at=observed_at,
                )
                evidence_inserted += evidence_result.inserted_count
                duplicate_evidence += evidence_result.duplicate_count
                if not parsed.evidence_candidates:
                    unattributed += 1
                discovered_entities.update(
                    candidate.provider_entity_id
                    for candidate in parsed.evidence_candidates
                    if candidate.provider_entity_id
                    and candidate.provider_entity_id not in existing_entities
                )
            if points_before + points >= campaign_point_limit:
                blocked = len(planned) > requests
                if blocked:
                    outcomes["campaign_point_limit"] += 1
                break

        new_entities = tuple(sorted(discovered_entities))
        seed_result = CoverageEntitySeedService(self.database).import_seeds(
            [
                CoverageEntitySeedInput(
                    provider_entity_id=entity_id,
                    priority=(
                        100 if cohort == "urgent" else 90 if cohort == "p0" else 80
                    ),
                    source_reference=(
                        "btc-v2s-address-enrichment:"
                        + campaign_id
                        + ":"
                        + queue_manifest_sha
                    ),
                    requested_at=started_at,
                )
                for entity_id in new_entities
            ],
            created_at=started_at,
        )
        status = "blocked" if blocked else "completed"
        if not blocked and (failed or malformed):
            status = "partial"
        self._complete_run(
            run_id=run_id,
            status=status,
            campaign_id=campaign_id,
            queue_manifest_sha256=queue_manifest_sha,
            cohort=cohort,
            points=points,
            outcomes=outcomes,
        )
        return AddressEnrichmentResult(
            status=status,
            dry_run=False,
            requests=requests,
            successful_addresses=successful,
            parsed_addresses=parsed_count,
            unattributed_addresses=unattributed,
            malformed_addresses=malformed,
            failed_addresses=failed,
            evidence_inserted=evidence_inserted,
            duplicate_evidence=duplicate_evidence,
            new_entities=len(new_entities),
            entity_seeds_inserted=seed_result.inserted_count,
            response_bytes=response_bytes,
            estimated_points=points,
            campaign_points_after=points_before + points,
            outcome_counts=dict(outcomes),
            raw_payloads_written=raw_written,
            run_id=run_id,
            **common,
        )

    def _validate_campaign_binding(
        self,
        *,
        campaign_id: str,
        queue_manifest_sha256: str,
        cohort: str,
        point_limit: int,
    ) -> None:
        with self.database.read_connection() as connection:
            row = connection.execute(
                """
                SELECT queue_manifest_sha256, cohort, point_limit
                FROM coverage_address_campaign
                WHERE campaign_id = ?
                """,
                (campaign_id,),
            ).fetchone()
        if row is not None and (
            row["queue_manifest_sha256"] != queue_manifest_sha256
            or row["cohort"] != cohort
            or row["point_limit"] != point_limit
        ):
            raise ValueError(
                "address campaign is bound to different immutable inputs"
            )

    def _bind_campaign(
        self,
        *,
        campaign_id: str,
        queue_manifest_sha256: str,
        cohort: str,
        point_limit: int,
        created_at: datetime,
    ) -> None:
        with self.database.write_transaction() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO coverage_address_campaign (
                    campaign_id, queue_manifest_sha256, cohort,
                    point_limit, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    campaign_id,
                    queue_manifest_sha256,
                    cohort,
                    point_limit,
                    _utc_string(created_at),
                ),
            )
        self._validate_campaign_binding(
            campaign_id=campaign_id,
            queue_manifest_sha256=queue_manifest_sha256,
            cohort=cohort,
            point_limit=point_limit,
        )

    def _reserve_address_attempt(
        self,
        *,
        campaign_id: str,
        queue_manifest_sha256: str,
        cohort: str,
        row: dict[str, Any],
        reserved_at: datetime,
    ) -> bool:
        subject = normalize_bitcoin_address(row["normalized_address"])
        fingerprint = _json_sha256(
            {
                "campaign_id": campaign_id,
                "queue_manifest_sha256": queue_manifest_sha256,
                "address_id": subject.address_id,
            }
        )
        timestamp = _utc_string(reserved_at)
        with self.database.write_transaction() as connection:
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
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO coverage_address_campaign_attempt (
                    address_attempt_id, address_attempt_fingerprint,
                    campaign_id, queue_manifest_sha256, address_id,
                    cohort, reserved_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    fingerprint,
                    campaign_id,
                    queue_manifest_sha256,
                    subject.address_id,
                    cohort,
                    timestamp,
                ),
            )
        return cursor.rowcount == 1

    def _attempted_address_ids(self, campaign_id: str) -> set[str]:
        with self.database.read_connection() as connection:
            rows = connection.execute(
                """
                SELECT address_id FROM coverage_address_campaign_attempt
                WHERE campaign_id = ?
                """,
                (campaign_id,),
            ).fetchall()
        return {row["address_id"] for row in rows}

    def _current_coverage(self) -> tuple[set[str], set[str], set[str]]:
        with self.database.read_connection() as connection:
            terminal = {
                row["address_id"]
                for row in connection.execute(
                    """
                    SELECT DISTINCT address_id
                    FROM coverage_address_parse_result
                    WHERE parse_outcome = 'parsed_success'
                    """
                ).fetchall()
            }
            memberships = {
                row["address_id"]
                for row in connection.execute(
                    """
                    SELECT DISTINCT address_id
                    FROM coverage_entity_prediction
                    """
                ).fetchall()
            }
            local_evidence = {
                row["address_id"]
                for row in connection.execute(
                    """
                    SELECT DISTINCT address_id FROM identity_evidence
                    WHERE assertion_type = 'entity_control'
                      AND evidence_status = 'valid'
                    """
                ).fetchall()
            }
        return terminal, memberships, local_evidence

    def _known_entity_ids(self) -> set[str]:
        with self.database.read_connection() as connection:
            rows = connection.execute(
                """
                SELECT provider_entity_id FROM coverage_entity_seed
                UNION SELECT provider_entity_id
                      FROM coverage_entity_observation
                UNION SELECT provider_entity_id
                      FROM coverage_entity_prediction
                """
            ).fetchall()
        return {row["provider_entity_id"] for row in rows}

    def _campaign_points(self, campaign_id: str) -> int:
        profile = _campaign_query_profile(campaign_id)
        with self.database.read_connection() as connection:
            rows = connection.execute(
                """
                SELECT response_bytes FROM source_observation
                WHERE query_profile = ?
                """,
                (profile,),
            ).fetchall()
        return sum(_request_points(row["response_bytes"]) for row in rows)

    def _record_observation(
        self,
        *,
        observation_id: str,
        run_id: str,
        campaign_id: str,
        observed_at: datetime,
        http_status: int | None,
        outcome: str,
        response_bytes: int,
        payload_sha256: str | None,
        address_id: str,
    ) -> None:
        timestamp = _utc_string(observed_at)
        with self.database.write_transaction() as connection:
            connection.execute(
                """
                INSERT INTO source_observation (
                    observation_id, source_id, source_version, source_kind,
                    endpoint_template, query_profile, requested_at,
                    completed_at, http_status, outcome, response_bytes,
                    payload_sha256, chain_key, address_id, ingestion_run_id
                ) VALUES (
                    ?, '0xrouter',
                    'chaindata_v2s_address_enrichment_v1', 'provider',
                    '/chaindata/intelligence/address_enriched/{address}/all',
                    ?, ?, ?, ?, ?, ?, ?, 'bitcoin', ?, ?
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
                    address_id,
                    run_id,
                ),
            )

    def _record_parse_result(
        self,
        *,
        observation_id: str,
        address_id: str,
        parse_outcome: str,
        parsed_at: datetime,
    ) -> None:
        fingerprint = _json_sha256(
            {
                "observation_id": observation_id,
                "address_id": address_id,
                "parse_outcome": parse_outcome,
            }
        )
        with self.database.write_transaction() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO coverage_address_parse_result (
                    parse_result_id, parse_result_fingerprint,
                    observation_id, address_id, parse_outcome, parsed_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    fingerprint,
                    observation_id,
                    address_id,
                    parse_outcome,
                    _utc_string(parsed_at),
                ),
            )

    def _complete_run(
        self,
        *,
        run_id: str,
        status: str,
        campaign_id: str,
        queue_manifest_sha256: str,
        cohort: str,
        points: int,
        outcomes: Counter[str],
    ) -> None:
        summary = {
            "coverage_mode": "btc_v2s_address_enrichment",
            "campaign_id": campaign_id,
            "queue_manifest_sha256": queue_manifest_sha256,
            "cohort": cohort,
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


def _read_queue(queue_root: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    manifest = _verified_manifest(queue_root / "manifest.json")
    if manifest.get("schema_version") != "btc_v2s_address_enrichment_queue_v1":
        raise AddressEnrichmentArtifactError(
            "unsupported address queue schema"
        )
    path = _verified_single_parquet(queue_root, manifest)
    table = pq.read_table(path, columns=list(_QUEUE_COLUMNS))
    rows = table.to_pylist()
    expected_rows = manifest.get("queue_rows")
    if expected_rows != len(rows):
        raise AddressEnrichmentArtifactError(
            "address queue row count mismatch"
        )
    seen: set[str] = set()
    for expected_rank, row in enumerate(rows, start=1):
        if row["queue_rank"] != expected_rank:
            raise AddressEnrichmentArtifactError(
                "address queue rank is not contiguous"
            )
        subject = normalize_bitcoin_address(row["normalized_address"])
        if subject.address_id != row["address_id"]:
            raise AddressEnrichmentArtifactError(
                "address queue subject ID mismatch"
            )
        if subject.address_id in seen:
            raise AddressEnrichmentArtifactError(
                "address queue contains duplicate subjects"
            )
        if row["cohort"] not in _COHORT_ORDER:
            raise AddressEnrichmentArtifactError(
                "address queue contains invalid cohort"
            )
        seen.add(subject.address_id)
    return manifest, rows


def _verified_manifest(path: Path) -> dict[str, Any]:
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AddressEnrichmentArtifactError(
            "manifest is unreadable"
        ) from exc
    if not isinstance(manifest, dict):
        raise AddressEnrichmentArtifactError("manifest is not an object")
    declared = manifest.get("manifest_sha256")
    if not _is_sha256(declared):
        raise AddressEnrichmentArtifactError(
            "manifest semantic checksum is invalid"
        )
    semantic = dict(manifest)
    semantic.pop("manifest_sha256", None)
    if _json_sha256(semantic) != declared:
        raise AddressEnrichmentArtifactError(
            "manifest semantic checksum mismatch"
        )
    return manifest


def _verified_single_parquet(root: Path, manifest: dict[str, Any]) -> Path:
    records = manifest.get("files")
    if not isinstance(records, list):
        raise AddressEnrichmentArtifactError("manifest files are invalid")
    parquet_records = [
        record
        for record in records
        if isinstance(record, dict)
        and isinstance(record.get("path"), str)
        and record["path"].endswith(".parquet")
    ]
    if len(parquet_records) != 1:
        raise AddressEnrichmentArtifactError(
            "manifest must contain exactly one parquet"
        )
    return _verified_file(root, parquet_records[0])


def _verified_candidate_parquets(
    root: Path, manifest: dict[str, Any], *, tiers: set[str]
) -> tuple[Path, ...]:
    records = manifest.get("files")
    if not isinstance(records, list):
        raise AddressEnrichmentArtifactError(
            "candidate manifest files are invalid"
        )
    paths: list[Path] = []
    for record in records:
        if not isinstance(record, dict):
            raise AddressEnrichmentArtifactError(
                "candidate file record is invalid"
            )
        relative = record.get("path")
        if not isinstance(relative, str) or not relative.endswith(".parquet"):
            continue
        declared_tier = next(
            (
                tier
                for tier in tiers
                if f"/tier={tier}/" in "/" + relative
            ),
            None,
        )
        if declared_tier is None:
            continue
        paths.append(_verified_file(root, record))
    if not paths:
        raise AddressEnrichmentArtifactError(
            "candidate manifest has no P0/P1 parquet files"
        )
    return tuple(paths)


def _verified_file(root: Path, record: dict[str, Any]) -> Path:
    relative = record.get("path")
    expected_size = record.get("size")
    expected_sha = record.get("sha256")
    expected_rows = record.get("row_count")
    if (
        not isinstance(relative, str)
        or not isinstance(expected_size, int)
        or expected_size < 0
        or not _is_sha256(expected_sha)
        or not isinstance(expected_rows, int)
        or expected_rows < 0
    ):
        raise AddressEnrichmentArtifactError(
            "manifest file metadata is invalid"
        )
    path = _safe_child(root, relative)
    if not path.is_file() or path.stat().st_size != expected_size:
        raise AddressEnrichmentArtifactError(
            "manifest file is missing or has wrong size"
        )
    if _file_sha256(path) != expected_sha:
        raise AddressEnrichmentArtifactError(
            "manifest file checksum mismatch"
        )
    if pq.ParquetFile(path).metadata.num_rows != expected_rows:
        raise AddressEnrichmentArtifactError(
            "manifest parquet row count mismatch"
        )
    return path


def _safe_child(root: Path, relative_path: str) -> Path:
    relative = Path(relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise AddressEnrichmentArtifactError("artifact path is unsafe")
    resolved_root = root.resolve()
    candidate = (resolved_root / relative).resolve()
    if not candidate.is_relative_to(resolved_root):
        raise AddressEnrichmentArtifactError(
            "artifact path escapes its root"
        )
    return candidate


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
    return f"btc_v2s_address_enrichment:{campaign_id}"


def _request_points(response_bytes: int) -> int:
    return max(1, math.ceil(response_bytes / _POINT_UNIT_BYTES))


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _utc_string(value: datetime) -> str:
    return _as_utc(value).isoformat().replace("+00:00", "Z")


def _json_sha256(value: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )
