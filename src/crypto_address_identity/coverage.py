"""Coverage-driven BTC identity synchronization over 0xRouter Chaindata.

The gateway does not expose a full Arkham update feed.  This module therefore
builds a bounded, business-relevant address universe from entity ranking,
entity-prediction fanout, and the existing address candidate queue.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Iterable

from pydantic import BaseModel, Field, field_validator

from crypto_address_identity.candidates import (
    ByteBudgetExceeded,
    QuotaManager,
    RateLimitExceeded,
)
from crypto_address_identity.chains.bitcoin import BitcoinAddressError, normalize_bitcoin_address
from crypto_address_identity.core.config import Settings
from crypto_address_identity.evidence import EvidenceService
from crypto_address_identity.providers.zero_x_router import (
    ProviderPayloadError,
    ProviderTokenMissing,
    ZeroXRouterClient,
    parse_bitcoin_response,
)
from crypto_address_identity.storage.raw_payloads import RawPayloadStore
from crypto_address_identity.storage.sqlite import IdentityDatabase


_ESTIMATED_DISCOVERY_BYTES = 102_400
_ESTIMATED_ENTITY_BYTES = 102_400
_ESTIMATED_PREDICTION_BYTES = 512_000
_ESTIMATED_ADDRESS_BYTES = 65_536
_POINT_UNIT_BYTES = 100_000
_DISCOVERY_REQUEST_COUNT = 2


class CoveragePayloadError(ValueError):
    """Raised when a Chaindata result has no safely usable BTC coverage records."""


class CoverageEntitySeedInput(BaseModel):
    """Append-only seed for a provider entity discovered outside the sync."""

    provider_entity_id: str = Field(min_length=1, max_length=256)
    priority: int = Field(default=80, ge=0, le=100)
    source_reference: str = Field(min_length=1, max_length=512)
    requested_at: datetime

    @field_validator("provider_entity_id")
    @classmethod
    def normalize_entity_id(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or any(char.isspace() for char in normalized):
            raise ValueError("provider_entity_id must be a compact provider identifier")
        return normalized

    @field_validator("requested_at")
    @classmethod
    def normalize_requested_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("requested_at must be timezone-aware")
        return value.astimezone(UTC)


@dataclass(frozen=True)
class EntitySeedImportResult:
    inserted_count: int
    duplicate_count: int


@dataclass(frozen=True)
class CoverageSyncResult:
    status: str
    dry_run: bool
    run_id: str | None
    entity_discovery_requests: int
    entity_count: int
    entity_detail_requests: int
    prediction_requests: int
    prediction_address_count: int
    address_enrichment_requests: int
    address_evidence_count: int
    duplicate_count: int
    response_bytes: int
    estimated_points: int
    outcome_counts: dict[str, int]
    written_paths: tuple[str, ...]


@dataclass(frozen=True)
class CoverageAddressTarget:
    """One deduplicated BTC address selected for direct label enrichment."""

    address_id: str
    normalized_address: str
    priority: int
    source_kind: str


class CoverageEntitySeedService:
    """Immutable local source of explicit business-relevant entity identifiers."""

    def __init__(self, database: IdentityDatabase) -> None:
        self.database = database

    def import_seeds(
        self, seeds: list[CoverageEntitySeedInput], *, created_at: datetime | None = None
    ) -> EntitySeedImportResult:
        timestamp = _utc_string(created_at or datetime.now(UTC))
        inserted = 0
        duplicates = 0
        with self.database.write_transaction() as connection:
            for seed in seeds:
                fingerprint = _sha256(
                    {
                        "provider_entity_id": seed.provider_entity_id,
                        "priority": seed.priority,
                        "source_reference": seed.source_reference,
                        "requested_at": _utc_string(seed.requested_at),
                    }
                )
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO coverage_entity_seed (
                        seed_id, seed_fingerprint, provider_entity_id, priority,
                        source_reference, requested_at, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        fingerprint,
                        seed.provider_entity_id,
                        seed.priority,
                        seed.source_reference,
                        _utc_string(seed.requested_at),
                        timestamp,
                    ),
                )
                if cursor.rowcount == 1:
                    inserted += 1
                else:
                    duplicates += 1
        return EntitySeedImportResult(inserted, duplicates)


class CoverageSyncService:
    """Batched entity discovery plus bounded, deduplicated address enrichment."""

    def __init__(
        self,
        *,
        database: IdentityDatabase,
        settings: Settings,
        provider: ZeroXRouterClient,
        raw_payloads: RawPayloadStore,
        evidence: EvidenceService,
    ) -> None:
        self.database = database
        self.settings = settings
        self.provider = provider
        self.raw_payloads = raw_payloads
        self.evidence = evidence
        self.seeds = CoverageEntitySeedService(database)
        self.quota = QuotaManager(database)

    def run(
        self,
        *,
        dry_run: bool,
        entity_types: tuple[str, ...] = ("exchange", "fund"),
        entity_limit: int | None = None,
        address_limit: int | None = None,
        now: datetime | None = None,
    ) -> CoverageSyncResult:
        observed_at = (now or datetime.now(UTC)).astimezone(UTC)
        entity_limit = entity_limit or self.settings.coverage_max_entities_per_run
        address_limit = address_limit or self.settings.coverage_max_addresses_per_run
        if entity_limit < 1 or address_limit < 1:
            raise ValueError("entity_limit and address_limit must be positive")
        if not entity_types:
            raise ValueError("at least one entity type is required")

        entity_limit = min(entity_limit, self._entity_capacity_limit())
        planned_entities = self._select_due_entities(limit=entity_limit, now=observed_at)
        planned_addresses = self._select_due_addresses(
            limit=min(address_limit, self._remaining_request_capacity(
                consumed_requests=_DISCOVERY_REQUEST_COUNT + 2 * len(planned_entities)
            )),
            now=observed_at,
        )
        if dry_run:
            return CoverageSyncResult(
                status="dry_run",
                dry_run=True,
                run_id=None,
                entity_discovery_requests=2,
                entity_count=len(planned_entities),
                entity_detail_requests=len(planned_entities),
                prediction_requests=len(planned_entities),
                prediction_address_count=0,
                address_enrichment_requests=len(planned_addresses),
                address_evidence_count=0,
                duplicate_count=0,
                response_bytes=0,
                estimated_points=0,
                outcome_counts={},
                written_paths=(),
            )

        if self.settings.provider_token_value() is None:
            raise ProviderTokenMissing()
        run_id = self.quota.create_run(
            mode="execute",
            request_limit=self.settings.coverage_requests_per_minute,
            response_bytes_budget=self.settings.coverage_response_bytes_budget,
            started_at=observed_at,
        )
        outcomes: Counter[str] = Counter()
        paths: list[str] = []
        response_bytes = 0
        request_count = 0
        duplicate_count = 0
        prediction_address_count = 0
        evidence_count = 0
        detail_requests = 0
        prediction_requests = 0
        discovery_requests = 0
        blocked = False

        discovered_entity_ids: list[str] = []
        for interval, order_by in (("30d", "balanceUsd"), ("7d", "balanceUsdChange")):
            outcome = self._dispatch(
                run_id=run_id,
                endpoint_template="/chaindata/intelligence/entity_balance_changes",
                query_profile=f"coverage_discovery:{interval}:{order_by}",
                request=self.provider.build_entity_balance_changes_request(
                    entity_types=entity_types, interval=interval, order_by=order_by
                ),
                estimated_bytes=_ESTIMATED_DISCOVERY_BYTES,
                observed_at=observed_at,
            )
            discovery_requests += outcome.requested
            request_count += outcome.requested
            response_bytes += outcome.response_bytes
            paths.extend(outcome.written_paths)
            outcomes[outcome.outcome] += 1
            if outcome.outcome == "success" and outcome.payload is not None and outcome.observation_id:
                try:
                    entities = parse_discovery_entities(outcome.payload)
                except CoveragePayloadError:
                    outcomes["malformed_payload"] += 1
                else:
                    duplicate_count += self._record_entities(
                        observation_id=outcome.observation_id,
                        entities=entities,
                        discovery_method="balance_changes",
                        observed_at=observed_at,
                    )[1]
                    discovered_entity_ids.extend(entity.provider_entity_id for entity in entities)
            elif outcome.outcome == "blocked":
                blocked = True
                break

        if not blocked:
            planned_entities = self._select_due_entities(
                limit=entity_limit, now=observed_at, extra_entity_ids=discovered_entity_ids
            )
            for entity_id in planned_entities:
                detail = self._dispatch(
                    run_id=run_id,
                    endpoint_template="/chaindata/intelligence/entity/{entity}",
                    query_profile="coverage_entity_detail",
                    request=self.provider.build_entity_request(entity_id),
                    estimated_bytes=_ESTIMATED_ENTITY_BYTES,
                    observed_at=observed_at,
                )
                detail_requests += detail.requested
                request_count += detail.requested
                response_bytes += detail.response_bytes
                paths.extend(detail.written_paths)
                outcomes[detail.outcome] += 1
                if detail.outcome == "success" and detail.payload and detail.observation_id:
                    try:
                        entity = parse_entity_detail(detail.payload, expected_entity_id=entity_id)
                    except CoveragePayloadError:
                        outcomes["malformed_payload"] += 1
                    else:
                        duplicate_count += self._record_entities(
                            observation_id=detail.observation_id,
                            entities=(entity,),
                            discovery_method="entity_detail",
                            observed_at=observed_at,
                        )[1]
                if detail.outcome == "blocked":
                    blocked = True
                    break

                predictions = self._dispatch(
                    run_id=run_id,
                    endpoint_template="/chaindata/intelligence/entity_predictions/{entity}",
                    query_profile="coverage_entity_predictions",
                    request=self.provider.build_entity_predictions_request(entity_id),
                    estimated_bytes=_ESTIMATED_PREDICTION_BYTES,
                    observed_at=observed_at,
                )
                prediction_requests += predictions.requested
                request_count += predictions.requested
                response_bytes += predictions.response_bytes
                paths.extend(predictions.written_paths)
                outcomes[predictions.outcome] += 1
                if predictions.outcome == "success" and predictions.payload and predictions.observation_id:
                    try:
                        addresses = parse_prediction_addresses(predictions.payload)
                    except CoveragePayloadError:
                        self._record_prediction_parse_result(
                            observation_id=predictions.observation_id,
                            entity_id=entity_id,
                            parse_outcome="malformed_payload",
                            observed_at=observed_at,
                        )
                        outcomes["malformed_payload"] += 1
                    else:
                        self._record_prediction_parse_result(
                            observation_id=predictions.observation_id,
                            entity_id=entity_id,
                            parse_outcome=(
                                "parsed_success" if addresses else "no_bitcoin_addresses"
                            ),
                            observed_at=observed_at,
                        )
                        inserted, duplicates = self._record_predictions(
                            observation_id=predictions.observation_id,
                            entity_id=entity_id,
                            addresses=addresses,
                            observed_at=observed_at,
                        )
                        prediction_address_count += inserted
                        duplicate_count += duplicates
                if predictions.outcome == "blocked":
                    blocked = True
                    break

        if not blocked:
            address_capacity = min(
                address_limit, self._remaining_request_capacity(consumed_requests=request_count)
            )
            for candidate in self._select_due_addresses(limit=address_capacity, now=observed_at):
                enrichment = self._dispatch(
                    run_id=run_id,
                    endpoint_template="/chaindata/intelligence/address_enriched/{address}",
                    query_profile="coverage_address_enrichment",
                    request=self.provider.build_btc_coverage_enrichment_request(
                        candidate.normalized_address
                    ),
                    estimated_bytes=_ESTIMATED_ADDRESS_BYTES,
                    observed_at=observed_at,
                    address_id=candidate.address_id,
                )
                request_count += enrichment.requested
                response_bytes += enrichment.response_bytes
                paths.extend(enrichment.written_paths)
                outcomes[enrichment.outcome] += 1
                if enrichment.outcome == "success" and enrichment.payload and enrichment.observation_id:
                    try:
                        parsed = parse_bitcoin_response(
                            enrichment.payload, normalize_bitcoin_address(candidate.normalized_address)
                        )
                    except ProviderPayloadError:
                        self._record_address_parse_result(
                            observation_id=enrichment.observation_id,
                            address_id=candidate.address_id,
                            parse_outcome="malformed_payload",
                            observed_at=observed_at,
                        )
                        outcomes["malformed_payload"] += 1
                    else:
                        self._record_address_parse_result(
                            observation_id=enrichment.observation_id,
                            address_id=candidate.address_id,
                            parse_outcome="parsed_success",
                            observed_at=observed_at,
                        )
                        result = self.evidence.append_provider_candidates(
                            address=candidate.normalized_address,
                            observation_id=enrichment.observation_id,
                            candidates=parsed.evidence_candidates,
                            source_url=self.settings.provider_base_url,
                            artifact_sha256=enrichment.payload_sha256,
                            observed_at=observed_at,
                        )
                        evidence_count += result.inserted_count
                        duplicate_count += result.duplicate_count
                if enrichment.outcome == "blocked":
                    blocked = True
                    break

        status = "blocked" if blocked else "completed"
        if not blocked and any(key not in {"success"} for key in outcomes):
            status = "partial"
        self._complete_run(run_id, status=status, outcomes=outcomes, extra={
            "coverage_mode": "business_relevant_btc",
            "estimated_points": _estimate_points(response_bytes),
            "entity_types": list(entity_types),
        })
        return CoverageSyncResult(
            status=status,
            dry_run=False,
            run_id=run_id,
            entity_discovery_requests=discovery_requests,
            entity_count=len(planned_entities),
            entity_detail_requests=detail_requests,
            prediction_requests=prediction_requests,
            prediction_address_count=prediction_address_count,
            address_enrichment_requests=request_count - discovery_requests - detail_requests - prediction_requests,
            address_evidence_count=evidence_count,
            duplicate_count=duplicate_count,
            response_bytes=response_bytes,
            estimated_points=_estimate_points(response_bytes),
            outcome_counts=dict(outcomes),
            written_paths=tuple(paths),
        )

    def _entity_capacity_limit(self) -> int:
        """Leave at least one request for direct address enrichment per run."""

        return max(0, (self.settings.coverage_requests_per_minute - _DISCOVERY_REQUEST_COUNT - 1) // 2)

    def _remaining_request_capacity(self, *, consumed_requests: int) -> int:
        return max(0, self.settings.coverage_requests_per_minute - consumed_requests)

    def _select_due_entities(
        self,
        *,
        limit: int,
        now: datetime,
        extra_entity_ids: Iterable[str] = (),
    ) -> tuple[str, ...]:
        freshness_cutoff = _utc_string(now - timedelta(hours=self.settings.coverage_entity_ttl_hours))
        priority: dict[str, int] = {}
        with self.database.read_connection() as connection:
            fresh_entities = {
                row["provider_entity_id"]
                for row in connection.execute(
                    """
                    SELECT provider_entity_id FROM coverage_entity_observation
                    WHERE discovery_method = 'entity_detail' AND observed_at > ?
                    """,
                    (freshness_cutoff,),
                ).fetchall()
            }
            fresh_prediction_entities = {
                row["provider_entity_id"]
                for row in connection.execute(
                    """
                    SELECT provider_entity_id FROM coverage_entity_prediction_parse_result
                    WHERE parse_outcome IN ('parsed_success', 'no_bitcoin_addresses')
                      AND parsed_at > ?
                    """,
                    (freshness_cutoff,),
                ).fetchall()
            }
            fresh_entities &= fresh_prediction_entities
            for entity_id in extra_entity_ids:
                if entity_id not in fresh_entities:
                    priority[entity_id] = max(priority.get(entity_id, 0), 50)
            for row in connection.execute(
                """
                SELECT provider_entity_id, MAX(priority) AS priority
                FROM coverage_entity_seed GROUP BY provider_entity_id
                """
            ).fetchall():
                if row["provider_entity_id"] not in fresh_entities:
                    priority[row["provider_entity_id"]] = max(
                        priority.get(row["provider_entity_id"], 0), row["priority"]
                    )
            for row in connection.execute(
                """
                SELECT provider_entity_id FROM coverage_entity_observation
                WHERE discovery_method = 'entity_detail'
                GROUP BY provider_entity_id
                """
            ).fetchall():
                if row["provider_entity_id"] not in fresh_entities:
                    priority.setdefault(row["provider_entity_id"], 50)
        return tuple(entity for entity, _ in sorted(priority.items(), key=lambda item: (-item[1], item[0]))[:limit])

    def _select_due_addresses(self, *, limit: int, now: datetime) -> tuple[CoverageAddressTarget, ...]:
        cutoff = _utc_string(now - timedelta(hours=self.settings.coverage_address_ttl_hours))
        with self.database.read_connection() as connection:
            candidate_rows = connection.execute(
                """
                SELECT cr.address_id, address.normalized_address,
                       MAX(cr.priority) AS priority
                FROM candidate_request AS cr
                JOIN address_subject AS address ON address.address_id = cr.address_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM coverage_address_parse_result AS parse_result
                    WHERE parse_result.address_id = cr.address_id
                      AND parse_result.parse_outcome = 'parsed_success'
                      AND parse_result.parsed_at > ?
                )
                GROUP BY cr.address_id, address.normalized_address
                ORDER BY priority DESC, MIN(cr.requested_at) ASC, cr.address_id ASC
                LIMIT ?
                """,
                (cutoff, limit),
            ).fetchall()
            prediction_rows = connection.execute(
                """
                SELECT prediction.address_id, address.normalized_address,
                       MIN(prediction.prediction_rank) AS prediction_rank
                FROM coverage_entity_prediction AS prediction
                JOIN address_subject AS address ON address.address_id = prediction.address_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM coverage_address_parse_result AS parse_result
                    WHERE parse_result.address_id = prediction.address_id
                      AND parse_result.parse_outcome = 'parsed_success'
                      AND parse_result.parsed_at > ?
                )
                GROUP BY prediction.address_id, address.normalized_address
                ORDER BY prediction_rank ASC, MIN(prediction.observed_at) ASC,
                         prediction.address_id ASC
                LIMIT ?
                """,
                (cutoff, limit),
            ).fetchall()
        selected: list[CoverageAddressTarget] = []
        seen: set[str] = set()
        for row in candidate_rows:
            if row["address_id"] in seen:
                continue
            seen.add(row["address_id"])
            selected.append(
                CoverageAddressTarget(
                    address_id=row["address_id"],
                    normalized_address=row["normalized_address"],
                    priority=row["priority"],
                    source_kind="candidate_request",
                )
            )
        for row in prediction_rows:
            if len(selected) >= limit:
                break
            if row["address_id"] in seen:
                continue
            seen.add(row["address_id"])
            selected.append(
                CoverageAddressTarget(
                    address_id=row["address_id"],
                    normalized_address=row["normalized_address"],
                    priority=0,
                    source_kind="entity_prediction",
                )
            )
        return tuple(selected)

    def _record_prediction_parse_result(
        self,
        *,
        observation_id: str,
        entity_id: str,
        parse_outcome: str,
        observed_at: datetime,
    ) -> None:
        timestamp = _utc_string(observed_at)
        fingerprint = _sha256(
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
                    prediction_parse_result_id, prediction_parse_result_fingerprint,
                    observation_id, provider_entity_id, parse_outcome, parsed_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    fingerprint,
                    observation_id,
                    entity_id,
                    parse_outcome,
                    timestamp,
                ),
            )

    def _record_address_parse_result(
        self,
        *,
        observation_id: str,
        address_id: str,
        parse_outcome: str,
        observed_at: datetime,
    ) -> None:
        timestamp = _utc_string(observed_at)
        fingerprint = _sha256(
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
                    parse_result_id, parse_result_fingerprint, observation_id,
                    address_id, parse_outcome, parsed_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (str(uuid.uuid4()), fingerprint, observation_id, address_id, parse_outcome, timestamp),
            )

    def _dispatch(
        self,
        *,
        run_id: str,
        endpoint_template: str,
        query_profile: str,
        request,
        estimated_bytes: int,
        observed_at: datetime,
        address_id: str | None = None,
    ) -> "_DispatchResult":
        try:
            reservation = self.quota.reserve(
                run_id=run_id, now=observed_at, estimated_response_bytes=estimated_bytes
            )
        except RateLimitExceeded:
            return _DispatchResult("rate_limited", 0, 0, (), None, None, None)
        except ByteBudgetExceeded:
            return _DispatchResult("budget_exhausted", 0, 0, (), None, None, None)
        try:
            response = self.provider.fetch_request(request)
        except ProviderTokenMissing:
            self.quota.complete(reservation.reservation_id, actual_response_bytes=0, outcome="failed")
            return _DispatchResult("blocked", 0, 0, (), None, None, None)

        stored = self.raw_payloads.persist(response.body) if response.body else None
        outcome = response.outcome
        observation_id = str(uuid.uuid4())
        self._record_observation(
            observation_id=observation_id,
            run_id=run_id,
            endpoint_template=endpoint_template,
            query_profile=query_profile,
            observed_at=observed_at,
            http_status=response.http_status,
            outcome=outcome,
            response_bytes=len(response.body),
            payload_sha256=stored.payload_sha256 if stored else None,
            address_id=address_id,
        )
        self.quota.complete(
            reservation.reservation_id,
            actual_response_bytes=len(response.body),
            outcome="completed" if outcome == "success" else "rate_limited" if outcome == "rate_limited" else "failed",
        )
        return _DispatchResult(
            outcome,
            1,
            len(response.body),
            (stored.relative_path,) if stored else (),
            response.body if response.outcome == "success" else None,
            observation_id,
            stored.payload_sha256 if stored else None,
        )

    def _record_observation(
        self,
        *,
        observation_id: str,
        run_id: str,
        endpoint_template: str,
        query_profile: str,
        observed_at: datetime,
        http_status: int | None,
        outcome: str,
        response_bytes: int,
        payload_sha256: str | None,
        address_id: str | None,
    ) -> None:
        timestamp = _utc_string(observed_at)
        with self.database.write_transaction() as connection:
            connection.execute(
                """
                INSERT INTO source_observation (
                    observation_id, source_id, source_version, source_kind,
                    endpoint_template, query_profile, requested_at, completed_at,
                    http_status, outcome, response_bytes, payload_sha256,
                    chain_key, address_id, ingestion_run_id
                ) VALUES (?, '0xrouter', 'chaindata_coverage_v1', 'provider', ?, ?, ?, ?,
                          ?, ?, ?, ?, 'bitcoin', ?, ?)
                """,
                (
                    observation_id,
                    endpoint_template,
                    query_profile,
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

    def _record_entities(
        self,
        *,
        observation_id: str,
        entities: tuple["CoverageEntity", ...],
        discovery_method: str,
        observed_at: datetime,
    ) -> tuple[int, int]:
        timestamp = _utc_string(observed_at)
        inserted = 0
        duplicates = 0
        with self.database.write_transaction() as connection:
            for entity in entities:
                fingerprint = _sha256(
                    {
                        "observation_id": observation_id,
                        "provider_entity_id": entity.provider_entity_id,
                        "provider_entity_name": entity.provider_entity_name,
                        "provider_entity_type": entity.provider_entity_type,
                        "discovery_method": discovery_method,
                        "discovery_rank": entity.rank,
                    }
                )
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO coverage_entity_observation (
                        entity_observation_id, entity_observation_fingerprint, observation_id,
                        provider_entity_id, provider_entity_name, provider_entity_type,
                        discovery_method, discovery_rank, observed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        fingerprint,
                        observation_id,
                        entity.provider_entity_id,
                        entity.provider_entity_name,
                        entity.provider_entity_type,
                        discovery_method,
                        entity.rank,
                        timestamp,
                    ),
                )
                if cursor.rowcount == 1:
                    inserted += 1
                else:
                    duplicates += 1
        return inserted, duplicates

    def _record_predictions(
        self,
        *,
        observation_id: str,
        entity_id: str,
        addresses: tuple[str, ...],
        observed_at: datetime,
    ) -> tuple[int, int]:
        timestamp = _utc_string(observed_at)
        inserted = 0
        duplicates = 0
        with self.database.write_transaction() as connection:
            for rank, address in enumerate(addresses, start=1):
                subject = normalize_bitcoin_address(address)
                connection.execute(
                    """
                    INSERT OR IGNORE INTO address_subject (
                        address_id, chain_key, normalized_address, display_address,
                        address_type, first_seen_at
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
                fingerprint = _sha256(
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
                        provider_entity_id, address_id, prediction_rank, observed_at
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
        self, run_id: str, *, status: str, outcomes: Counter[str], extra: dict[str, Any]
    ) -> None:
        with self.database.write_transaction() as connection:
            connection.execute(
                """
                UPDATE ingestion_run
                SET status = ?, completed_at = ?, summary_json = ?
                WHERE ingestion_run_id = ?
                """,
                (status, _utc_string(datetime.now(UTC)), json.dumps({**extra, "outcomes": dict(outcomes)}, sort_keys=True), run_id),
            )


@dataclass(frozen=True)
class _DispatchResult:
    outcome: str
    requested: int
    response_bytes: int
    written_paths: tuple[str, ...]
    payload: bytes | None
    observation_id: str | None
    payload_sha256: str | None


@dataclass(frozen=True)
class CoverageEntity:
    provider_entity_id: str
    provider_entity_name: str | None
    provider_entity_type: str | None
    rank: int | None


def parse_discovery_entities(payload: bytes) -> tuple[CoverageEntity, ...]:
    decoded = _json_value(payload)
    records = decoded if isinstance(decoded, list) else _first_list(decoded, "entities", "results", "data")
    if records is None:
        raise CoveragePayloadError("entity discovery has no list")
    entities = tuple(
        entity
        for rank, record in enumerate(records, start=1)
        if (entity := _parse_entity_record(record, rank=rank)) is not None
    )
    if not entities:
        raise CoveragePayloadError("entity discovery has no valid entities")
    return _dedupe_entities(entities)


def parse_entity_detail(payload: bytes, *, expected_entity_id: str) -> CoverageEntity:
    decoded = _json_object(payload)
    record = decoded.get("entity") if isinstance(decoded.get("entity"), dict) else decoded
    entity = _parse_entity_record(record, rank=None)
    if entity is None or entity.provider_entity_id != expected_entity_id:
        raise CoveragePayloadError("entity detail does not match requested identifier")
    return entity


def parse_prediction_addresses(payload: bytes) -> tuple[str, ...]:
    decoded = _json_value(payload)
    records = decoded if isinstance(decoded, list) else _first_list(decoded, "addresses", "predictions", "results", "data")
    if records is None:
        raise CoveragePayloadError("entity predictions has no list")
    addresses: list[str] = []
    for item in records:
        raw = item if isinstance(item, str) else item.get("address") if isinstance(item, dict) else None
        if not isinstance(raw, str):
            continue
        try:
            subject = normalize_bitcoin_address(raw)
        except BitcoinAddressError:
            continue
        addresses.append(subject.normalized_address)
    return tuple(dict.fromkeys(addresses))


def _json_value(payload: bytes) -> dict[str, Any] | list[Any]:
    try:
        decoded = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CoveragePayloadError("provider body is not JSON") from exc
    if not isinstance(decoded, (dict, list)):
        raise CoveragePayloadError("provider body must be an object or list")
    return decoded


def _json_object(payload: bytes) -> dict[str, Any]:
    decoded = _json_value(payload)
    if not isinstance(decoded, dict):
        raise CoveragePayloadError("provider body is not an object")
    return decoded


def _first_list(decoded: dict[str, Any], *keys: str) -> list[Any] | None:
    for key in keys:
        value = decoded.get(key)
        if isinstance(value, list):
            return value
    return None


def _parse_entity_record(record: Any, *, rank: int | None) -> CoverageEntity | None:
    if not isinstance(record, dict):
        return None
    entity_id = record.get("id") or record.get("entityId") or record.get("entity_id")
    if not isinstance(entity_id, str) or not entity_id.strip() or any(char.isspace() for char in entity_id):
        return None
    name = record.get("name") or record.get("entityName")
    entity_type = record.get("type") or record.get("entityType") or record.get("entity_type")
    return CoverageEntity(
        provider_entity_id=entity_id.strip(),
        provider_entity_name=name.strip() if isinstance(name, str) and name.strip() else None,
        provider_entity_type=entity_type.strip() if isinstance(entity_type, str) and entity_type.strip() else None,
        rank=rank,
    )


def _dedupe_entities(entities: tuple[CoverageEntity, ...]) -> tuple[CoverageEntity, ...]:
    selected: dict[str, CoverageEntity] = {}
    for entity in entities:
        selected.setdefault(entity.provider_entity_id, entity)
    return tuple(selected.values())


def _estimate_points(response_bytes: int) -> int:
    return (response_bytes + _POINT_UNIT_BYTES - 1) // _POINT_UNIT_BYTES


def _sha256(value: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _utc_string(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
