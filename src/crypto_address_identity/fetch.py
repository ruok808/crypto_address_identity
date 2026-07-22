"""Fixture-tested BTC-only provider fetch orchestration."""

from __future__ import annotations

import json
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from crypto_address_identity.candidates import (
    ByteBudgetExceeded,
    CandidateService,
    QuotaManager,
    RateLimitExceeded,
    SelectedCandidate,
)
from crypto_address_identity.core.config import Settings
from crypto_address_identity.evidence import EvidenceService
from crypto_address_identity.observations import build_observation_metadata
from crypto_address_identity.providers.zero_x_router import (
    ProviderPayloadError,
    ProviderProfile,
    ProviderTokenMissing,
    ZeroXRouterClient,
    parse_bitcoin_response,
)
from crypto_address_identity.storage.raw_payloads import RawPayloadStore
from crypto_address_identity.storage.sqlite import IdentityDatabase


_ESTIMATED_RESPONSE_BYTES = 65_536
_HIGH_PRIORITY = 90


@dataclass(frozen=True)
class FetchRunResult:
    status: str
    dry_run: bool
    run_id: str | None
    selected_count: int
    skipped_fresh_count: int
    duplicate_candidate_count: int
    profile_counts: dict[str, int]
    request_count: int
    response_bytes: int
    outcome_counts: dict[str, int]
    evidence_count: int
    written_paths: tuple[str, ...]


class FetchService:
    """Orchestrates safe provider observations after candidate validation."""

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
        self.candidates = CandidateService(database)
        self.quota = QuotaManager(database)

    def run(self, *, dry_run: bool, limit: int, now: datetime | None = None) -> FetchRunResult:
        if limit < 1:
            raise ValueError("limit must be positive")
        observed_at = (now or datetime.now(UTC)).astimezone(UTC)
        selected = self.candidates.select_candidates(limit=limit)
        unique_selected: list[SelectedCandidate] = []
        duplicate_candidates: list[SelectedCandidate] = []
        seen_address_ids: set[str] = set()
        for candidate in selected:
            if candidate.address_id in seen_address_ids:
                duplicate_candidates.append(candidate)
            else:
                seen_address_ids.add(candidate.address_id)
                unique_selected.append(candidate)
        work = [(candidate, self._select_profile(candidate, observed_at)) for candidate in unique_selected]
        active_work = [(candidate, profile) for candidate, profile in work if profile is not None]
        skipped_fresh_count = len(work) - len(active_work)
        profile_counts = Counter(profile.value for _, profile in active_work)
        if dry_run:
            return FetchRunResult(
                status="dry_run",
                dry_run=True,
                run_id=None,
                selected_count=len(active_work),
                skipped_fresh_count=skipped_fresh_count,
                duplicate_candidate_count=len(duplicate_candidates),
                profile_counts=dict(profile_counts),
                request_count=0,
                response_bytes=0,
                outcome_counts={},
                evidence_count=0,
                written_paths=(),
            )

        run_id = self.quota.create_run(
            mode="execute",
            request_limit=self.settings.requests_per_minute,
            response_bytes_budget=self.settings.response_bytes_budget,
            started_at=observed_at,
        )
        outcomes: Counter[str] = Counter()
        written_paths: list[str] = []
        evidence_count = 0
        request_count = 0
        response_bytes = 0
        blocked = False

        for candidate in duplicate_candidates:
            outcomes["deduplicated"] += 1
            self._record_attempt(candidate, run_id, None, "deduplicated", observed_at, None)

        for candidate, profile in work:
            if profile is None:
                outcomes["skipped_fresh"] += 1
                self._record_attempt(candidate, run_id, None, "skipped_fresh", observed_at, None)
                continue
            try:
                reservation = self.quota.reserve(
                    run_id=run_id,
                    now=observed_at,
                    estimated_response_bytes=_ESTIMATED_RESPONSE_BYTES,
                )
            except RateLimitExceeded:
                outcomes["rate_limited"] += 1
                self._record_attempt(candidate, run_id, profile, "rate_limited", observed_at, None)
                continue
            except ByteBudgetExceeded:
                outcomes["budget_exhausted"] += 1
                self._record_attempt(candidate, run_id, profile, "budget_exhausted", observed_at, None)
                continue

            try:
                response = self.provider.fetch(candidate.normalized_address, profile)
            except ProviderTokenMissing:
                self.quota.complete(reservation.reservation_id, actual_response_bytes=0, outcome="failed")
                outcomes["blocked"] += 1
                self._record_attempt(candidate, run_id, profile, "blocked", observed_at, None)
                blocked = True
                break

            request_count += 1
            response_bytes += len(response.body)
            stored = self.raw_payloads.persist(response.body) if response.body else None
            observation_id = str(uuid.uuid4())
            outcome = response.outcome
            schema_fingerprint = None
            parsed = None
            if response.outcome == "success":
                try:
                    parsed = parse_bitcoin_response(
                        response.body, _subject_for_candidate(candidate.normalized_address)
                    )
                    schema_fingerprint = parsed.schema_fingerprint
                except ProviderPayloadError:
                    outcome = "malformed_payload"

            self._record_observation(
                observation_id=observation_id,
                run_id=run_id,
                candidate=candidate,
                profile=profile,
                observed_at=observed_at,
                http_status=response.http_status,
                outcome=outcome,
                response_bytes=len(response.body),
                payload_sha256=stored.payload_sha256 if stored else None,
                schema_fingerprint=schema_fingerprint,
            )
            if stored:
                written_paths.append(stored.relative_path)
            if parsed is not None:
                append_result = self.evidence.append_provider_candidates(
                    address=candidate.normalized_address,
                    observation_id=observation_id,
                    candidates=parsed.evidence_candidates,
                    source_url=self.settings.provider_base_url,
                    artifact_sha256=stored.payload_sha256 if stored else None,
                    observed_at=observed_at,
                )
                evidence_count += append_result.inserted_count

            completion_outcome = {
                "success": "completed",
                "rate_limited": "rate_limited",
            }.get(outcome, "failed")
            self.quota.complete(
                reservation.reservation_id,
                actual_response_bytes=len(response.body),
                outcome=completion_outcome,
            )
            self._record_attempt(candidate, run_id, profile, outcome, observed_at, observation_id)
            outcomes[outcome] += 1

        problem_outcomes = set(outcomes) - {"success", "skipped_fresh"}
        status = "blocked" if blocked else "completed" if not problem_outcomes else "partial"
        self._complete_run(run_id, status, outcomes, evidence_count)
        return FetchRunResult(
            status=status,
            dry_run=False,
            run_id=run_id,
            selected_count=len(active_work),
            skipped_fresh_count=skipped_fresh_count,
            duplicate_candidate_count=len(duplicate_candidates),
            profile_counts=dict(profile_counts),
            request_count=request_count,
            response_bytes=response_bytes,
            outcome_counts=dict(outcomes),
            evidence_count=evidence_count,
            written_paths=tuple(written_paths),
        )

    def _select_profile(self, candidate: SelectedCandidate, now: datetime) -> ProviderProfile | None:
        now_utc = _utc_string(now)
        near_expiry = _utc_string(now.astimezone(UTC) + timedelta(days=7))
        with self.database.read_connection() as connection:
            latest_discovery = connection.execute(
                """
                SELECT completed_at FROM source_observation
                WHERE address_id = ? AND source_id = '0xrouter'
                  AND query_profile = 'discovery' AND outcome = 'success'
                ORDER BY completed_at DESC LIMIT 1
                """,
                (candidate.address_id,),
            ).fetchone()
            latest_detail = connection.execute(
                """
                SELECT completed_at FROM source_observation
                WHERE address_id = ? AND source_id = '0xrouter'
                  AND query_profile = 'detail' AND outcome = 'success'
                ORDER BY completed_at DESC LIMIT 1
                """,
                (candidate.address_id,),
            ).fetchone()
            has_active_conflict = connection.execute(
                "SELECT 1 FROM conflict_set WHERE address_id = ? AND status = 'active' LIMIT 1",
                (candidate.address_id,),
            ).fetchone()
            has_changed_discovery = connection.execute(
                """
                SELECT COUNT(DISTINCT payload_sha256) > 1
                FROM source_observation
                WHERE address_id = ? AND source_id = '0xrouter'
                  AND query_profile = 'discovery' AND outcome = 'success'
                  AND payload_sha256 IS NOT NULL
                """,
                (candidate.address_id,),
            ).fetchone()[0]
            has_near_expiry_evidence = connection.execute(
                """
                SELECT 1 FROM identity_evidence
                WHERE address_id = ? AND expires_at > ? AND expires_at <= ?
                LIMIT 1
                """,
                (candidate.address_id, now_utc, near_expiry),
            ).fetchone()
        if latest_discovery is None:
            return ProviderProfile.DISCOVERY
        if latest_detail is not None and _is_fresh(
            latest_detail["completed_at"], now, self.settings.detail_ttl_hours
        ):
            return None
        discovery_is_fresh = _is_fresh(
            latest_discovery["completed_at"], now, self.settings.discovery_ttl_hours
        )
        needs_detail = bool(
            candidate.priority >= _HIGH_PRIORITY
            or has_active_conflict
            or has_changed_discovery
            or has_near_expiry_evidence
        )
        if discovery_is_fresh and latest_detail is None and needs_detail:
            return ProviderProfile.DETAIL
        if discovery_is_fresh:
            return None
        return ProviderProfile.DISCOVERY

    def _record_observation(
        self,
        *,
        observation_id: str,
        run_id: str,
        candidate: SelectedCandidate,
        profile: ProviderProfile | None,
        observed_at: datetime,
        http_status: int | None,
        outcome: str,
        response_bytes: int,
        payload_sha256: str | None,
        schema_fingerprint: str | None,
    ) -> None:
        metadata = build_observation_metadata(
            endpoint_template="/chaindata/intelligence/address_enriched/{address}/all",
            query_profile=profile.value,
        )
        timestamp = _utc_string(observed_at)
        with self.database.write_transaction() as connection:
            connection.execute(
                """
                INSERT INTO source_observation (
                    observation_id, source_id, source_version, source_kind,
                    endpoint_template, query_profile, requested_at, completed_at,
                    http_status, outcome, response_bytes, payload_sha256,
                    schema_fingerprint, chain_key, address_id, ingestion_run_id
                ) VALUES (?, '0xrouter', 'address_enriched_v1', 'provider', ?, ?, ?, ?,
                          ?, ?, ?, ?, ?, 'bitcoin', ?, ?)
                """,
                (
                    observation_id,
                    metadata.endpoint_template,
                    metadata.query_profile,
                    timestamp,
                    timestamp,
                    http_status,
                    outcome,
                    response_bytes,
                    payload_sha256,
                    schema_fingerprint,
                    candidate.address_id,
                    run_id,
                ),
            )

    def _record_attempt(
        self,
        candidate: SelectedCandidate,
        run_id: str,
        profile: ProviderProfile,
        outcome: str,
        observed_at: datetime,
        observation_id: str | None,
    ) -> None:
        with self.database.write_transaction() as connection:
            connection.execute(
                """
                INSERT INTO candidate_attempt (
                    candidate_attempt_id, candidate_request_id, ingestion_run_id,
                    profile, outcome, attempted_at, observation_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    candidate.candidate_request_id,
                    run_id,
                    profile.value if profile else None,
                    outcome,
                    _utc_string(observed_at),
                    observation_id,
                ),
            )

    def _complete_run(
        self, run_id: str, status: str, outcomes: Counter[str], evidence_count: int
    ) -> None:
        with self.database.write_transaction() as connection:
            connection.execute(
                """
                UPDATE ingestion_run
                SET status = ?, completed_at = ?, summary_json = ?
                WHERE ingestion_run_id = ?
                """,
                (
                    status,
                    _utc_string(datetime.now(UTC)),
                    json.dumps(
                        {"outcomes": dict(outcomes), "evidence_count": evidence_count},
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    run_id,
                ),
            )


def _subject_for_candidate(address: str):
    from crypto_address_identity.chains.bitcoin import normalize_bitcoin_address

    return normalize_bitcoin_address(address)


def _utc_string(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("Timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _is_fresh(completed_at: str, now: datetime, ttl_hours: int) -> bool:
    completed = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
    return completed + timedelta(hours=ttl_hours) > now.astimezone(UTC)
