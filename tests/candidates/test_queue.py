from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from crypto_address_identity.candidates import (
    ByteBudgetExceeded,
    CandidateInput,
    CandidateService,
    QuotaManager,
    RateLimitExceeded,
)
from crypto_address_identity.storage.sqlite import IdentityDatabase


BTC_ADDRESS = "1BoatSLRHtKNngkdXEeobR76b53LETtpyT"


def _candidate(*, priority: int = 80, source_reference: str = "fixture-1") -> CandidateInput:
    return CandidateInput.model_validate(
        {
            "chain_key": "bitcoin",
            "address": BTC_ADDRESS,
            "reason": "whale_counterparty",
            "priority": priority,
            "source_reference": source_reference,
            "requested_at": "2026-07-22T00:00:00Z",
        }
    )


def test_repeated_candidate_import_preserves_provenance_without_duplicate_subject(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    service = CandidateService(database)

    result = service.import_candidates([_candidate(), _candidate(source_reference="fixture-2")])

    assert result.imported_count == 2
    with database.read_connection() as connection:
        subject_count = connection.execute("SELECT COUNT(*) FROM address_subject").fetchone()[0]
        candidate_count = connection.execute("SELECT COUNT(*) FROM candidate_request").fetchone()[0]
    assert subject_count == 1
    assert candidate_count == 2


def test_priority_selection_is_descending_then_oldest(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    service = CandidateService(database)
    service.import_candidates(
        [
            _candidate(priority=40, source_reference="low"),
            _candidate(priority=90, source_reference="high"),
        ]
    )

    selected = service.select_candidates(limit=2)

    assert [candidate.source_reference for candidate in selected] == ["high", "low"]


def test_shared_rolling_window_rejects_twenty_first_reservation(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    quota = QuotaManager(database)
    run_id = quota.create_run(mode="execute", request_limit=20, response_bytes_budget=1_000)
    now = datetime(2026, 7, 22, tzinfo=UTC)

    for _ in range(20):
        quota.reserve(run_id=run_id, now=now, estimated_response_bytes=1)

    with pytest.raises(RateLimitExceeded):
        quota.reserve(run_id=run_id, now=now + timedelta(seconds=59), estimated_response_bytes=1)

    quota.reserve(run_id=run_id, now=now + timedelta(seconds=60), estimated_response_bytes=1)


def test_response_budget_accounts_for_reserved_and_actual_bytes(runtime_root) -> None:
    database = IdentityDatabase(runtime_root / "identity.sqlite3")
    database.migrate()
    quota = QuotaManager(database)
    run_id = quota.create_run(mode="execute", request_limit=20, response_bytes_budget=100)
    reservation = quota.reserve(
        run_id=run_id,
        now=datetime(2026, 7, 22, tzinfo=UTC),
        estimated_response_bytes=80,
    )
    quota.complete(reservation.reservation_id, actual_response_bytes=90, outcome="completed")

    with pytest.raises(ByteBudgetExceeded):
        quota.reserve(
            run_id=run_id,
            now=datetime(2026, 7, 22, 0, 1, tzinfo=UTC),
            estimated_response_bytes=11,
        )
