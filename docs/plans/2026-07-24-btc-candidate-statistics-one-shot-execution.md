# BTC Candidate Statistics One-Shot Execution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute the checksum-pinned BTC candidate-statistics aggregate exactly
once under a 650 billion-byte BigQuery billing cap, validate its single
identifier-free row, and stop before candidate materialization.

**Architecture:** Keep the fixed aggregate SQL and cost-only probe unchanged.
Add an isolated one-shot execution harness that performs the existing
metadata/monthly-usage/dry-run gate, writes an exclusive local authorization
receipt before submitting one deterministic BigQuery job, fetches at most two
rows with SDK retries disabled, and validates the result through the existing
candidate-statistics quality contract.

**Tech Stack:** Python 3.13, Pydantic, Google Cloud BigQuery SDK, pytest.

---

## Task 1: Lock The Execution Contract

**Files:**

- Add: `tests/universe/test_candidate_execution.py`
- Modify: `tests/universe/test_bigquery_probe.py`
- Modify: `tests/universe/test_cli.py`

- [x] Add failing tests for fixed query/schema checksums, cutoff height/time,
      the `650_000_000_000` cap, and the `1_557_951_354` source baseline.
- [x] Prove offline dry-run constructs no backend and writes no receipt.
- [x] Prove an existing receipt blocks before any network request.
- [x] Prove the cost gate runs before receipt creation and execution.
- [x] Prove exactly one backend execution call and no automatic retry.
- [x] Prove zero rows, two rows, malformed rows, and identifier-bearing rows
      are quality-blocked without candidate materialization.
- [x] Prove a valid one-row result completes the receipt and remains
      aggregate-only.

## Task 2: Add The No-Retry BigQuery Boundary

**Files:**

- Modify: `src/crypto_address_identity/universe/bigquery.py`

- [x] Add a typed result containing at most two normalized rows and billed /
      processed byte counters.
- [x] Add a dedicated backend protocol method for one-shot aggregate execution.
- [x] Submit with a deterministic job id and
      `Client.query(..., retry=None, job_retry=None)`.
- [x] Fetch with
      `QueryJob.result(..., retry=None, job_retry=None, max_results=2,
      page_size=2)`.
- [x] Recursively normalize BigQuery nested rows without exposing raw upstream
      errors.

## Task 3: Implement Exclusive Receipt And Quality Gates

**Files:**

- Add: `src/crypto_address_identity/universe/candidate_execution.py`

- [x] Validate the authorization id, fixed cap, expected source baseline,
      query hash, schema hash, cutoff, budget, and reserve before network use.
- [x] Run the existing metadata/monthly-usage/dry-run probe and require
      `within_budget`.
- [x] Create a mode-0600 receipt with exclusive-create semantics before query
      submission; a pre-existing receipt permanently blocks re-execution.
- [x] Execute the query once and validate the result with
      `parse_candidate_statistics_rows`.
- [x] Update the receipt atomically to `completed`, `quality_blocked`, or
      `failed`, storing only aggregate-safe evidence.
- [x] Never create candidate rows, materialization artifacts, provider calls,
      or automatic retry loops.

## Task 4: Add The Explicit CLI

**Files:**

- Modify: `src/crypto_address_identity/cli.py`
- Modify: `docs/btc_identity_operations.md`

- [x] Add:

```text
cai universe execute bigquery-candidate-statistics --dry-run ...
cai universe execute bigquery-candidate-statistics --execute-once ...
```

- [x] Require explicit authorization id, as-of date, cutoff height, expected
      query/schema checksums, expected source baseline, exact billing cap,
      Sandbox budget, and reserve.
- [x] Return structured aggregate-only output with receipt status, cost gate,
      result quality, row count, billed/processed bytes, and zero provider
      calls/points.
- [x] Document that this command does not materialize candidate addresses and
      that reruns with the same authorization id are blocked.

## Task 5: Verify, Merge, And Execute Once

- [x] Run targeted candidate execution, BigQuery, and CLI tests.
- [x] Run all universe tests and the full repository suite.
- [x] Run Python compile checks and `git diff --check`.
- [x] Review for no identifiers, no secrets, no retries, exclusive receipt
      semantics, and no materialization path.
- [x] Stage only this feature, run staged preflight, commit, fast-forward
      `main`, and push exact reviewed code.
- [x] Re-run the live checkpoint/cost gate from exact `origin/main`; abort on
      any checksum, cutoff, usage, or reserve drift.
- [x] Execute one authorized aggregate job and inspect the sanitized receipt.
- [x] Verify the same authorization id cannot submit a second job.
- [x] Decide the next phase:
  - P0 `<=120,000` and coarse union `<=5,000,000`: design candidate
    materialization.
  - P0 `120,001..1,000,000`: rank in tiers before materialization.
  - P0 `>1,000,000`: pause and revise `btc_importance_v2`.
  - Edge frontier `>1,000,000`: defer graph expansion.

## Acceptance Criteria

- The execution uses the pinned query and schema and a maximum billed-byte cap
  of exactly 650 billion bytes.
- The expected standard-address baseline is exactly 1,557,951,354.
- The SDK receives no query or result retries.
- The harness obtains at most two rows and accepts exactly one.
- Accepted output contains only aggregate fields and no address or transaction
  hash.
- Every execution attempt has a durable exclusive receipt before submission.
- A failed or blocked quality report cannot materialize candidates.
- Provider usage, identity evidence, resolver state, and consumers remain
  unchanged.
