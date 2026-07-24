# BTC Importance V2 One-Shot Execution Plan

**Goal:** Execute the fixed `btc_importance_v2` aggregate census exactly once
after Billing activation, without returning or materializing any Bitcoin
address or transaction identifier.

**Architecture:** Preserve the dry-run-only v2 probe and all v1 execution
artifacts. Add an isolated v2 one-shot executor with a deterministic BigQuery
job id, an exclusive local receipt, exact checkpoint matching, a 650 billion
byte per-query cap, a 2 trillion byte monthly project-processing budget, and
zero automatic retries.

## Safety Boundary

- Execute only the fixed SQL whose SHA-256 is
  `47b0b8977cc1443578bc3daf3f90a2cf5e0e48ae758a4b7a133d3caa7d301e74`.
- Pin cutoff height `959187`, cutoff date `2026-07-24`, source-address baseline
  `1,557,941,780`, input-only baseline `3`, and schema SHA-256
  `7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7`.
- Require the dry-run estimate to remain exactly `637,999,682,243` bytes.
- Set `maximum_bytes_billed=650_000_000_000`.
- Set the project monthly processing budget to `2_000_000_000_000` bytes and
  retain at least `250_000_000_000` bytes after the projected query.
- Require explicit acknowledgement that Billing is enabled.
- Compare successful query-job count and month-to-date billed bytes with a
  fresh live checkpoint immediately before execution.
- Never retry automatically. Any receipt, including `started` or `failed`,
  permanently consumes the authorization id.
- Store only identifier-free aggregate results in the local mode-0600 receipt.
- Do not materialize candidates, call identity providers, or change consumers.

## Tasks

### Task 1: Tests First

- [x] Add failing tests for the pinned v2 execution request and Billing
  acknowledgement.
- [x] Add failing tests for offline preview, exclusive receipt, exact cost and
  usage checkpoint matching, one execution call, malformed result handling,
  safe failure recording, and second-run rejection.
- [x] Add failing CLI tests for explicit `--dry-run` and `--execute-once`.

### Task 2: One-Shot Executor

- [x] Add `candidate_execution_v2.py` without modifying the v1 executor.
- [x] Reuse the v2 cost-only probe and strict result parser.
- [x] Create a deterministic job id and exclusive receipt before query
  submission.
- [x] Invoke only `execute_aggregate_at_most_two_no_retry`.
- [x] Record identifier-free outcome and quality evidence atomically.

### Task 3: CLI And Documentation

- [x] Add `cai universe execute bigquery-candidate-statistics-v2`.
- [x] Require all pinned contract values, live usage checkpoint values, and
  explicit Billing acknowledgement.
- [x] Document preview, execution, receipt, and no-materialization semantics.

### Task 4: Verification And Execution

- [x] Run targeted and full tests, compile checks, diff checks, and staged
  preflight.
- [x] Review for v1 immutability, no identifiers, no provider calls, no
  materialization, and no automatic retries.
- [x] Commit, merge, and push the executor before live use.
- [ ] Run one escalated-network live checkpoint and pin its usage counters.
- [ ] Run one CLI preview and verify no receipt exists.
- [ ] Execute exactly once only if every checkpoint and invariant passes.
- [ ] Parse the single aggregate row through v2 quality gates.
- [ ] Record aggregate-only execution evidence and the materialization
  decision without committing the private receipt.

## Acceptance Criteria

- The execution SQL, cutoff, schema, source baselines, dry-run bytes, byte cap,
  monthly budget, and live usage checkpoint all match exactly.
- A receipt is created before the only BigQuery execution call.
- A second invocation cannot submit another query.
- Result cardinality is exactly one and contains no identifier field.
- Quality failure blocks interpretation and candidate materialization but does
  not erase the execution receipt.
- The final report exposes P0, P1, edge, coarse, overlap, and quality counts
  only.
