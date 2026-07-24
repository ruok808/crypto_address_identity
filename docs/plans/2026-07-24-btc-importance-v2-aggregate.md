# BTC Importance V2 Aggregate-Only Implementation Plan

> **For agentic workers:** Execute this plan task by task with tests first.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the approved `btc_importance_v2` aggregate census with a
fixed identifier-free BigQuery SQL resource, strict result and quality models,
and a CLI that can only preview or submit a free BigQuery dry run.

**Architecture:** Preserve every v1 artifact and execution path. Add isolated
v2 SQL and Python contracts, then expose them through additive query-plan and
CLI fields. The live v2 CLI may call table metadata, monthly query usage, and
BigQuery dry run only. It has no billed execution or candidate-materialization
method.

**Tech Stack:** Python 3.13, Pydantic v2, fixed BigQuery Standard SQL,
argparse, pytest, SHA-256.

---

## Safety Boundary

- Do not modify `candidate_statistics.sql`, the v1 result model, the v1
  one-shot executor, or its immutable execution receipt.
- Do not call BigQuery query execution, create a destination table, export an
  address, or write a candidate file.
- Do not call Arkham, 0xRouter, Bitcoin Core, or another identity/provider
  endpoint.
- Do not change resolver precedence or any consumer behavior.
- Do not stage
  `docs/audits/2026-07-23-btc-third-strong-evidence-search.md`.
- A free live dry run is allowed only after local tests pass and the query
  checksum is reviewed.

## File Scope

Create:

- `src/crypto_address_identity/universe/sql/bigquery/candidate_statistics_v2.sql`
- `src/crypto_address_identity/universe/candidate_statistics_v2.py`
- `tests/universe/test_candidate_statistics_v2.py`

Modify:

- `src/crypto_address_identity/universe/query_plan.py`
- `src/crypto_address_identity/cli.py`
- `tests/universe/test_cli.py`
- `docs/btc_identity_operations.md`

## Task 1: Lock The V2 SQL Contract

- [x] Add failing query-plan tests for:
  - deterministic v2 checksum;
  - exactly one transaction-table reference;
  - one input/output stream;
  - one address-transaction aggregation;
  - 90/365-day receipt windows;
  - residual-gross subtraction;
  - retained, repeated, and sustained support;
  - strict, balanced, and retention-only variants;
  - one final aggregate row with no address or transaction identifier.
- [x] Run the focused test and confirm the expected missing-v2 failures.
- [x] Add `candidate_statistics_v2.sql`.
- [x] Add additive `candidate_statistics_v2_sql` and
  `candidate_statistics_v2_sha256` fields to `BigQueryQueryPlan`.
- [x] Run the focused SQL tests and make them green.

## Task 2: Lock Result And Quality Semantics

- [x] Add failing tests for the strict v2 result model.
- [x] Cover exact source baseline `1,557,941,780`, pinned input-only count `3`,
  checksum and cutoff matching, forbidden identifier fields, and exactly one
  result row.
- [x] Cover receipt-funnel and threshold-ladder monotonicity.
- [x] Cover all three variant overlap, histogram, P0/P1, coarse, and excluded
  reconciliation checks.
- [x] Cover blocking size gates and review warnings:
  - strict P0 above 1,000,000 blocks;
  - any coarse union above 5,000,000 blocks;
  - edge frontier above 1,000,000 blocks;
  - strict P0 above 120,000 warns;
  - strict coarse above 2,000,000 warns;
  - incremental supported-receipt P0 above 95,906 warns.
- [x] Implement immutable Pydantic models and fail-closed parsing in
  `candidate_statistics_v2.py`.
- [x] Run the focused model tests and make them green.

## Task 3: Add A Cost-Only V2 Probe

- [x] Add failing fake-backend tests proving the probe calls exactly:
  `table_metadata`, `monthly_successful_query_usage`, and `dry_run`.
- [x] Prove a wrong expected query checksum blocks before network access.
- [x] Prove the existing 650 billion-byte cap, Sandbox budget, and reserve
  checks apply to v2.
- [x] Implement `BigQueryCandidateStatisticsV2Probe` without a query-execution
  dependency or method.
- [x] Use v2-specific status, query kind, and blocking reason codes.
- [x] Run focused probe tests and make them green.

## Task 4: Add The Dry-Run-Only CLI

- [x] Add failing CLI tests for:

```text
cai universe probe bigquery-candidate-statistics-v2 --dry-run \
  --as-of-date 2026-07-24 --cutoff-height 959187

cai universe probe bigquery-candidate-statistics-v2 --live-dry-run \
  --as-of-date 2026-07-24 --cutoff-height 959187 \
  --expected-query-sha256 <digest> \
  --sandbox-budget-bytes 1099511627776 \
  --reserve-bytes 250000000000
```

- [x] Prove offline mode constructs no backend and writes nothing.
- [x] Prove live mode exposes only aggregate cost/checksum fields, reports zero
  provider requests and zero written paths, and performs no query execution.
- [x] Add the parser and handler.
- [x] Do not add `execute`, `execute-once`, output, destination, or
  materialization arguments for v2.
- [x] Run focused CLI tests and make them green.

## Task 5: Documentation And Verification

- [x] Update `docs/btc_identity_operations.md` with v2 commands, safety
  boundary, fixed cutoff, and the distinction between dry-run estimates and
  actual candidate counts.
- [x] Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/universe/test_candidate_statistics_v2.py tests/universe/test_cli.py
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/universe
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider
/Users/barry/.pyenv/versions/3.13.7/bin/python3 -m py_compile \
  src/crypto_address_identity/cli.py \
  src/crypto_address_identity/universe/query_plan.py \
  src/crypto_address_identity/universe/candidate_statistics_v2.py
git diff --check
```

- [x] Review for v1 immutability, no identifier output, no query execution,
  no provider calls, no writes, and no consumer effect.
- [x] Run one free live BigQuery dry run only if credentials are available and
  all invariants pass.
- [x] Record the estimated bytes and current budget status without executing
  the aggregate query.
- [x] Stage only this implementation, run staged preflight, and commit.

## Verification Record

- Fixed v2 query SHA-256:
  `47b0b8977cc1443578bc3daf3f90a2cf5e0e48ae758a4b7a133d3caa7d301e74`.
- Source schema SHA-256:
  `7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7`.
- Free live dry-run bytes: `637,999,682,243`.
- Successful billed jobs before the dry run: `5`.
- Month-to-date billed bytes: `838,768,525,312`.
- Projected month-to-date bytes if executed: `1,476,768,207,555`.
- Projected reserve: `-377,256,579,779`.
- Cost decision: blocked by monthly budget and reserve gates.
- Provider requests/points: `0/0`.
- Written paths: `0`.
- Billed v2 aggregate executions: `0`.
- Final local suite: `296 passed`.

## Acceptance Criteria

- V1 SQL, models, executor, and receipt remain byte-identical.
- A stale lifetime 500 BTC receipt has no v2 P0 or score effect.
- One recent 1,000 BTC receipt cannot become P0 through raw gross flow.
- A recent receipt needs retained, repeated, or sustained independent chain
  evidence for strict receipt-based P0.
- V2 exposes strict, balanced, and retention-only aggregate variants.
- The fixed SQL returns one identifier-free aggregate row.
- Result parsing is fail-closed and checksum/cutoff pinned.
- The v2 CLI has no billed execution or materialization path.
- No address, transaction hash, provider request, runtime state, or consumer
  behavior changes in this implementation.
