# BTC First-Wave Candidate Statistics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Implement the checksum-pinned, aggregate-only BTC first-wave candidate census contract without executing the estimated 638 GB statistics query.

**Architecture:** Add one immutable BigQuery SQL resource that computes the complete `btc_importance_v1` chain census and returns exactly one aggregate row. Keep result parsing and quality evaluation independent from a cost-only probe; the only live CLI path added in this phase may read table metadata, summarize current-month query billing through the Jobs API, and submit a BigQuery dry run, but it has no data-query execution method.

**Tech Stack:** Python 3.13, Pydantic v2, Google BigQuery optional SDK, fixed Standard SQL, argparse, pytest, SHA-256.

---

## Scope And Safety Boundary

This plan implements immutable `candidate_statistics.sql`, its checksum, strict
aggregate result and quality models, current-month Jobs API cost accounting,
and an aggregate-only CLI. It does not call `query_one`, Arrow streaming, or
another execution method for the candidate census. It does not write addresses,
lake files, SQLite rows, provider queues, evidence, or consumer state. The
estimated `637,999,682,243` byte query remains explicitly unauthorized.

## File Structure

Create:

- `src/crypto_address_identity/universe/sql/bigquery/candidate_statistics.sql`
- `src/crypto_address_identity/universe/candidate_statistics.py`
- `tests/universe/test_candidate_statistics.py`

Modify:

- `src/crypto_address_identity/universe/query_plan.py`
- `src/crypto_address_identity/universe/bigquery.py`
- `src/crypto_address_identity/cli.py`
- `tests/universe/test_bigquery_probe.py`
- `tests/universe/test_cli.py`
- `docs/btc_identity_operations.md`

Do not modify or stage
`docs/audits/2026-07-23-btc-third-strong-evidence-search.md`.

## Task 1: Pin The SQL Contract

**Files:**

- Create: `src/crypto_address_identity/universe/sql/bigquery/candidate_statistics.sql`
- Modify: `src/crypto_address_identity/universe/query_plan.py`
- Create: `tests/universe/test_candidate_statistics.py`

- [x] **Step 1: Write failing query-plan tests**

Require deterministic `candidate_statistics_sha256`, one configured transaction
table reference, one `ARRAY_CONCAT` input/output stream, `BIGNUMERIC` casts,
the historical duplicate-tx-safe grouping key, all P0/P1 thresholds, overlap
and score histograms, and no final address/transaction identifiers.

```python
def test_candidate_statistics_sql_is_fixed_and_aggregate_only() -> None:
    plan = BigQueryQueryPlan.load("bigquery-public-data.crypto_bitcoin")
    repeated = BigQueryQueryPlan.load("bigquery-public-data.crypto_bitcoin")
    assert plan.candidate_statistics_sha256 == repeated.candidate_statistics_sha256
    assert plan.candidate_statistics_sql.count(
        "bigquery-public-data.crypto_bitcoin.transactions"
    ) == 1
    assert "ARRAY_CONCAT" in plan.candidate_statistics_sql
    assert "AS BIGNUMERIC" in plan.candidate_statistics_sql
    assert "p0_overlap_distribution" in plan.candidate_statistics_sql
    assert "score_histogram" in plan.candidate_statistics_sql
```

- [x] **Step 2: Run the focused test and verify red**

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/universe/test_candidate_statistics.py
```

Expected: missing SQL/query-plan fields.

- [x] **Step 3: Implement fixed SQL and query-plan hashing**

The SQL must filter cutoff height/time/month before unnesting; combine inputs
and outputs once; count null/cast failures; aggregate by block hash,
transaction hash, address, and row kind; calculate the exact existing policy;
and return one row of counters plus nested mask/score histograms. Add
`candidate_statistics_sql` and `candidate_statistics_sha256` to
`BigQueryQueryPlan`.

- [x] **Step 4: Run focused tests and verify green**

Run the command from Step 2. Expected: SQL contract tests pass.

## Task 2: Add Strict Result And Quality Models

**Files:**

- Create: `src/crypto_address_identity/universe/candidate_statistics.py`
- Modify: `tests/universe/test_candidate_statistics.py`

- [x] **Step 1: Write failing model tests**

Cover one valid fixture, forbidden address/hash result fields, zero/multiple
rows, null/cast failures, negative balance, input-only data, P0/P1 overlap,
hash/cutoff/baseline mismatch, histogram reconciliation, excluded/union
reconciliation, and all warning thresholds.

The public parser is named `parse_candidate_statistics_rows`. It accepts a
sequence of mapping rows plus expected query/schema checksums, source baseline,
cutoff height/time, current time, and maximum source age, and returns a tuple of
`CandidateStatisticsResult | None` and `CandidateStatisticsQualityReport`.

- [x] **Step 2: Run tests and verify red**

Run the candidate-statistics test file. Expected: missing module/contracts.

- [x] **Step 3: Implement immutable models and fail-closed gates**

Use strict extra-field rejection, lower-case SHA-256 validation, UTC timestamp
validation, unique histogram keys, non-negative counters, sorted unique reason
codes, and explicit `p0_p1_overlap_count`. Never include raw rows in errors.

- [x] **Step 4: Run tests and verify green**

Run the candidate-statistics test file. Expected: all result/gate tests pass.

## Task 3: Add Monthly Jobs Accounting And Cost Probe

**Files:**

- Modify: `src/crypto_address_identity/universe/bigquery.py`
- Modify: `src/crypto_address_identity/universe/candidate_statistics.py`
- Modify: `tests/universe/test_bigquery_probe.py`
- Modify: `tests/universe/test_candidate_statistics.py`

- [x] **Step 1: Write failing Jobs API and cost tests**

Require successful query-job bytes only. Require the candidate probe to call
exactly metadata, monthly usage, and dry run, never query execution. Test
schema/partition/freshness, query hash, `650_000_000_000` query cap, Sandbox
budget, required reserve, and safe boundary-failure classifications.

- [x] **Step 2: Run tests and verify red**

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/universe/test_bigquery_probe.py \
  tests/universe/test_candidate_statistics.py
```

- [x] **Step 3: Implement accounting and probe**

Add `MonthlyQueryUsage(successful_query_jobs, total_bytes_billed)` and
`monthly_successful_query_usage(month_start, month_end)`. The Google adapter
uses `Client.list_jobs` with `all_users=True` and explicit UTC bounds, excluding failed, incomplete,
non-query, and dry-run jobs. The probe computes:

```text
projected_month_to_date = month_to_date_billed + dry_run_bytes
projected_reserve = sandbox_budget - projected_month_to_date
```

It returns `within_budget` only when query bytes are at most 650 billion and
the projected reserve satisfies the operator-supplied minimum.

- [x] **Step 4: Run tests and verify green**

Run the command from Step 2. Expected: fake backends record no execution calls.

## Task 4: Add Aggregate-Only CLI

**Files:**

- Modify: `src/crypto_address_identity/cli.py`
- Modify: `tests/universe/test_cli.py`

- [x] **Step 1: Write failing CLI tests**

Cover:

```text
cai universe probe bigquery-candidate-statistics --dry-run \
  --as-of-date 2026-07-24 --cutoff-height 959187

cai universe probe bigquery-candidate-statistics --execute-readonly \
  --as-of-date 2026-07-24 --cutoff-height 959187 \
  --expected-query-sha256 <digest> \
  --sandbox-budget-bytes 1099511627776 \
  --reserve-bytes 250000000000
```

Offline mode must not construct a backend. Live mode must return cost hashes,
usage, projection, quality, zero provider requests/points, and no written paths.

- [x] **Step 2: Run CLI tests and verify red**

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/universe/test_cli.py
```

- [x] **Step 3: Implement parser and handler**

Reject invalid budgets/reserve/cutoff and a missing expected query hash in live
mode. The handler exposes the cost contract and has no call to a data-query
execution method.

- [x] **Step 4: Run CLI tests and verify green**

Run the command from Step 2. Expected: all CLI tests pass.

## Task 5: Document And Verify

**Files:**

- Modify: `docs/btc_identity_operations.md`

- [x] **Step 1: Document preview and live dry-run boundaries**

Explain that offline preview emits the checksum and live mode performs only
metadata/usage/dry-run. State that neither output is the candidate census and
the 638 GB execution remains unauthorized.

- [x] **Step 2: Run targeted and full tests**

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/universe/test_candidate_statistics.py \
  tests/universe/test_bigquery_probe.py tests/universe/test_cli.py
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/universe
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider
```

- [x] **Step 3: Compile and inspect**

```bash
/Users/barry/.pyenv/versions/3.13.7/bin/python3 -m py_compile \
  src/crypto_address_identity/cli.py \
  src/crypto_address_identity/universe/bigquery.py \
  src/crypto_address_identity/universe/candidate_statistics.py \
  src/crypto_address_identity/universe/query_plan.py
git diff --check
```

- [x] **Step 4: Run one free live BigQuery dry run only**

After fresh memory discovery/preflight, use the offline checksum in one
`--execute-readonly` invocation. Verify estimate at most 650 billion bytes,
projected reserve at least 250 billion bytes, unchanged billed Jobs API totals,
zero provider calls/points, and zero written paths. Do not run query execution.

- [x] **Step 5: Review, explicitly stage, and commit**

Run staged preflight and inspect the cached diff. Keep the unrelated audit file
unstaged. Create one coherent commit; do not push unless separately requested.

## Acceptance Criteria

- The fixed SQL references the Bitcoin transaction table once and outputs one
  aggregate row with no address or transaction identifier.
- SQL policy logic exactly matches existing `btc_importance_v1` chain rules.
- Result parsing blocks malformed shape, accounting drift, unsafe counters,
  and hash/cutoff/baseline mismatches.
- Live CLI cannot execute the 638 GB candidate census.
- Jobs API accounting preserves the required Sandbox reserve.
- Provider calls, provider points, written paths, identity state, and consumer
  behavior remain unchanged.
