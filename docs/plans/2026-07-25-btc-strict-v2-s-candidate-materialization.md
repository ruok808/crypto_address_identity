# BTC Strict V2-S Candidate Materialization Implementation Plan

## Goal

Implement the candidate-materialization path designed in
`docs/designs/2026-07-25-btc-strict-v2-s-candidate-materialization-design.md`
without conflating the free cost checkpoint with authorization to execute the
approximately 638 GB source scan.

Strict V2-S is frozen for the BTC bootstrap stage and does not require a V3
before materialization. The implementation must make the next separately
authorized source scan produce durable address rows, not only aggregate
counts. Long-term policy changes are downstream of address-level quality
review and cannot be introduced as a prerequisite to this delivery.

The implementation milestone ends after reviewed code and a fresh exact-main
free BigQuery dry run report the fixed SQL/schema checksums, estimated bytes,
current-month usage, projected usage, and reserve. It must not create a
destination table until the user separately authorizes the billed execution.

## Architecture

- Keep the accepted dual-population contract as the first local gate.
- Add an isolated fixed SQL resource for Strict V2-S candidate rows.
- Add a fixed candidate result-schema contract and checksum.
- Add a cost-only probe that uses the existing BigQuery backend for:
  table metadata, current-month successful query usage, and one free dry run.
- Add offline and live-dry-run CLI modes.
- Add an exact-contract one-shot executor and same-job reconciler, but keep
  billed execution disabled until separate approval.
- Add bounded extraction and immutable local publication so the authorized job
  delivers actual address rows.
- Keep provider enrichment and consumer integration outside this milestone.

## Fixed Contracts

- cutoff height: `959187`;
- cutoff time: `2026-07-24T23:59:59.999999Z`;
- policy: `btc_importance_v2`;
- variant: `V2-S`;
- output-defined addresses: `1,557,941,780`;
- positive-value denominator: `1,531,420,608`;
- expected coarse union: `1,090,411`;
- expected tiers:
  - P0 `21,736`;
  - P1 `2,143`;
  - edge `133,730`;
  - coarse-other `932,802`;
- future execution cap: `650,000,000,000` bytes;
- automatic query retries: `0`;
- provider requests/points: `0/0`.

## Phase 1: Free Cost Checkpoint

- [x] Run memory discovery and the repository preflight.
- [x] Create a focused `codex/` branch and preserve unrelated dirty work.
- [x] Save this implementation plan.
- [x] Add failing tests for deterministic SQL loading, source identifier
  replacement, query checksum stability, result schema checksum stability,
  exact tier predicates, identifier-free result fields, and no destination
  statement.
- [x] Add failing tests for the cost probe:
  - offline preview makes zero network calls and reads no receipts;
  - live mode requires the accepted dual-population receipts before network;
  - wrong query or result-schema checksum blocks before network;
  - metadata/schema drift blocks before the BigQuery dry run;
  - one live probe performs exactly metadata, monthly usage, and dry run;
  - estimated bytes over `650,000,000,000` block execution eligibility;
  - budget or reserve failure is machine-readable;
  - provider calls and writes stay zero.
- [x] Add CLI tests for:
  - `cai universe probe bigquery-strict-v2-s-materialization --dry-run`;
  - `cai universe probe bigquery-strict-v2-s-materialization --live-dry-run`;
  - structured, secret-free output and no materialization side effects.
- [x] Add
  `src/crypto_address_identity/universe/sql/bigquery/candidate_materialization_v2_s.sql`.
- [x] Add an isolated materialization query-plan loader, fixed Arrow/BigQuery
  result schema, and `btc_strict_v2_s_candidate_schema_v1` checksum.
- [x] Add the cost-only probe and CLI integration.
- [x] Update `docs/btc_identity_operations.md` with checkpoint commands and
  explicit non-authorization semantics.
- [x] Run focused, universe, and full tests; compile checks; `git diff --check`;
  sensitive-value scan; staged preflight; and final code review.
- [x] Merge and push the reviewed implementation to exact `origin/main`.
- [x] From exact main, run one free live dry run with:
  - fixed cutoff;
  - exact query and result-schema checksums;
  - explicit monthly byte budget and reserve;
  - zero destination/write/provider behavior.
- [x] Save an aggregate-only audit with the exact checkpoint result. Do not
  store credentials, raw upstream payloads, address rows, or private paths.
- [x] Stop. Do not proceed to Phase 2 without separate user approval.

## Phase 2: One-Shot Cloud Materialization

Implementation is approved; billed execution remains separately gated.

- [x] Review the refreshed free dry-run estimate and available billing reserve.
- [ ] Obtain explicit approval for the exact estimate, SQL checksum, schema
  checksum, job id, destination table id, and `650,000,000,000` byte cap.
- [x] Add a no-retry one-shot executor with an exclusive mode-`0600` receipt.
- [x] Use a deterministic private destination table, `WRITE_EMPTY`, and a
  seven-day expiration.
- [x] Refuse overwrite, automatic query retry, fallback query, or a second
  materialization variant.
- [x] Add existing-job reconciliation that uses `get_job` and destination-table
  metadata only; it must never resubmit the query.
- [ ] Execute once only after all reviewed values still match.

## Phase 3: Extraction And Publication

Implementation is approved; publication waits for the authorized completed
destination job.

- [x] Stream the completed destination table through the BigQuery Storage API
  in bounded Arrow batches.
- [x] Write deterministic Parquet partitions by tier and 64-way address bucket.
- [x] Recompute every tier, score, P0 mask, support mask, and row checksum
  locally.
- [x] Enforce exact total/tier counts, unique valid mainnet addresses, fixed
  schema, zero unapproved identifiers, and file checksum reconciliation.
- [x] Publish by atomic directory rename only after every gate passes.
- [x] Produce an immutable manifest and execution receipt; do not create a
  mutable latest pointer in v1.
- [x] Keep provider enrichment and any `quant_crypto` consumer effect outside
  this phase.

## Verification Commands

```bash
PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider \
  tests/universe/test_candidate_materialization_v2_s.py \
  tests/universe/test_candidate_materialization_execution_v2_s.py \
  tests/universe/test_candidate_materialization_cli_v2_s.py \
  tests/universe/test_candidate_publication_v2_s.py \
  tests/universe/test_cli.py

PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider \
  tests/universe

PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider

python -m py_compile \
  src/crypto_address_identity/cli.py \
  src/crypto_address_identity/universe/candidate_materialization_v2_s.py \
  src/crypto_address_identity/universe/candidate_materialization_execution_v2_s.py \
  src/crypto_address_identity/universe/candidate_publication_v2_s.py

git diff --check
```

## Acceptance Criteria For The Current Milestone

- SQL and result schema are deterministic and checksum-pinned.
- The SQL compiles in a real free BigQuery dry run.
- The checkpoint reports exact estimated bytes and current/projected monthly
  usage without submitting a billed query.
- The dual-population receipt contract is accepted before network use.
- The result contains aggregate cost/contract data only.
- `network_requests=3`, `provider_requests=0`, `provider_points=0`, and
  `written_paths=[]`.
- No destination table, address row, transaction hash, candidate artifact, or
  consumer behavior is created before separate billed-execution approval.
- The one-shot executor can write one expiring private destination table and
  cannot automatically retry or submit a second query.
- A completed destination table can be extracted repeatedly without another
  source scan.
- Successful publication contains the actual `1,090,411` unique valid address
  rows in exact tier counts, with locally recomputed policy fields and hashes.
- No V3 is required before the Strict V2-S bootstrap artifact is delivered.
- The final decision is either `checkpoint_passed` or a machine-readable
  fail-closed status. Neither status authorizes the billed execution.
