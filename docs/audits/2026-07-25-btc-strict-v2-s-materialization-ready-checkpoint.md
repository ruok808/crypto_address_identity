# BTC Strict V2-S Materialization-Ready Cost Checkpoint

## Decision

The frozen Strict V2-S address materialization query compiled successfully in
one free BigQuery dry run from exact `origin/main`. Its estimated processing is
below the fixed per-query cap, and the reviewed 2.4 TB monthly processing
budget preserves more than the required 250 GB reserve.

This checkpoint does not authorize billed execution. It created no destination
table, address row, candidate artifact, provider request, or consumer change.

## Pinned Inputs

- source commit:
  `a35d8a9cdc8d72297f61b5e96b79840d0a4d5801`;
- cutoff height: `959187`;
- cutoff time: `2026-07-24T23:59:59.999999Z`;
- policy / variant: `btc_importance_v2 / V2-S`;
- query SHA-256:
  `5cb4990e01b4983910d0d813b67e148b985111108e6a26a251fadf95b18506d3`;
- result schema SHA-256:
  `ae5e08ff63b55f9bce3f5bbd17f858f2a29ec3da85223fd2f3c6675043883683`;
- source schema SHA-256:
  `7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7`;
- per-query cap: `650,000,000,000` bytes;
- reviewed monthly processing budget: `2,400,000,000,000` bytes;
- required reserve: `250,000,000,000` bytes.

## Free Dry-Run Result

| Field | Value |
| --- | ---: |
| status | `checkpoint_passed` |
| dry-run bytes | 638,126,831,178 |
| bytes below per-query cap | 11,873,168,822 |
| successful billed query jobs visible this UTC month | 6 |
| month-to-date billed bytes | 1,476,768,301,056 |
| projected bytes after hypothetical execution | 2,114,895,132,234 |
| projected reserve under the 2.4 TB budget | 285,104,867,766 |
| network requests | 3 |
| provider requests / points | 0 / 0 |
| candidate rows returned | 0 |
| written paths | 0 |

The dual-population contract was accepted before network use:

- output-defined addresses: `1,557,941,780`;
- positive-value policy denominator: `1,531,420,608`;
- expected coarse union: `1,090,411`;
- expected tiers: P0 `21,736`, P1 `2,143`, edge `133,730`,
  coarse-other `932,802`.

There were no query, schema, population, source freshness, cost-cap, monthly
budget, or reserve blocking reasons.

## Boundary Verification

- The BigQuery operations were limited to source metadata, current-month
  successful-query usage, and one dry run.
- `candidate_materialization_allowed=false` and
  `execution_authorized=false` remained fixed.
- No destination table, billed materialization job, address row, transaction
  identifier, provider request, local artifact, resolver state, or consumer
  state was created.
- The prior blocked checkpoint remains immutable historical evidence; its
  earlier estimate is not reused as the execution authorization value.

## Next Gate

A separate explicit authorization must repeat the exact estimate, query and
schema checksums, destination table, deterministic job id, monthly usage,
2.4 TB budget, 250 GB reserve, and `650,000,000,000` byte cap before the
one-shot billed query may run.
