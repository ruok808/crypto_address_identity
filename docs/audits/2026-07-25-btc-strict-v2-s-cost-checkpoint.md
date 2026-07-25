# BTC Strict V2-S Free Cost Checkpoint

## Decision

The fixed Strict V2-S candidate materialization query compiled successfully in
one real BigQuery dry run. The query estimate is below the fixed per-query cap,
but the checkpoint is `blocked` by the reviewed monthly processing budget and
reserve gates.

No candidate materialization is authorized. No destination table, address row,
transaction hash, provider request, local artifact, or consumer change was
created.

## Pinned Inputs

- source commit: `42d9db3fac576581a82e20a548e3a184e9f9bdcd`;
- checkpoint time: `2026-07-25T01:42:44Z`;
- cutoff height: `959187`;
- cutoff time: `2026-07-24T23:59:59.999999Z`;
- policy / variant: `btc_importance_v2 / V2-S`;
- query SHA-256:
  `46af66e53382264ce8948720ac40f1c556d44e682847f3bf9ef829b317ae31c6`;
- result schema SHA-256:
  `ae5e08ff63b55f9bce3f5bbd17f858f2a29ec3da85223fd2f3c6675043883683`;
- source schema SHA-256:
  `7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7`;
- future per-query cap: `650,000,000,000` bytes;
- reviewed monthly processing budget: `2,000,000,000,000` bytes;
- required reserve: `250,000,000,000` bytes.

The dual-population receipt contract was `accepted` before network use:

- output-defined addresses: `1,557,941,780`;
- positive-value policy denominator: `1,531,420,608`;
- expected Strict V2-S coarse union: `1,090,411`;
- expected tiers: P0 `21,736`, P1 `2,143`, edge `133,730`,
  coarse-other `932,802`.

## Free Dry-Run Result

| Field | Value |
| --- | ---: |
| status | `blocked` |
| dry-run bytes | 638,112,721,818 |
| bytes below per-query cap | 11,887,278,182 |
| successful billed query jobs visible this UTC month | 6 |
| month-to-date billed bytes | 1,476,768,301,056 |
| projected bytes after hypothetical execution | 2,114,881,022,874 |
| projected reserve under the 2 TB budget | -114,881,022,874 |
| minimum budget required to preserve 250 GB reserve | 2,364,881,022,874 |
| network requests | 3 |
| provider requests / points | 0 / 0 |
| candidate rows returned | 0 |
| written paths | 0 |

Blocking reasons:

- `strict_v2_s_materialization_monthly_budget_exceeded`;
- `strict_v2_s_materialization_monthly_reserve_insufficient`.

There were no schema, query checksum, population contract, source partition,
source freshness, or per-query-cap blocking reasons.

## Boundary Verification

- BigQuery calls were limited to transaction-table metadata, current-month
  successful-job usage, and one dry run with `maximum_bytes_billed=0`.
- The dry run returned compile/cost metadata only; it did not execute the
  address-producing query.
- `candidate_materialization_allowed=false` and
  `execution_authorized=false` remained fixed in the output.
- The checkpoint made no 0xRouter or other provider request.
- No lake, manifest, candidate artifact, resolver snapshot, or consumer state
  changed.
- The unrelated untracked July 23 evidence audit remained outside this work.

## Next Gate

Do not proceed to one-shot materialization under the 2 TB monthly processing
budget. A later Phase 2 review must either establish a separately approved
budget of at least `2,364,881,022,874` bytes for the current month, or wait for
a new UTC billing month and rerun this exact free checkpoint. That review must
pin the same query/schema checksums and still issue a separate explicit billed
execution authorization.
