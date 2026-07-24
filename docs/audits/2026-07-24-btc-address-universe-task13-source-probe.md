# BTC Address Universe Task 13 Source Probe

Date: 2026-07-24 UTC

## Scope

This audit covers the provider-free source-probe boundary only. It did not run
an identity-provider canary, spend 0xRouter points, materialize a chain
campaign, write universe data, or modify the coverage-sync LaunchAgent.

## Environment Gates

- Query project: `cai-btc-universe-20260724`
- Project lifecycle: `ACTIVE`
- Billing enabled: `false`
- Billing account present: `false`
- BigQuery API: `ENABLED`
- BigQuery checkpoint cap: `8,589,934,592` bytes
- Local free disk before any chain read: approximately `67 GiB`
- Coverage-sync state: loaded but not running; run count remained zero

## BigQuery Contract

The live public dataset contract differs from the original flattened-view
assumption:

- `transactions` is a partitioned table using `block_timestamp_month`.
- `blocks` is a partitioned table using `timestamp_month`.
- Nested `transactions.inputs` and `transactions.outputs` expose address,
  script, value, and spent-output fields.
- Top-level `inputs` and `outputs` are unpartitioned compatibility views and
  are not accepted as source partition authorities.

The source contract and SQL were corrected to use `transactions` and `blocks`.
The address query references the partitioned transaction table once and uses
`(block_hash, transaction_hash)` as its same-transaction key.

## Probe Results

- BigQuery status: `accepted`
- Schema SHA-256:
  `839e5f6e43ef41a32e1975378f498da6e87f564ec77b1fa45365acd3e6103c47`
- Address query SHA-256:
  `5a59bf13649b3f5f4e100b6d58dc788dbd8e571e03c32324fe8de895772b8f7c`
- Checkpoint query SHA-256:
  `3adfdf1e364b643fa5672f401133a4ef3accce7e21487d098a99b8d17aba0237`
- Full-history dry-run bytes: `1,414,991,392,737`
- Seven-day checkpoint dry-run bytes: `1,761,251,972`
- Checkpoint estimate below 8 GiB cap: `true`
- Latest height: `959,193`
- Latest time: `2026-07-22T23:51:20Z`
- Finalized height at six confirmations: `959,187`
- Provider requests: `0`
- Provider points: `0`
- Runtime written paths: `[]`

The full-history estimate is approximately `1.415 TB` decimal
(`1.287 TiB`). It is not approved for execution because it exceeds the
reviewed Sandbox/account budget and the local machine does not have sufficient
free space for a safe full materialization.

## Independent Source Result

- Bitcoin Core status: `blocked`
- Blocking reason: `bitcoin_rpc_unavailable`
- Cutoff reconciliation status: `partial`

The BigQuery checkpoint therefore cannot establish an independently reconciled
canonical full-history cutoff. BigQuery exposes the required script fields,
but campaign-level script completeness is not claimed without the independent
source check.

## Decision

- Full BigQuery chain read approved: `false`
- Immutable universe campaign created: `false`
- Real address count available: `false`
- Candidate statistics run: `false`
- Canary approved: `false`
- Provider calls executed: `0`
- Provider points spent: `0`

Task 13 stops at its fail-closed source and cost gates. The next approved work
must provide an unpruned Bitcoin Core-compatible source for cutoff/sample
reconciliation and a storage/cost plan that does not assume the full
`1,414,991,392,737`-byte query is free.

## Aggregate-Only Address Scale Extension

The follow-up implementation adds a separate exact-address-count cost probe.
It scans only `transactions.outputs.addresses`, keeps the source table
partition and cutoff predicates, and excludes input, value, and script
columns. Its live mode remains cost-only: table metadata plus a BigQuery dry
run. It never executes `COUNT(DISTINCT normalized_address)` and therefore
does not produce an address count.

The operator-supplied Sandbox comparison budget is an explicit assertion
boundary, not an execution cap or proof of remaining account allowance.
Bitcoin Core reconciliation and durable full-universe storage remain separate
tasks.

Free live dry-run result:

- Status: `within_budget`
- Estimated bytes: `195,483,438,068`
- Decimal size: approximately `195.48 GB`
- Binary size: approximately `182.06 GiB`
- Reviewed comparison budget: `1,099,511,627,776` bytes (`1 TiB`)
- Estimated share of comparison budget: approximately `17.78%`
- Exact-distinct query: `true`
- Query SHA-256:
  `3fd8371afe3fc971aa0d1995e8f2957a2aaaa48fed92847b31a9b628b31a146b`
- Transaction schema SHA-256:
  `7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7`
- Blocking reasons: `[]`
- Provider requests and points: `0`
- Written paths: `[]`
- Aggregate query executed: `false`
- Address count produced: `false`

This estimate is substantially below the supplied Sandbox comparison budget,
so a separately approved one-time aggregate execution is technically
budget-feasible. It is not authorized by this audit. Before any execution, the
operator must still verify the account's current month-to-date BigQuery usage
and reserve enough free allowance for other workloads.

## Approved One-Time Aggregate Execution

After a separate explicit approval, the checksum-pinned aggregate was
submitted once with a deterministic idempotency key and a
`200,000,000,000`-byte hard billing cap.

- Execution state: `DONE`
- Error count: `0`
- Created: `2026-07-24T11:50:02.558000Z`
- Ended: `2026-07-24T11:50:15.072000Z`
- Cache hit: `false`
- Exact unique standard addresses: `1,557,951,354`
- Query SHA-256:
  `3fd8371afe3fc971aa0d1995e8f2957a2aaaa48fed92847b31a9b628b31a146b`
- Total bytes processed: `195,483,438,068`
- Total bytes billed: `195,483,926,528`
- Destination table created: `false`
- Local written paths: `[]`
- Provider requests and points: `0`

Post-execution month-to-date Jobs API accounting:

- Successful billed query jobs: `2`
- Total bytes billed: `197,245,534,208`
- Sandbox comparison limit: `1,099,511,627,776` bytes
- Remaining comparison allowance: `902,266,093,568` bytes
- Usage: approximately `17.94%`

The address count is not the count of active wallets, owners, entities, or
currently funded addresses. It is the exact number of distinct decoded
single-address outputs in the accepted public transaction-table contract
through the fixed UTC cutoff. Empty, multi-address, and undecodable scripts
remain outside this aggregate.
