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
