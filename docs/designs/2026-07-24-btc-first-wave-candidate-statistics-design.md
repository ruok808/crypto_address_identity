# BTC First-Wave Candidate Statistics Design

Date: 2026-07-24 UTC

## Status

Proposed for review. This document defines an aggregate-only candidate census
over the BTC address universe. It does not authorize the census query, export
addresses, create provider requests, start a canary, or change a consumer.

## Decision Summary

The accepted public source contains exactly `1,557,951,354` distinct decoded
single-address outputs through the fixed `2026-07-24` UTC cutoff. Enriching
that population address by address is neither economically nor operationally
valid.

The first chain-selection step will therefore be one checksum-pinned BigQuery
query that:

1. scans the partitioned `transactions` table once;
2. reads only transaction identity, time, address, and value columns;
3. aggregates input/output value by transaction and address;
4. computes only the chain features required by `btc_importance_v1`;
5. applies all P0/P1 thresholds inside BigQuery; and
6. returns aggregate counts and overlaps, never address values.

The final design dry-run estimate is `637,999,682,243` bytes. It is preferred
over separate historical, balance, and recent-flow scans because it covers
the complete chain policy in one pass and avoids paying repeatedly for the
same full-history columns.

## Goals

- Measure the exact scale of every existing chain-derived P0 and P1 bucket.
- Measure overlap so one address is counted once in the candidate union.
- Identify near-threshold addresses that could become P1 after graph evidence.
- Estimate whether the current policy is operationally feasible before
  materializing any candidate address.
- Preserve the existing `btc_importance_v1` thresholds for this census.
- Keep provider requests, provider points, identity evidence, and consumer
  behavior unchanged.

## Non-Goals

- Do not enrich any address through Arkham, 0xRouter, or another provider.
- Do not create the full `AddressFeatureRow` lake.
- Do not read `script_hex`, output indexes, spent-output references, or raw
  transaction payloads.
- Do not infer ownership, entity identity, or wallet role.
- Do not compute graph edges or common-input ownership clusters.
- Do not create a canary or provider queue.
- Do not replace the separately required Bitcoin Core verification and
  storage design.

## Source Boundary

The query reads only:

```text
bigquery-public-data.crypto_bitcoin.transactions
```

Required top-level fields:

- `block_number`
- `block_hash`
- `hash`
- `block_timestamp`
- `block_timestamp_month`
- `inputs`
- `outputs`

Required nested fields:

- `inputs.addresses`
- `inputs.value`
- `outputs.addresses`
- `outputs.value`

The source table must remain time-partitioned on
`block_timestamp_month`. The query rejects a schema, partition, or freshness
drift before execution.

Only rows where `ARRAY_LENGTH(addresses) = 1` enter address economics. Empty,
multi-address, and undecodable scripts remain source-accounting exclusions.
The resulting population is a source-decoded address population, not an
ownership or active-wallet population.

## Query Architecture

### Stage 1: Partition And Column Pushdown

Apply all immutable source bounds before unnesting:

```sql
tx.block_number <= @cutoff_height
AND tx.block_timestamp <= @cutoff_time
AND tx.block_timestamp_month
    <= DATE_TRUNC(DATE(@cutoff_time), MONTH)
```

Project no columns beyond the source boundary above. This removes script,
index, and spent-reference bytes from the scan.

### Stage 2: One Input/Output Stream

Use one `ARRAY_CONCAT` over nested inputs and outputs so the transaction table
appears once in SQL. Each row carries:

- `block_hash`
- `transaction_hash`
- `block_timestamp`
- `row_kind`
- `addresses`
- `value_sats`

Apply `SAFE_CAST(value AS BIGNUMERIC)` before aggregation and retain a cast
failure counter. High-turnover addresses can accumulate lifetime flow beyond
a conservative signed-64-bit intermediate even though Bitcoin's point-in-time
supply cannot. Any cast failure blocks interpretation.

### Stage 3: Same-Transaction Address Aggregation

Aggregate by:

```text
(block_hash, transaction_hash, normalized_address, row_kind)
```

This key preserves correctness for historical duplicate transaction IDs.
It yields exact same-transaction received value without joining output rows
back to the source table.

### Stage 4: Address Economics

Aggregate the same-transaction rows once by address:

- `lifetime_received_sats`
- `lifetime_spent_sats`
- `current_utxo_sats`
- `max_same_tx_received_sats`
- `gross_flow_90d_sats`
- `last_seen_time`
- `has_output`
- `has_input`

The query deliberately omits 30-day and 365-day flow, script features, and
counterparty graph features because they do not change the current chain P0
or base P1 decision. `source_standard_address_count` counts only distinct
addresses with `has_output=true`; input-only addresses are a separate quality
counter.

### Stage 5: Threshold Pushdown

Apply the existing integer-satoshi policy in SQL.

Mandatory chain P0:

| Reason | Threshold |
| --- | ---: |
| `utxo_ge_100_btc` | `current_utxo_sats >= 100 BTC` |
| `same_tx_receive_ge_500_btc` | `max_same_tx_received_sats >= 500 BTC` |
| `gross_90d_ge_1000_btc` | `gross_flow_90d_sats >= 1,000 BTC` |
| `lifetime_ge_10000_active_365d` | lifetime received `>=10,000 BTC` and last seen within 365 days |

P1 score buckets:

| Family | Buckets |
| --- | --- |
| Current balance | `1/10/100/1,000 BTC -> 5/12/20/25` |
| Maximum same-transaction receipt | `100/500/1,000/5,000 BTC -> 10/18/20/25` |
| 90-day gross flow | `10/100/1,000/10,000 BTC -> 3/8/15/20` |
| Recency | `<=365/90/30 days -> 3/7/10` |

Only the highest bucket in each family contributes. Base chain P1 is:

```text
not chain P0 and chain_importance_score >= 25
```

The census also counts non-P0 addresses scoring `15..24`. These are the exact
`direct_large_selected_edge +10` upgrade frontier. The graph edge is not
invented in this pass.

### Stage 6: Aggregate-Only Result

The final result contains one row of counters. It must not contain an address,
transaction hash, block hash, provider identifier, or raw source value.

## Required Counters

### Source And Quality

- `source_standard_address_count`
- `source_input_only_address_count`
- `negative_current_utxo_count`
- `null_value_count`
- `value_cast_failure_count`
- `max_observed_activity_time`
- `source_cutoff_height`
- `source_cutoff_time`
- `query_sha256`
- `schema_sha256`

`source_standard_address_count` must reconcile to the accepted
`1,557,951,354` output-address baseline. Input-only addresses are reported
separately and block policy interpretation until explained.

### Threshold Ladders

- Current UTXO: `>=1`, `>=10`, `>=100`, `>=1,000 BTC`
- Maximum same-transaction receipt:
  `>=100`, `>=500`, `>=1,000`, `>=5,000 BTC`
- 90-day gross flow:
  `>=10`, `>=100`, `>=1,000`, `>=10,000 BTC`
- Recency: `<=30`, `<=90`, `<=365 days`
- Lifetime received `>=10,000 BTC` and active within 365 days

### Policy Counts

- count for each chain P0 reason;
- exact chain P0 union;
- exact chain P0 overlap bitmask distribution across all four chain reasons;
- chain-only P1 count excluding P0;
- chain importance score histogram;
- `15..24` edge-upgrade frontier;
- addresses with at least one positive economic score component;
- coarse candidate union;
- excluded source-address count.

### Cohort Counts

- `current_capital`
- `historical_large_receipt`
- `high_turnover`
- `dormant_holder`
- chain P0
- chain P1
- edge-upgrade frontier

Calibration and provider-derived cohorts are counted separately from a
checksum-pinned local snapshot. They must not be folded into BigQuery source
facts:

- official or signed evidence;
- existing provider conflict;
- existing system watchlist;
- stored provider entity prediction.

## Coarse Candidate Union

The aggregate query reports, but does not materialize, the deduplicated union
of addresses satisfying at least one of:

```text
chain P0
chain_importance_score >= 15
current_utxo_sats >= 1 BTC
max_same_tx_received_sats >= 100 BTC
gross_flow_90d_sats >= 10 BTC
```

The `>=15` frontier preserves addresses that could cross the P1 threshold
with one verified high-value graph edge. The economic floors preserve every
address with a positive chain score component. This is an intentionally broad
reservoir estimate, not a provider queue.

## Exact Deduplication

- One source IO row belongs to one transaction/address/row-kind aggregate.
- One address occupies one policy slot even when it matches every P0 reason.
- P0 and P1 are mutually exclusive in final policy counts.
- Threshold overlap is represented by explicit intersection counters, not by
  summing reason counts.
- Calibration-only addresses absent from chain rows remain separate and never
  receive invented economics.
- No uppercase/lowercase transformation is applied to Base58 addresses.
  Candidate materialization must run the local Bitcoin normalizer before any
  provider request.

## Quality Gates

Blocking:

- transaction schema or partition mismatch;
- stale source metadata beyond the configured source-age limit;
- query hash mismatch;
- cutoff after the accepted source checkpoint;
- dry-run estimate above `650,000,000,000` bytes;
- projected month-to-date usage leaving less than
  `250,000,000,000` bytes of reserve;
- any null or failed value cast in an eligible row;
- any negative current UTXO balance;
- P0/P1 overlap after exclusive classification;
- source baseline reconciliation failure;
- aggregate result containing an address or transaction identifier;
- more or fewer than one aggregate result row.

Warnings:

- coarse candidate union above `5,000,000`;
- chain P0 union above `120,000`;
- edge-upgrade frontier above `1,000,000`;
- any one non-mandatory cohort above 40% of the candidate union;
- source metadata age above 24 hours but within the blocking limit.

Warnings do not silently change thresholds. They trigger a reviewed policy
version or a bounded materialization design.

## Cost Evidence

All values below are free BigQuery dry-run estimates at the accepted cutoff.
None of the candidate-statistics queries were executed.

| Query shape | Dry-run bytes | Coverage |
| --- | ---: | --- |
| Exact standard-address count | `195,483,438,068` | Output addresses only; already executed separately |
| Historical receipt census | `349,488,807,306` | Lifetime and same-transaction receipts; no balance |
| Recent 90-day flow | `19,327,744,963` | Recent input/output gross flow only |
| Incomplete combined prototype | `545,432,371,517` | All policy economics but unsafe historical tx key |
| Final candidate statistics | `637,999,682,243` | Complete chain P0/base-P1 census |
| Original full feature query | `1,414,991,392,737` | Full lake features and scripts |

The final census reduces scanned bytes by approximately `54.91%` relative to
the original full feature query.

Current month-to-date billed bytes after the address-count execution are
`197,245,534,208`. If the final census were separately approved and billed at
its dry-run estimate:

- projected month-to-date bytes: `835,245,216,451`;
- projected usage: approximately `75.97%` of `1 TiB`;
- projected reserve: `264,266,411,325` bytes, about `246.12 GiB`.

These are planning values, not an execution authorization.

## Why Not Split The Census

Executing the historical-receipt and recent-flow queries separately would use
about `368.82 GB` while still omitting current balance. A later balance scan
would reread the full input/output address and value columns. The final query
instead pays once for the complete chain policy.

The cheap 90-day query remains useful after bootstrap as a future incremental
activity audit. It is not a substitute for the first complete census.

## Decision Bands After Census

The census determines the next design; it does not automatically start work.

| Observation | Decision |
| --- | --- |
| Chain P0 `<=120,000` and coarse union `<=5,000,000` | Design bounded candidate materialization under existing policy |
| Chain P0 `120,001..1,000,000` | Keep P0 semantics but design segmented priority tiers before provider work |
| Chain P0 `>1,000,000` | Stop and review `btc_importance_v2`; existing P0 is operationally too broad |
| Edge frontier `>1,000,000` | Defer graph expansion and rank a bounded structural sample |
| Any blocking quality gate | No candidate materialization or canary |

The first provider segment remains bounded by the established rate and point
formula. No aggregate count implies authorization to enrich every matching
address.

## Proposed Implementation Boundary

The next implementation task should add:

- immutable `candidate_statistics.sql`;
- query-plan hash support;
- a cost-only probe;
- an aggregate result model;
- CLI:
  `cai universe candidates estimate --dry-run|--execute-readonly`;
- schema, cutoff, budget, and result-shape quality gates;
- fixture tests for every threshold and overlap;
- one deterministic, explicitly approved execution path.

The initial implementation must not add:

- address output;
- Parquet or SQLite writes;
- candidate queue writes;
- provider client construction;
- recurring scheduling;
- consumer integration.

## Acceptance Criteria

- The query references the partitioned transaction table once.
- The final query dry-run remains at or below `650,000,000,000` bytes.
- All `btc_importance_v1` chain P0 and base P1 conditions are represented
  exactly in integer satoshis.
- Historical duplicate transaction IDs cannot merge same-transaction values.
- All value aggregation uses `BIGNUMERIC`.
- The output is exactly one aggregate row and contains no address.
- Provider requests, provider points, and written paths remain zero.
- Existing identity resolution and consumer behavior remain unchanged.
