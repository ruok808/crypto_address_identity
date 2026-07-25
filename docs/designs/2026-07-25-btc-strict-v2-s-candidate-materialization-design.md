# BTC Strict V2-S Candidate Materialization Design

## Status

Approved as the frozen BTC bootstrap materialization policy.

Strict V2-S is not a placeholder for a required V3. The next billed source
scan, if separately authorized, must materialize the actual address rows under
this contract. Whether Strict V2-S remains the long-term policy is decided only
after address-level quality review of the published artifact.

This approval covers implementation and free cost validation. It does not
authorize a billed BigQuery execution, provider request, or consumer behavior
change.

## Goal

Materialize the exact Strict V2-S coarse candidate union from the fixed
positive-value BTC population in one full-history source scan. The artifact
must support deterministic prioritization and later provider enrichment
without repeating the expensive source scan.

The bootstrap deliverable is the immutable address artifact, not another
aggregate-only count.

## Source Contract

Materialization is allowed to start only when the offline dual-population
validator returns:

- `status=accepted`;
- `policy_denominator=positive_value`;
- `positive_value_standard_address_count=1,531,420,608`;
- `output_defined_standard_address_count=1,557,941,780`;
- `allow_materialization_design=true`;
- `candidate_materialization_allowed=false`;
- no blocking reasons.

The source evidence is pinned to:

- cutoff height `959187`;
- cutoff time `2026-07-24T23:59:59.999999Z`;
- transaction schema SHA-256
  `7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7`;
- positive-value receipt SHA-256
  `c3123159ba77e0bcd5ba4735483027899bc451a50c8648784ec4317dfe20a236`;
- aggregate query SHA-256
  `47b0b8977cc1443578bc3daf3f90a2cf5e0e48ae758a4b7a133d3caa7d301e74`.

The materialization SQL will have its own checksum. It must reproduce the
Strict V2-S definitions from the aggregate query, not reinterpret them.

## Exact Target

The materialized coarse union must contain exactly `1,090,411` unique
addresses in four disjoint tiers:

| Tier | Definition | Expected rows |
| --- | --- | ---: |
| `p0` | `strict_p0_mask != 0` | 21,736 |
| `p1` | non-P0 and `v2_chain_score >=25` | 2,143 |
| `edge` | non-P0 and score `15..24` | 133,730 |
| `coarse_other` | remaining Strict V2-S coarse members | 932,802 |

The source's remaining `1,530,330,197` positive-value addresses are excluded.
The `26,521,172` output-defined but non-positive addresses are outside the
economic-policy denominator and cannot enter the artifact.

## Query Strategy

Use one fixed standard SQL query against the same public Bitcoin transaction
table and cutoff. Reuse the aggregate query's stages through
`variant_policy`, then filter `strict_is_coarse`.

The query must:

- scan the transaction table once;
- retain only single-address input/output subjects;
- use positive-value `has_output` semantics;
- reproduce every Strict V2-S score and reason;
- assign exactly one tier using precedence `p0`, `p1`, `edge`,
  `coarse_other`;
- return no transaction hash, block hash, raw input/output row, provider
  identifier, or source payload;
- add a deterministic bucket in `0..63` from the normalized address;
- use a deterministic job id;
- set `maximum_bytes_billed=650_000_000_000`;
- disable automatic query retry;
- write with `WRITE_EMPTY` to a deterministic, private destination table with
  a seven-day expiration.

Do not use `CREATE OR REPLACE`. An existing destination table triggers
same-job reconciliation and schema/checksum validation, never overwrite or a
second query.

Before execution, run one free dry run. Pin its exact byte estimate, query
checksum, schema checksum, monthly billed usage, and reserve in a separate
authorization receipt. Any drift requires review; it must not be accepted by
raising the byte cap.

## Candidate Row Contract

Each row contains only address-level derived features:

- `normalized_address`;
- `candidate_tier`;
- `tier_rank`;
- `address_bucket`;
- `v2_chain_score`;
- `strict_p0_mask`;
- `receipt_support_mask`;
- `current_utxo_sats`;
- `lifetime_received_sats`;
- `residual_gross_90d_sats`;
- `max_same_tx_received_lifetime_sats`;
- `max_same_tx_received_365d_sats`;
- `max_same_tx_received_90d_sats`;
- 90-day and 365-day qualifying receipt counts;
- 90-day and 365-day active transaction/day counts;
- `last_seen_time`;
- `candidate_row_sha256`.

Satoshi amounts use exact `DECIMAL(38,0)` semantics. Timestamps are UTC.
`candidate_row_sha256` is computed from a versioned canonical encoding of all
preceding row fields.

The P0 mask is:

- bit `1`: current UTXO at least 100 BTC;
- bit `2`: sustained residual 90-day gross flow at least 1,000 BTC;
- bit `4`: supported recent receipt;
- bit `8`: supported active lifetime receipt.

The receipt-support mask is:

- bit `1`: retained;
- bit `2`: repeated;
- bit `4`: sustained activity.

## Extraction And Local Publication

After the one query reaches `DONE`, read the private destination table through
the BigQuery Storage API in bounded Arrow batches. Result transfer may resume
from the completed destination table; query submission may not retry.

Write into a local staging directory using:

```text
data/universe/campaigns/<campaign-id>/
  candidates/tier=<tier>/bucket=<00..63>/part-00000.parquet
  manifest.json
  execution_receipt.json
```

For deterministic artifacts:

- sort each tier/bucket partition by normalized address;
- use one pinned Arrow schema and Parquet writer version;
- use directory mode `0700` and file mode `0600`;
- record every file size, row count, and SHA-256 in the manifest;
- publish by atomic directory rename only after all gates pass;
- never expose a mutable `latest` pointer in the first version.

If extraction fails, discard or quarantine the incomplete staging directory.
The completed cloud job remains recoverable until destination-table expiry.
Recovery reads the same table and cannot submit a replacement query.

## Quality Gates

Blocking gates before publication:

1. dual-population contract is no longer accepted;
2. source cutoff, schema, aggregate receipt, or materialization query checksum
   drifts;
3. BigQuery dry-run bytes exceed the fixed cap or budget reserve;
4. destination schema differs from the pinned candidate schema;
5. total row count is not exactly `1,090,411`;
6. tier counts differ from `21,736 / 2,143 / 133,730 / 932,802`;
7. a normalized address is null, invalid Bitcoin mainnet, or duplicated;
8. tier predicates, score, P0 mask, or receipt-support mask cannot be
   recomputed from the row features;
9. any satoshi/count field is null, negative where prohibited, fractional, or
   outside its declared type;
10. a row contains a transaction/block identifier or an unapproved field;
11. partition row counts, row hashes, or file checksums do not reconcile;
12. provider requests or points are nonzero;
13. publication would overwrite an existing campaign.

Warnings requiring review:

- artifact size exceeds the reviewed local storage budget;
- extraction throughput risks destination-table expiry;
- one P0 reason dominates more than 80% of P0;
- a tier/bucket partition is unexpectedly empty or highly skewed;
- the source table reports a newer modification during extraction.

## Cost And Failure Boundaries

The source scan is expected to remain near the prior 638 GB aggregate scan,
but only a fresh free dry run may establish the execution estimate. The
operator must separately approve the exact estimate and available billing
reserve.

There is one query authorization:

- no automatic query retry;
- no fallback query;
- no second materialization variant;
- no P0-only trial scan, because it would pay for nearly the same full source
  read and then require another scan for the coarse union.

Local extraction can be restarted against the same completed destination table
without additional source-query billing. All retries are bounded data-transfer
recovery, not query resubmission.

## Downstream Use

Publication creates candidate evidence only. A later provider campaign:

1. imports the artifact without altering its tiers;
2. deduplicates addresses already present in identity SQLite;
3. enriches P0 first, followed by P1, edge, and coarse-other;
4. applies independent point, response-byte, TTL, and request-rate budgets;
5. records provider evidence under the existing
   `provider_default + local_override + conflict_first` resolver policy.

No candidate row becomes an alert suppression rule. Any `quant_crypto`
consumer integration remains a separate reviewed change and must pin a
resolver snapshot checksum.

## Implementation Approval Checklist

Implementation may begin only after separate approval of:

- fixed materialization SQL and result schema;
- TDD plan and no-retry executor;
- free dry-run byte estimate;
- billing and local-storage budget;
- deterministic destination table and campaign id;
- recovery procedure for a completed job;
- final statement that no provider or consumer action is included.
