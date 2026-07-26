# BTC High-Value Bootstrap P2 Checkpoint

Date: 2026-07-26

## Scope

This checkpoint closes the first immutable BTC resolver publication and fixes
the input, ranking, and point budget for the next address-level enrichment
campaign. It contains aggregate metadata only. It does not disclose addresses,
entities, provider payloads, or credentials.

No provider request was made while producing this checkpoint.

## Immutable Resolver V2

The first consumer-readable resolver snapshot is:

```text
data/exports/bitcoin/v2/20260726T062042Z
```

| Contract | Result |
| --- | ---: |
| Schema | `btc_identity_export_v2` |
| As of | `2026-07-26T06:20:42Z` |
| Resolution rows | 27,018 |
| Evidence summary rows | 27,018 |
| Resolved rows | 13,520 |
| Unattributed rows | 13,433 |
| Ambiguous rows | 65 |
| Provider-default rows | 7,560 |
| Conflict-first rows | 65 |
| Lookup-usable rows | 7,560 |

Checksums:

```text
manifest.json
8ffbeef71dd177e69f8e95b63b8497a46e04304131f48ca632062b74ddad40f6

resolutions.ndjson
c701a4bd7b731fe0f2dd46f14d23751c33ecd59016756281821c9205fc4f6932

evidence_summary.ndjson
eff4589a12786bc4d8b44d2a245bed40fcb5951471efc8d86da714d0d9f04946
```

The repository's `ResolverSnapshot.load` verifier accepted the snapshot. Both
NDJSON files contain exactly 27,018 rows and match their declared checksums.

The mutable database contained later resolver activity when this audit was
run. Those later rows are intentionally excluded by the snapshot's fixed
`as_of`; the snapshot is not expected to equal the current mutable row count.

## P2 Source Contract

P2 reads only the existing checksum-pinned Strict V2-S candidate artifact and
the final P0/P1 closure coverage snapshot:

| Input | Contract |
| --- | --- |
| Candidate campaign | `btc-v2s-bootstrap-959187` |
| Candidate rows | 1,090,398 |
| Coverage snapshot | `20260726T050439Z-ac61fe606468` |
| Coverage manifest SHA | `ed5ae7464eb5ae5dc4436892486cf33845a8c41c3dae7f74cf7e9966a25e7042` |
| Uncovered P2-eligible rows | 1,065,956 |
| Remaining mandatory direct rows | 0 |

The queue builder verifies every selected source parquet's byte size, row
count, and SHA-256. It then requires exact row reconciliation across:

1. the `edge` and `coarse_other` candidate population;
2. the same population in the coverage snapshot; and
3. the exact address, tier, and candidate-row-hash join.

Any duplicate, missing row, metadata mismatch, or checksum drift blocks
publication.

## Point Budget

The operator-confirmed 0xRouter balance for this checkpoint is 225,353 points.

| Budget component | Points |
| --- | ---: |
| Account balance | 225,353 |
| Protected account reserve | 25,353 |
| Total P2 campaign ceiling | 200,000 |
| Fanout and transport-recovery reserve | 10,000 |
| Direct address enrichment ceiling | 190,000 |
| Observed canary p95 points per address | 1 |
| Maximum direct addresses | 190,000 |

The direct campaign must use 190,000, not 200,000, as its address-enrichment
point limit. The remaining 10,000 points are not available to direct dispatch.

## Selection Policy

Policy: `btc_v2s_p2_economic_leader_v1`.

The policy reuses the economic-leader ranking whose canary attribution yield
was materially higher than deterministic random samples. It selects uncovered
`edge` and `coarse_other` addresses by:

1. unresolved mandatory address-level work first;
2. descending maximum across current UTXO, lifetime received, recent gross
   activity, and lifetime/365-day/90-day maximum same-transaction receipt;
3. descending V2 chain score; and
4. a fixed seeded SHA-256 tie breaker.

The zero-request dry-run produced:

| Result | Value |
| --- | ---: |
| Queue rows | 190,000 |
| `edge` rows | 87,680 |
| `coarse_other` rows | 102,320 |
| Selection boundary | 1,310,604,297 sats |
| Mandatory rows selected | 0 |
| Dry-run writes | 0 |
| Provider requests | 0 |

Two separate dry-runs produced the same candidate parquet SHA:

```text
ebf60e8a50d4d36e0ef6c6b56aebb2241bfbc4ec31048e42928a8e0bd7672a70
```

The manifest SHA changes with its UTC build timestamp; the parquet SHA is the
content identity for the selected address sequence.

## Execution Gates

Paid execution is allowed only after the P2 tooling reaches canonical `main`
and the queue is published from that commit.

Operational gates:

- the daily maintenance LaunchAgent must not overlap the resident P2 campaign;
- dispatch remains at 25 requests per minute, below the provider's 30/minute
  limit;
- each address/profile pair is attempted at most once per campaign;
- no retry is made for exhausted entity 502 outcomes;
- transport failures use one bounded recovery ledger and never restart the
  main queue;
- coverage and entity fanout are rebuilt at deterministic checkpoints;
- the campaign stops before consuming the protected account reserve;
- raw payloads and ledgers remain restricted runtime artifacts.

P2 remains Tier C discovery and lookup evidence. It does not override local
evidence and does not directly alter downstream alert or suppression policy.

## Pre-Execution Schema Gate

The first resident-process start exposed a local schema compatibility gap
before any provider request was sent. Application code accepted cohort `p2`,
but migration 009 still constrained the two campaign tables to
`urgent/p0/p1`. SQLite therefore ignored the new P2 attempt rows.

Observed impact:

| Metric | Result |
| --- | ---: |
| Provider observations | 0 |
| Provider points | 0 |
| Address attempts accepted | 0 |
| Local failed quota reservations | 205 |

The process was stopped. Migration 010 rebuilds both campaign tables with
`p2` in the cohort CHECK while copying all existing P0/P1 campaigns and
attempts, recreating indexes, foreign keys, and append-only triggers. Tests
prove a v9 database preserves a legacy P0 attempt and accepts a new P2 attempt
after migration.

The failed local reservations remain audit evidence and are not deleted. The
orphan run is marked failed with a schema-gate reason before the campaign is
restarted from the checksum-pinned queue.
