# BTC V2-S P2 Enrichment Closure Audit

Date: 2026-08-01

Status: **accepted for the bounded P2 campaign scope**

## Scope

This audit closes the checksum-pinned, point-bounded BTC V2-S P2 address
enrichment campaign. It contains aggregate metadata only. It does not disclose
addresses, provider entity identifiers, payloads, credentials, payment headers,
or secret configuration.

The paid campaign ran from the exact code commit
`b40ebd0547a8d1da02ca8c2b35605cc6955a85da`. Migration
`010_v2s_p2_address_enrichment_campaign` was applied before dispatch. The daily
coverage-sync LaunchAgent remained unloaded, and one detached supervisor exited
naturally after the final checkpoint.

No provider request was made during the closure audits, resolver rebuild,
snapshot export, consumer validation, or replay.

## Pinned Lineage

| Artifact | Semantic SHA-256 | File or Parquet SHA-256 |
| --- | --- | --- |
| Strict V2-S candidate manifest | `a003ca6761f74bdafacac289b6c04d499e322e43e449e2d400bedfe2a353b5d4` | `ac61fe60646846b4bbdf4920000eba45ed416f6adb6806cf7cf51eb9d476dd5d` |
| P0/P1 closure coverage manifest | `ed5ae7464eb5ae5dc4436892486cf33845a8c41c3dae7f74cf7e9966a25e7042` | `32c6c73567031d326af72e0088793bf3d124267b18266447b84f432b43ed6fe4` |
| P2 queue manifest | `0fafc57036aa145768c441e86f5e21a1586f91ee46f9a71f67eac2905098259b` | `62b2d2bdaeaa3905146731ac1c41fdf2a21d3132df4c294552ad5ecc5e10e36f` |
| P2 queue Parquet | n/a | `ebf60e8a50d4d36e0ef6c6b56aebb2241bfbc4ec31048e42928a8e0bd7672a70` |
| Final P2 coverage manifest | `a6347ccc73e37a31adf9246206d737597a7455cc0ee6ded3ddf299d7152847b0` | `70a2a4bfca251837fec4c69ed0c018b6ad1bcd277ee3205b667952ae4dc340da` |
| Final P2 coverage Parquet | n/a | `48ee7615eaf123202491becaeecc88d714f90365a8d3f2d35e8915f6d2899caa` |

All four manifests passed their semantic checksum validators. Every declared
Parquet size, row count, and file checksum matched. The P2 queue pins the exact
candidate and prior-coverage semantic checksums above.

The queue contains 190,000 rows, 190,000 distinct subjects, contiguous ranks
from 1 through 190,000, and no invalid cohort. Its fixed tier allocation is
87,680 `edge` rows and 102,320 `coarse_other` rows.

## Provider And Budget Accounting

| Metric | Result |
| --- | ---: |
| Direct attempts | 190,000 |
| Provider requests | 190,000 |
| Successful responses | 189,879 |
| Transport failures | 114 |
| HTTP failures | 7 |
| In-flight requests at closure | 0 |
| Parsed-success responses | 189,879 |
| Malformed responses | 0 |
| Payload records | 189,886 |
| Response bytes | 56,613,192 |
| Evidence rows appended | 245,473 |
| Duplicate evidence rows | 0 |
| New entity seeds appended | 112 |
| New membership rows | 0 |

The supervisor reached the full 190,000-point direct safety accounting ceiling.
Response-backed provider point evidence was 189,886; the 114 transport failures
are retained separately instead of being assumed billable or free. The
separately bounded fanout/recovery ledger used one paid run, three requests, and
three points, with no membership growth.

| Budget component | Points |
| --- | ---: |
| User-reported starting balance | 225,353 |
| Direct ceiling | 190,000 |
| Response-backed provider point evidence | 189,886 |
| Fanout/recovery point use | 3 |
| Conservative accounted use | 190,003 |
| Conservative retained balance | 35,350 |
| Protected reserve | 25,353 |
| Conservative margin above reserve | 9,997 |

Both the 190,000 direct ceiling and the 10,000 fanout/recovery ceiling were
respected. The protected account reserve remained intact under the conservative
accounting above; the retained balance is computed from the user-reported
starting balance rather than a new provider-account query.

## Ledger And Idempotence Audit

The append-only campaign tables contain exactly 190,000 attempt rows for
190,000 distinct subjects. All attempts bind to one queue manifest.

| Integrity check | Result |
| --- | ---: |
| Duplicate attempt rows | 0 |
| Queue-manifest mismatches | 0 |
| Subjects with repeated campaign requests | 0 |
| Subjects with repeated successful requests | 0 |
| Exhausted historical 502 retries after P2 start | 0 |
| Supervisor error-log rows | 0 |

A post-completion dry-run against the original queue and campaign ledger
returned:

| Dry-run field | Result |
| --- | ---: |
| Eligible queue rows | 190,000 |
| Already attempted | 190,000 |
| Planned requests | 0 |
| Provider requests | 0 |
| Estimated points | 0 |
| Campaign points before/after | 190,000 / 190,000 |

This proves the closed campaign cannot repeat a successful paid request through
the normal dispatcher path.

## Final Coverage

The final immutable coverage snapshot is
`data/coverage/btc-v2s-p2-959187/20260801T025955Z-ac61fe606468`.

It contains 1,090,398 rows and 1,090,398 distinct subjects. Duplicate subjects,
unknown states, and source-pin mismatches are all zero.

| Coverage state | Rows |
| --- | ---: |
| `direct_enriched` | 213,857 |
| `entity_membership_covered` | 464 |
| `local_evidence_covered` | 0 |
| `needs_direct_enrichment` | 876,077 |

The increase in `direct_enriched` rows is exactly 189,879, matching the P2
successful-response count. The remaining population is outside this closed
190,000-row selection and requires a new immutable queue and separately approved
budget before any future provider work.

## Resolver Snapshot

The resolver was rebuilt append-only at `2026-08-01T02:59:55Z`, the final
campaign checkpoint, and exported to:

```text
data/exports/bitcoin/v2/20260801T025955Z
```

| Snapshot contract | Result |
| --- | ---: |
| Schema | `btc_identity_export_v2` |
| Manifest SHA-256 | `a9549bad4e797e5744a795f71cf53f6c8813ce5c54c65dc9a8b787cec8116368` |
| Resolution rows | 169,922 |
| Evidence summary rows | 169,922 |
| `resolutions.ndjson` SHA-256 | `1b551e15df8fb5b2f9542700b2d71c4e8ea7778d02f1eb4acb88b783b537a70b` |
| `evidence_summary.ndjson` SHA-256 | `dd3ef47ad59fd17e8991c83dd1e177acb1fdadc35f35b1eb3ff07e41e962a7f9` |
| Lookup-usable rows | 40,542 |
| Conflict-first rows | 65 |

The CAI loader verified every file checksum and loaded all 169,922 rows. Both
NDJSON line counts matched the manifest, the registry contains one row for this
immutable path, and a repeated export returned the same manifest checksum.

## Read-Only Consumer Replay

Two immediate replay-first checks were completed without changing consumer
configuration or production state.

1. The CAI summary-only `quant-crypto-btc` fixture replay processed three fixture
   events. Existing business-field changes, mail-action changes, and
   suppression-action changes were all zero. One lookup was found and two were
   not found.
2. The quant_crypto checksum-pinned loader accepted the candidate snapshot and
   loaded 40,657 `entity_control` rows, including 40,542 lookup-usable rows. Its
   focused identity-suppression and snapshot-cutover test suites passed 23/23.

These are local fixture and compatibility results, not a production cutover.
No alert threshold, ownership-semantic rule, suppression rule, email behavior,
environment file, timer, worker, lake object, cursor, outbox, or production
service was changed.

## Runtime Cleanup And Acceptance

After all audits and snapshot validations passed, only the temporary supervisor
script and state file were removed. The SQLite database, append-only supervisor
log, empty error log, provider observations, raw-payload records, source
artifacts, final coverage snapshot, original queue, and resolver export remain
preserved outside Git.

The P2 campaign is closed and accepted:

- the fixed queue is fully attempted with no in-flight work;
- point ceilings and the protected reserve are intact;
- historical exhausted 502 work was not retried;
- checksum, queue, coverage, and idempotence audits pass;
- the resolver snapshot is immutable and consumer-readable;
- the detached supervisor and lock are absent;
- the daily coverage-sync LaunchAgent remains unloaded; and
- no additional paid request is required for this campaign.
