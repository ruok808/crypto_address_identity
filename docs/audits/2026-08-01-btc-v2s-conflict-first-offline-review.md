# BTC V2-S Conflict-First Offline Review

Date: 2026-08-01

Status: **offline remediation validated; production consumer unchanged**

## Scope

This audit records the aggregate-only review of the seven `conflict_first`
events found by the `quant_crypto` BTC V2-S P2 candidate replay. It does not
disclose addresses, entity identifiers or names, asserted values, raw provider
payloads, credentials, payment headers, or secret configuration.

The review was purely offline. No provider request was issued and no paid point
was consumed. The historical identity evidence, claims, conflicts, source
observations, and prior immutable snapshots were preserved.

## Reproduction

The fixed 42-day consumer replay covered completed UTC days from
`2026-06-20T00:00:00Z` inclusive through `2026-08-01T00:00:00Z` exclusive. It
scanned 2,850 raw files and evaluated 8,774 severity-eligible candidate alerts.
The first candidate produced seven `conflict_first` events involving two
distinct snapshot subjects.

For all seven events, the conflict subject appeared on both transaction sides.
The counterpart policy was `unreviewed_evidence` for all 35 side occurrences.
Omitting the conflict records would have failed closed as
`output_identity_unresolved` for all seven events; it would not have created a
suppression.

## Evidence Review

Both relevant subjects had the same aggregate evidence structure:

- three valid active evidence rows across two values;
- one Tier B official row and two Tier C commercial-provider rows;
- no cross-value source overlap;
- no cross-value independence-group overlap;
- one unique higher-tier official value; and
- no existing claim review or local override.

The producer therefore appended an `accept` claim review and a `select` local
override for the existing Tier B official value on each subject. The operation
added two review rows and two override rows. It did not delete or rewrite any
historical evidence or conflict record.

## Snapshot Validation

The deterministic rebuild changed the aggregate `entity_control` policy counts
by exactly two: `conflict_first` decreased by two and `local_override`
increased by two. Total resolution and evidence-summary counts remained 169,922
each.

The refreshed immutable snapshot is exported at `2026-08-01T05:55:20Z` with
manifest SHA-256
`4d3fc8c86c31deeef79ad1b3e9e7f32c4b906da89a7a0d42397fc8412b66ce01`.
The manifest and every declared file checksum passed. A repeated dry-run export
reproduced the same manifest. The CAI reader loaded 169,922 resolutions, and the
`quant_crypto` checksum-pinned reader loaded 40,657 `entity_control` records.

## Replay Gate

The same fixed-window gate returned `allow` with no blocking reasons:

| Metric | Active | Remediated candidate |
| --- | ---: | ---: |
| Snapshot loaded | yes | yes |
| Severity-eligible candidate alerts | 8,774 | 8,774 |
| Raw transaction files scanned | 2,850 | 2,850 |
| Provider-default bilateral suppressions | 263 | 674 |
| Conflict-first decisions | 0 | 0 |
| Local rejection decisions | 0 | 0 |
| Assertion failures | 0 | 0 |
| Provider default not observed | 0 | 7 |

All existing checks passed: both snapshots loaded, assertions remained zero,
conflict-first and local rejection counts did not increase, and
provider-default coverage did not regress.

## Operational State

The production consumer snapshot was not changed by this review. Alert
thresholds, ownership semantics, suppression predicates, email behavior, timer
configuration, and unrelated production services were not changed.

The paid P2 campaign remains closed at 190,000 direct attempts plus three
bounded fanout/recovery requests. No P2 supervisor or detached screen session
is running, and the daily CAI coverage-sync LaunchAgent remains unloaded.
