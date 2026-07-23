# BTC Whale Provider-Default Replay Audit

## Scope

This is a read-only replay of the 30-day BTC whale-alert outbox population
from `2026-06-23T00:00:00Z`. It did not modify a `quant_crypto` worker, timer,
cursor, lake row, manifest, outbox record, or email state.

The source was reduced before transfer to `output_address`, semantic-decision
category, and a positive aggregate count. It excluded alert IDs, transaction
IDs, recipients, message content, credentials, and provider payloads. The
temporary replay input was deleted after the run.

## Resolver Input

- Resolver snapshot as-of: `2026-07-23T00:00:00Z`
- Snapshot schema: `btc_identity_export_v2`
- Snapshot resolutions: `201`
- Snapshot manifest SHA-256:
  `32871277242026a978f161458adfa5f67d61c9f796a1c8541bca8c02c249c3f7`
- Resolver policy: `provider_default + local_override + conflict_first`

`provider_default` is permitted only for an uncontested Tier C commercial
`entity_control` value. `local_override` is append-only and may only select or
reject a value already supported by immutable evidence. An unresolved
disagreement remains `conflict_first` and is not promoted.

## Population And Baseline

The source population contained 6,192 unique alerts, represented by 758
sanitized `(output_address, semantic_decision)` aggregate records over 678
unique output addresses.

| Baseline category | Count |
| --- | ---: |
| Sent | 4,857 |
| Seeded historical | 360 |
| Already suppressed by the existing strong rule | 975 |
| `internal_candidate` | 5,584 |
| `digest_candidate` | 147 |
| `immediate_alert` | 26 |
| `needs_review` | 49 |
| No historic semantic value | 386 |

## Read-Only Replay Result

| Measurement | Result |
| --- | ---: |
| Weighted replay population | 6,192 |
| Existing business-field changes | 0 |
| Existing mail-action changes | 0 |
| Existing suppression-action changes | 0 |
| Found identity lookups | 1,482 |
| `provider_default` lookups | 1,477 |
| `unreviewed_evidence` lookups | 5 |
| `InternalCandidate` events with an identity lookup | 1,374 |
| `InternalCandidate` events with `provider_default` | 1,369 |
| `InternalCandidate` events with `local_override` | 0 |
| `InternalCandidate` events with `conflict_first` | 0 |
| `InternalCandidate` events without input-side context | 5,584 |

The replay proves non-interference: this resolver export only appends identity
fields, so it does not alter current email volume, alert actions, or existing
strong-condition suppression. It introduces no new missed-alert path in this
observation-only use.

## Decision

Adopt `provider_default` for read-only identity enrichment and continue to
surface `resolution_policy` to consumers. Do not use these results to expand
production suppression: every `InternalCandidate` in this historical outbox
lacks the input-address set needed to prove same-entity control and evaluate
the ownership semantics.

The next eligible analysis is a separate read-only reconstruction from raw BTC
whale transactions that retains both input and output addresses. It can test the
current strong suppression rule directly and quantify counterfactual email
reduction without changing live alert behavior.
