# BTC Whale Bilateral Raw-Transaction Replay Audit

## Scope

This read-only audit reconstructs input and output address context from the BTC
whale raw-transaction lake, joins it to the current 30-day whale-alert outbox,
and resolves both sides through a pinned local identity snapshot. It did not
modify a `quant_crypto` worker, timer, lake object, manifest, cursor, outbox
record, email state, or suppression rule.

The source fixture was exported in two non-overlapping UTC shards and retained
only public input/output addresses, existing semantic scores, quality tags, and
delivery status. It excluded transaction IDs, alert IDs, recipients, message
content, provider payloads, and all credentials. Both local temporary shards
were deleted after replay.

## Completeness

- Window start: `2026-06-23T00:00:00Z`
- Current outbox events: `6,201`
- Raw-transaction matches: `6,201`
- Missing raw transactions: `0`
- Malformed transaction projections: `0`
- Events missing a parsed input address: `0`
- Resolver snapshot: `btc_identity_export_v2`, as-of `2026-07-23T00:00:00Z`
- Resolver manifest SHA-256:
  `32871277242026a978f161458adfa5f67d61c9f796a1c8541bca8c02c249c3f7`

## Bilateral Result

| Measurement | Count |
| --- | ---: |
| `InternalCandidate` events | 5,593 |
| Input addresses reconstructed | 18,456 |
| Input identity lookups found | 1,497 |
| Output identity lookups found | 1,482 |
| Same-entity bilateral events | 1,331 |
| Same-entity events involving `provider_default` | 1,331 |
| Same-entity events with independent/local evidence on both sides | 0 |
| Events meeting existing source-rule preconditions | 1,090 |
| `provider_default` suppression candidates | 1,044 |
| Independent-evidence suppression candidates | 0 |
| `conflict_first` side observations | 0 |
| Live alert/email/suppression changes | 0 |

Of the 1,044 provider-default candidates, 935 already have
`suppressed_internal_audit` status under the existing strong rule and 109 have
`sent` status. The 109 are therefore the maximum observed email-noise reduction
available through a controlled provider-default suppression policy. They remain
subject to the exact bounded predicate below; the replay itself does not change
their live delivery status.

## Decision

The reconstruction closes the prior structural-data gap: every audited whale
alert now has bilateral input/output context. The source-rule precondition
reproduces the existing `InternalCandidate`/`self_churn_possible`/score gates;
it is not a new ownership conclusion. The replay does **not** close the
independent-evidence calibration gap: no eligible candidate has independent
reviewed or local-override identity on both sides.

Under the accepted BTC identity policy, that gap does **not** block use of an
uncontested `provider_default`. The 109 sent candidates are eligible for a
controlled suppression rollout because they have complete bilateral context,
the existing source-rule preconditions, and no `conflict_first` result. The 935
already-suppressed counterparts support the same structural pattern.

Production behavior remains unchanged until a separate, bounded `quant_crypto`
alert-dispatcher change is deployed. That change should implement this exact
predicate, preserve an audit-outbox row for every suppressed event, and treat a
later evidence conflict or local rejection as an automatic exclusion from the
predicate.
