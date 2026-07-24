# BTC Identity Operations

## Boundary

This service is BTC-first and local by default. It records source observations,
evidence, claims, conflicts, and resolver snapshots. It does not run a timer,
does not change a `quant_crypto` worker, and does not make a live provider call
without a separate explicit approval.

## Runtime Setup

Use Python 3.13 and non-secret settings from
`conf/env/address_identity.env.example`. `CAI_0XROUTER_TOKEN` is environment
only. It must not appear in shell history, committed files, JSON output, raw
observations, exports, or support reports.

```bash
PYTHONPATH=src python -m crypto_address_identity init-db
PYTHONPATH=src python -m crypto_address_identity candidates import --file candidates.ndjson --dry-run
PYTHONPATH=src python -m crypto_address_identity fetch run --dry-run --limit 10
```

Dry runs validate candidate selection but do not write an ingestion run, raw
payload, observation, evidence, claim, resolution, or export. Execute-mode
fetches require an explicit live-fetch approval and a configured token.

Official evidence importers are separate from provider enrichment. The OKX
import verifies signed PoR rows from an operator-supplied public archive. The
BITB importer retrieves only its fixed public issuer page; it stores a
sanitized address snapshot rather than page HTML because the page can contain
short-lived report links.

```bash
PYTHONPATH=src python -m crypto_address_identity evidence import-bitwise-bitb --dry-run
```

The Tier B BITB evidence expires after 31 days and remains subject to explicit
review before it becomes `lookup_usable`. Neither importer can modify a
consumer's alert or suppression policy.

## Candidate Intake

Candidate NDJSON is an audit handoff, not a provider request log. It accepts
only `bitcoin` and the reasons `known_watchlist`, `whale_counterparty`,
`transfer_counterparty`, `official_evidence`, `manual_review`, and `replay`.
Repeated candidates retain provenance but do not create a second address
subject. The fetcher applies discovery/detail freshness before provider work.

The configured gateway ceiling is 30 requests/minute. The service defaults to
20 requests/minute in a SQLite-backed shared rolling window. Its byte budget is
per run and includes received payload bytes. HTTP 429, timeout, or malformed
payload records an outcome; it never becomes negative identity evidence.

## Coverage-Driven Chaindata Sync

The currently available 0xRouter Chaindata routes do not expose a complete
Arkham update feed. `coverage-sync` therefore targets business-relevant BTC
coverage rather than claiming an all-Arkham export. Its bounded stages are:

1. Discover large BTC-relevant entities with two Bitcoin-filtered balance-change
   rankings.
2. Expand only due entities through `entity/{id}` and
   `entity_predictions/{id}`. Predictions are stored as provenance-preserving
   memberships, not silently promoted to direct address labels.
3. Enrich only due addresses from the existing candidate queue and the stored
   entity-prediction fan-out. Candidate requests win priority; predicted
   addresses fill the remaining bounded capacity. It uses the live-validated
   BTC `address_enriched/{address}/all` profile with tags, predictions, and
   clusters enabled; response budgets and TTLs bound the richer payload.

Seed provider entity IDs through an append-only NDJSON handoff:

```bash
PYTHONPATH=src python -m crypto_address_identity coverage-sync seed-entities \
  --file entity-seeds.ndjson --dry-run
PYTHONPATH=src python -m crypto_address_identity coverage-sync seed-entities \
  --file entity-seeds.ndjson
```

Run the plan first without network or writes:

```bash
PYTHONPATH=src python -m crypto_address_identity coverage-sync run --dry-run
```

Execute mode needs a healthy, explicitly approved provider token. It uses a
single SQLite-coordinated request window at 25/minute by default, stores
content-addressed raw responses, reports a conservative `ceil(bytes/100000)`
points estimate, and caches entity/address observations by independent TTLs.
Its default plan reserves two requests for discovery, two per entity for detail
and prediction fan-out, then fills the remaining request capacity with direct
address enrichment (eight entities leave seven direct-address requests). A
subsequent run where entity details are still fresh spends that capacity on more
due addresses instead.

Entity-detail and entity-prediction TTLs are independent: a structurally valid
prediction response containing no Bitcoin addresses is a terminal BTC-negative
coverage result for that entity until its TTL expires, while a transport or HTTP
failure remains due for retry even when the entity detail is fresh.
It never turns a failed, rate-limited, or malformed response into negative
evidence. A `403` must be resolved with the gateway before execute mode.

## Source-Scoped Calibration

An official evidence set can be queued as a bounded provider-calibration panel
without enrolling it in any consumer policy. Use a distinct source reference
for each independent evidence group, then force the discovery profile:

```bash
PYTHONPATH=src python -m crypto_address_identity audit seed-provider-panel \
  --official-independence-group okx_por \
  --source-reference calibration:okx_por:YYYY-MM-DD \
  --requested-at YYYY-MM-DDTHH:MM:SSZ --dry-run

PYTHONPATH=src python -m crypto_address_identity fetch run --dry-run \
  --profile discovery --limit 20 \
  --source-reference-prefix calibration:okx_por:

PYTHONPATH=src python -m crypto_address_identity audit provider-panel \
  --source-reference-prefix calibration:okx_por: \
  --official-evidence-tier A \
  --official-independence-group okx_por
```

For a direct issuer-publication panel, declare Tier B explicitly rather than
mixing it into signed-proof statistics. A source-scoped discovery run excludes
addresses with a fresh discovery observation before selecting its next batch;
this permits clean batches at the configured request ceiling. The panel reports
entity-name agreement only at its selected evidence tier and source group.
Provider address labels, product names, and wallet roles are reported
separately; they do not become entity-control or suppression evidence.

## Evidence and Review

- Tier A: valid cryptographic verifier output only.
- Tier B: official/regulator artifact with explicit review before
  `lookup_usable`.
- Tier C: 0xRouter/Arkham source observation. A single active commercial
  `entity_control` value with no competing active value resolves as
  `provider_default`; generic address labels and tags remain discovery-only.
- Tier D/E: public research or local heuristic corroboration/context.

Every evidence row preserves source URL, artifact hash when applicable, license,
independence group, timestamp, effective interval, and verification method.
Different entity assertions for the same address form a conflict set. An
unresolved conflict always returns `ambiguous` with `resolution_policy`
`conflict_first`; source ranking never chooses a winner. A local correction is
an append-only, review-referenced `select` or `reject` decision over a value
already represented in the evidence ledger. It cannot invent a new value,
overwrite source evidence, or delete a conflict history. A selected value is
returned as `local_override` and remains traceable to the underlying conflict.

Record a correction only after importing the evidence that supports the selected
or rejected value, then rebuild and export a new snapshot:

```bash
PYTHONPATH=src python -m crypto_address_identity resolve override \
  --chain bitcoin --address <btc-address> --assertion-type entity_control \
  --asserted-value '<canonical-value>' --decision select \
  --reviewer-ref '<review-record>' --reason-ref 'https://evidence.example/reason' \
  --reviewed-at 2026-07-23T00:00:00Z
```

The command creates an immutable override record; it does not rewrite a prior
resolution or export. A following `resolve rebuild` is required.

## Snapshot Export and Consumer Replay

Build a resolver revision, then export a pinned snapshot:

```bash
PYTHONPATH=src python -m crypto_address_identity resolve rebuild --as-of 2026-07-22T00:00:00Z
PYTHONPATH=src python -m crypto_address_identity export resolver --chain bitcoin --as-of 2026-07-22T00:00:00Z
```

The export contains `manifest.json`, `resolutions.ndjson`, and
`evidence_summary.ndjson`. Current resolver exports use the v2 record contract
and include `resolution_policy`, resolved display values, and the canonical
asserted value. A consumer verifies every file checksum and pins a manifest
hash. It does not follow a mutable latest pointer.

The BTC replay adapter is read-only. It adds identity caveat fields but must not
change event ids, amounts, directions, thresholds, quality decisions, alert
decisions, or ownership-semantics decisions. A real `quant_crypto` integration
requires a separate implementation plan and approval.

Use summary mode for production-derived data; it omits event records and reports
only aggregate coverage and non-interference accounting:

```bash
PYTHONPATH=src python -m crypto_address_identity replay quant-crypto-btc \
  --input sanitized-btc-whale-outbox.ndjson \
  --snapshot data/exports/bitcoin/v2/<as-of> --summary-only
```

`mail_action_changes` and `suppression_action_changes` must remain zero. The
historic BTC whale outbox stores output-side context, not the input-side address
set required to recompute ownership semantics. Therefore its replay can measure
output-address coverage, but cannot prove a hypothetical suppression is safe.
For a privacy-minimized outbox audit, an input row may carry a positive integer
`replay_weight`; aggregate counters are then weighted to the original alert
population while no alert identifiers, transaction ids, or recipients enter the
replay file.

The bilateral whale replay accepts repeated `--input` paths, so a production
derived fixture can be exported in bounded, non-overlapping time shards without
creating a remote temporary file:

```bash
PYTHONPATH=src python -m crypto_address_identity replay btc-whale-bilateral \
  --input sanitized-shard-a.ndjson --input sanitized-shard-b.ndjson \
  --snapshot data/exports/bitcoin/v2/<as-of>
```

Its summary distinguishes two counterfactual classes without changing live
delivery: `provider_default_suppression_candidates` require at least one
provider-default side of a same-entity match, while
`independent_evidence_suppression_candidates` require both sides to resolve
through reviewed evidence or an append-only local override. Under the BTC
identity policy, an uncontested `provider_default` is usable unless it is later
falsified; independent/local evidence is a higher-confidence calibration path,
not a prerequisite for a controlled suppression proposal. Any proposal still
requires complete input context, the existing source-rule preconditions,
`conflict_first` exclusion, and a durable audit outbox. The source
delivery-status breakdown is an audit of possible email reduction, not itself
an instruction to withhold a message.

## Raw Payload Retention

Raw payloads are gzip-compressed, SHA-256 addressed files under
`CAI_RAW_PAYLOAD_ROOT`, outside Git. SQLite records only hash, safe relative
path, compression, byte count, and retention status.

Retention procedure:

1. Export and verify a resolver snapshot for the payload period.
2. Record the retention decision and preserve the payload SHA-256 in SQLite.
3. Remove a restricted payload only through a future retention command that
   marks it expired or missing without deleting audit metadata.
4. Never replace a payload path with different content.

## BTC Address Validation Basis

The implementation uses an audited, test-vector-backed minimal parser rather
than a broad wallet library. It verifies Base58Check with double-SHA-256 and
implements BIP173 Bech32 plus BIP350 Bech32m checksums. Tests cover known
mainnet P2PKH/P2SH/BIP84-style SegWit forms, a BIP350 Bech32m vector, invalid
checksums, mixed case, and non-mainnet rejection. This parser normalizes an
identity subject; it is not a wallet or UTXO ownership verifier.

## Incident Triage

1. Stop execute-mode fetches if provider schema changes or a secret appears in
   output.
2. Use `cai audit coverage` to inspect safe outcome and evidence-tier counts.
3. Verify raw payload object hash and resolver snapshot manifest before drawing
   a conclusion from a label.
4. Keep unresolved conflicts as `ambiguous`; add a reviewed local override only
   when it selects or rejects existing evidence, never by patching exports or
   consumer rows manually.
5. Escalate a plan to promote a label into monitor or suppression behavior to a
   separately reviewed consumer policy change.
