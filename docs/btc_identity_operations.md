# BTC Identity Operations

## Boundary

This service is BTC-first and local by default. It records source observations,
evidence, claims, conflicts, and resolver snapshots. It does not change a
`quant_crypto` worker. Live provider work is explicitly approved and bounded;
the optional local daily coverage-sync LaunchAgent is the only recurring task.

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

## BTC Chain-Universe Phase 1

The chain-universe path is separate from identity-provider enrichment. It uses
free chain facts to measure the address population and rank an eventual first
provider wave. Offline planning and aggregate candidate statistics always
report `provider_requests=0`, `provider_points=0`, and no written paths.

Follow this exact operating sequence:

1. **Offline configuration validation.** Run the BigQuery `--dry-run` command.
   It loads the fixed SQL resources and validates identifiers without
   constructing a Google SDK, Bitcoin RPC, or 0xRouter client.
2. **BigQuery metadata and dry-run probe.** After a separate read-only
   approval, use `--execute-readonly` with a positive
   `--maximum-bytes-billed` cap. The cap applies to the executing seven-day
   checkpoint query. The full-history feature query remains an unbilled
   BigQuery dry run so its actual byte estimate can be reported even when it
   exceeds that checkpoint cap. Record only schema hashes, query hashes,
   aggregate bytes, and checkpoint facts. The accepted source contract uses
   partitioned `transactions` and `blocks` tables. It rejects the flattened
   `inputs` and `outputs` compatibility views as partition authorities.
3. **Address-scale cost gate.** Use the separate
   `bigquery-address-scale` command to estimate an exact, aggregate-only
   `COUNT(DISTINCT address)` query. It reads only standard single-address
   transaction outputs; it does not read values, scripts, or inputs. Its
   `--execute-readonly` mode performs table metadata reads plus a free
   BigQuery dry run. It does not execute the aggregate and therefore does not
   return the address count.
4. **Candidate-statistics cost gate.** First run
   `bigquery-candidate-statistics --dry-run` without constructing a backend and
   record the fixed query SHA-256. Then, after read-only approval, pass that
   exact digest to `--execute-readonly`. The live path performs only one table
   metadata lookup, one current-month Jobs API listing, and one free BigQuery
   dry run. It fails closed above the fixed 650 billion-byte query limit or
   when projected month usage would consume the operator-required reserve. It
   has no chain-query execution path and does not return candidate counts.
5. **Bitcoin Core read-only probe.** Run only the four allow-listed RPCs:
   `getblockchaininfo`, `getblockhash`, `getblockheader`, and `getindexinfo`.
   Cookie content, authorization data, and raw RPC errors never enter output.
6. **Cutoff height/hash reconciliation.** Require the finalized BigQuery and
   Bitcoin Core height/hash to agree. A partial or unavailable Core source
   cannot establish a canonical complete-history cutoff.
7. **Review dry-run bytes.** Compare the reported bytes with the exact job cap,
   remaining account allowance, and local storage budget.
8. **Separately approved chain read.** Run one `--execute-chain-read` command
   with the reviewed query hash, cutoff, and positive
   `--maximum-bytes-billed`. Do not retry a completed job.
9. **Campaign checksum verification.** Verify the immutable manifest, Parquet
   schemas, source probes, and every recorded artifact checksum.
10. **Aggregate-only candidate dry-run.** Run `cai universe candidates` to
   inspect coverage, P0/P1/control counts, overlap, capacity, and projected
   time. It does not output addresses or open the identity SQLite database.
11. **Stop and report.** Phase 1 stops after aggregate statistics.
   It does not approve the 1,000-address canary.

Example offline and bounded commands:

```bash
cai universe probe bigquery --dry-run --as-of-date YYYY-MM-DD

cai universe probe bigquery --execute-readonly \
  --as-of-date YYYY-MM-DD --maximum-bytes-billed REVIEWED_PROBE_CAP

cai universe probe bigquery-address-scale --dry-run \
  --as-of-date YYYY-MM-DD

cai universe probe bigquery-address-scale --execute-readonly \
  --as-of-date YYYY-MM-DD \
  --sandbox-budget-bytes REVIEWED_SANDBOX_BUDGET

cai universe probe bigquery-candidate-statistics --dry-run \
  --as-of-date YYYY-MM-DD --cutoff-height FINALIZED_HEIGHT

cai universe probe bigquery-candidate-statistics --execute-readonly \
  --as-of-date YYYY-MM-DD --cutoff-height FINALIZED_HEIGHT \
  --expected-query-sha256 REVIEWED_QUERY_SHA256 \
  --sandbox-budget-bytes REVIEWED_SANDBOX_BUDGET \
  --reserve-bytes REVIEWED_RESERVE_BYTES

cai universe probe bitcoin-core --execute-readonly

cai universe build bigquery --execute-chain-read \
  --campaign-id CAMPAIGN --cutoff-height HEIGHT \
  --cutoff-time YYYY-MM-DDTHH:MM:SSZ \
  --maximum-bytes-billed REVIEWED_CHAIN_READ_CAP

cai universe candidates --campaign-id CAMPAIGN --dry-run \
  --runtime-minutes 480 --requests-per-minute 25
```

A public BigQuery dataset is not automatically free.
BigQuery free tier is account-wide. The candidate-statistics
probe estimates remaining capacity only from successful billed query jobs
visible to the current identity in the configured billing project for the
current UTC month. Jobs outside that project or outside the identity's list
permission are not inferred.
A `within_budget` address-scale result means only that the estimated bytes are
below the operator-supplied budget. It is not proof that the account still has
that allowance available, and it is not approval to execute the query.
A `within_budget` candidate-statistics result additionally means that the
projected successful-job total preserves the requested reserve and that the
fixed SQL compiled in a BigQuery dry run. It is still not the census result and
does not authorize executing the approximately 638 GB query.
A pruned Bitcoin Core node cannot prove historical script coverage; it reports
partial capability and the workflow stops before claiming chain-universe
completeness.

The address-scale query scans full-history outputs because every spent
standard address first appears in an earlier transaction output. It counts
only rows with one decoded address and therefore deliberately excludes empty,
multi-address, and undecodable scripts. Those script classes remain part of
the separate full-universe and Bitcoin Core storage design.

If Bitcoin Core is unavailable, record `reconciliation_status=partial` and stop
before `universe build bigquery`, even when the BigQuery source probe itself is
accepted. A full-history dry-run above the reviewed account or storage budget
is also a stop condition; do not raise the execution cap merely to obtain an
address count.

The immutable campaign stores scripts and address features under
`CAI_UNIVERSE_ROOT`. It snapshots only the minimal checksum-pinned calibration
anchor surface from identity SQLite. Candidate statistics union anchor-only
subjects without inventing chain balances, flow, ownership, labels, or wallet
roles. No source probe or candidate dry-run changes the existing daily
coverage-sync LaunchAgent.

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
Within the discovery queue, provider rank is retained and used as local priority;
explicit seeds and incomplete prediction retries still take precedence.

Entity-detail and entity-prediction TTLs are independent: a structurally valid
prediction response containing no Bitcoin addresses is a terminal BTC-negative
coverage result for that entity until its TTL expires, while a transport or HTTP
failure remains due for retry even when the entity detail is fresh.
Such incomplete prediction fetches are scheduled ahead of newly discovered
entities after a configurable retry cooldown (60 minutes by default), so a
transient provider error cannot be starved by ranking churn or repeatedly spend
the same run's request budget.
It never turns a failed, rate-limited, or malformed response into negative
evidence. A `403` must be resolved with the gateway before execute mode.

### Daily Local Incremental Task

The optional macOS LaunchAgent runs one execute-mode coverage-sync batch each
day at `03:20` in the local time zone. It has no `RunAtLoad` or `KeepAlive`;
installation and validation do not make a provider request. The normal TTLs,
shared 25/minute ceiling, 50 MiB response budget, and bounded entity/address
limits remain the cost controls for every daily batch.

It has a project-owned, token-only runtime env file at
`~/.config/crypto_address_identity/coverage-sync.env`. The file is outside Git,
must be owned by the local user with mode `0600`, and contains only the
environment-only `CAI_0XROUTER_TOKEN` assignment. Do not point it at another
project's config or runtime data.

After securely provisioning that local file, install and validate the task:

```bash
ops/launchd/install_coverage_sync_launch_agent.sh
launchctl print "gui/$(id -u)/com.ruok808.crypto-address-identity.coverage-sync"
```

The worker writes only its structured stdout/stderr to the ignored local file
`logs/coverage_sync_worker.log`. An execute failure leaves existing raw evidence
and resolver exports immutable; it is retried by the next daily schedule rather
than being hidden as negative identity evidence.

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
2. Stop a chain-universe read on schema drift, cutoff disagreement, an exceeded
   BigQuery cap, invalid row accounting, or checksum failure. Never retry a
   completed source job merely because local publication failed.
3. Use `cai audit coverage` to inspect safe outcome and evidence-tier counts.
4. Verify raw payload object hash and resolver snapshot manifest before drawing
   a conclusion from a label.
5. Keep unresolved conflicts as `ambiguous`; add a reviewed local override only
   when it selects or rejects existing evidence, never by patching exports or
   consumer rows manually.
6. Escalate a plan to promote a label into monitor or suppression behavior to a
   separately reviewed consumer policy change.
