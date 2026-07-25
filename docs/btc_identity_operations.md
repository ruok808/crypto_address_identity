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
5. **One-shot candidate-statistics execution.** Only after the fixed query,
   source schema, cutoff, source-address baseline, monthly usage, and reserve
   have been reviewed, use the separate `universe execute` command. It requires
   the exact 650 billion-byte cap and creates a mode-0600 exclusive receipt
   before submitting one deterministic BigQuery job. The SDK query and result
   calls both disable automatic retries. At most two rows are fetched; exactly
   one strict aggregate row is accepted. A started, failed, blocked, or
   completed receipt prevents a second submission with the same authorization
   id. This command never materializes candidate addresses.
6. **Bitcoin Core read-only probe.** Run only the four allow-listed RPCs:
   `getblockchaininfo`, `getblockhash`, `getblockheader`, and `getindexinfo`.
   Cookie content, authorization data, and raw RPC errors never enter output.
7. **Cutoff height/hash reconciliation.** Require the finalized BigQuery and
   Bitcoin Core height/hash to agree. A partial or unavailable Core source
   cannot establish a canonical complete-history cutoff.
8. **Review dry-run bytes.** Compare the reported bytes with the exact job cap,
   remaining account allowance, and local storage budget.
9. **Separately approved chain read.** Run one `--execute-chain-read` command
   with the reviewed query hash, cutoff, and positive
   `--maximum-bytes-billed`. Do not retry a completed job.
10. **Campaign checksum verification.** Verify the immutable manifest, Parquet
   schemas, source probes, and every recorded artifact checksum.
11. **Aggregate-only candidate dry-run.** Run `cai universe candidates` to
   inspect coverage, P0/P1/control counts, overlap, capacity, and projected
   time. It does not output addresses or open the identity SQLite database.
12. **Stop and report.** Phase 1 stops after aggregate statistics.
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

cai universe execute bigquery-candidate-statistics --dry-run \
  --authorization-id REVIEWED_ONE_SHOT_ID \
  --as-of-date 2026-07-24 --cutoff-height 959187 \
  --expected-query-sha256 5dbb2c914448837ac43b20e4943abb33130cf2ce9c1b7c2a72eb5ce4d285012c \
  --expected-schema-sha256 7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7 \
  --expected-source-address-count 1557951354 \
  --maximum-bytes-billed 650000000000 \
  --sandbox-budget-bytes 1099511627776 \
  --reserve-bytes 250000000000

cai universe execute bigquery-candidate-statistics --execute-once \
  --authorization-id REVIEWED_ONE_SHOT_ID \
  --as-of-date 2026-07-24 --cutoff-height 959187 \
  --expected-query-sha256 5dbb2c914448837ac43b20e4943abb33130cf2ce9c1b7c2a72eb5ce4d285012c \
  --expected-schema-sha256 7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7 \
  --expected-source-address-count 1557951354 \
  --maximum-bytes-billed 650000000000 \
  --sandbox-budget-bytes 1099511627776 \
  --reserve-bytes 250000000000

cai universe probe bitcoin-core --execute-readonly

cai universe build bigquery --execute-chain-read \
  --campaign-id CAMPAIGN --cutoff-height HEIGHT \
  --cutoff-time YYYY-MM-DDTHH:MM:SSZ \
  --maximum-bytes-billed REVIEWED_CHAIN_READ_CAP

cai universe candidates --campaign-id CAMPAIGN --dry-run \
  --runtime-minutes 480 --requests-per-minute 25
```

## BTC Importance V2 Cost-Only Probe

`btc_importance_v2` replaces the unconditional lifetime
same-transaction-receipt P0 rule with a strict 90-day supported-receipt rule.
The v2 implementation is version-isolated from the v1 SQL, one-shot executor,
and immutable receipt.

The v2 command has only two modes:

```bash
cai universe probe bigquery-candidate-statistics-v2 --dry-run \
  --as-of-date 2026-07-24 --cutoff-height 959187

cai universe probe bigquery-candidate-statistics-v2 --live-dry-run \
  --as-of-date 2026-07-24 --cutoff-height 959187 \
  --expected-query-sha256 REVIEWED_V2_QUERY_SHA256 \
  --sandbox-budget-bytes 1099511627776 \
  --reserve-bytes 250000000000
```

Offline `--dry-run` loads the fixed SQL and returns its checksum without
constructing a BigQuery backend. `--live-dry-run` performs exactly one
transaction-table metadata read, one current-month successful-job usage read,
and one free BigQuery dry run. Both modes report zero provider requests and
zero written paths.

The probe remains dry-run-only. A separate one-shot execution boundary exists
for the reviewed billed census:

```bash
cai universe execute bigquery-candidate-statistics-v2 --dry-run \
  --authorization-id btc-importance-v2-20260724-one-shot \
  --acknowledge-billed-execution \
  --as-of-date 2026-07-24 --cutoff-height 959187 \
  --expected-query-sha256 47b0b8977cc1443578bc3daf3f90a2cf5e0e48ae758a4b7a133d3caa7d301e74 \
  --expected-schema-sha256 7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7 \
  --expected-source-address-count 1557941780 \
  --expected-input-only-address-count 3 \
  --expected-dry-run-bytes 637999682243 \
  --expected-successful-query-jobs REVIEWED_JOB_COUNT \
  --expected-month-to-date-billed-bytes REVIEWED_BILLED_BYTES \
  --maximum-bytes-billed 650000000000 \
  --monthly-processing-budget-bytes 2000000000000 \
  --reserve-bytes 250000000000
```

Replace `--dry-run` with `--execute-once` only after a fresh live probe reports
the exact reviewed job count and month-to-date billed bytes. The request
rejects every other authorization id, cutoff, checksum, baseline, dry-run
estimate, query cap, monthly byte budget, or reserve. It creates an exclusive
mode-0600 receipt before query submission, uses a deterministic job id, and
sets both query and result retries to none. Any receipt permanently consumes
the authorization, including `started`, `failed`, or `quality_blocked`.

The only reviewed recovery from the July 24 Sandbox quota rejection uses a
separate authorization and job id. The original failed receipt is immutable
and must remain present with mode `0600` and SHA-256
`80fe04a3ca6be426f4fbb1c2c5705674b54059589d49e91e731449afd771b661`.
The recovery request also pins the original cloud job to
`quotaExceeded`, zero processed bytes, and zero billed bytes:

```bash
cai universe execute bigquery-candidate-statistics-v2 --dry-run \
  --authorization-id btc-importance-v2-20260724-quota-recovery-one-shot \
  --acknowledge-billed-execution \
  --as-of-date 2026-07-24 --cutoff-height 959187 \
  --expected-query-sha256 47b0b8977cc1443578bc3daf3f90a2cf5e0e48ae758a4b7a133d3caa7d301e74 \
  --expected-schema-sha256 7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7 \
  --expected-source-address-count 1557941780 \
  --expected-input-only-address-count 3 \
  --expected-dry-run-bytes 637999682243 \
  --expected-successful-query-jobs 5 \
  --expected-month-to-date-billed-bytes 838768525312 \
  --maximum-bytes-billed 650000000000 \
  --monthly-processing-budget-bytes 2000000000000 \
  --reserve-bytes 250000000000 \
  --recovery-from-authorization-id btc-importance-v2-20260724-one-shot \
  --expected-previous-receipt-sha256 80fe04a3ca6be426f4fbb1c2c5705674b54059589d49e91e731449afd771b661 \
  --expected-previous-job-id cai_btc_importance_v2_5bf66cb53c91d059f860e2c44865303383ba694d \
  --expected-previous-job-error-reason quotaExceeded \
  --expected-previous-job-total-bytes-processed 0 \
  --expected-previous-job-total-bytes-billed 0
```

The executor validates the prior receipt before any BigQuery metadata or query
request. Replace `--dry-run` with `--execute-once` only after independently
rechecking the immutable cloud-job evidence and a fresh live cost probe. The
recovery receipt also permanently consumes this second authorization; there is
no supported third attempt.

If the fixed cloud job reaches `DONE` but the original client stalls while
reading its result, do not submit another query and do not remove the
`started` receipt. After confirming the original client process has stopped,
replace `--dry-run` in the recovery command above with
`--reconcile-existing-job`. This mode:

- accepts only the pinned recovery authorization and deterministic recovery
  job id;
- requires the existing receipt to remain a regular mode-`0600` file with
  exact started-state contract fields;
- calls BigQuery `get_job` and reads at most two rows from that existing job;
- never calls BigQuery `query`, performs no retry, and applies the same result,
  checksum, source-count, freshness, processed-byte, and billing-cap gates;
- atomically changes the existing receipt to `completed` or
  `quality_blocked`; a failed result fetch leaves it unchanged and retryable.

This is result reconciliation, not a third execution authorization. It is
valid only for recovering the already submitted fixed job.

The one-shot command can return one identifier-free aggregate row only. It has
no destination table, address export, provider call, candidate materialization,
or consumer effect. A successful live dry run still proves only compilation
and estimated cost; the census counts exist only after the separately reviewed
one-shot execution.

The v2 aggregate result contract, if separately authorized later, is pinned to
cutoff height `959187`, standard output-address baseline `1,557,941,780`, and
known input-only diagnostic count `3`. The three input-only subjects remain
excluded from the output-defined population and produce a warning only when
the exact pinned count is preserved. Any count drift blocks interpretation.

The completed July 24 receipts are reconciled offline through an explicit
dual-population contract:

```bash
cai universe validate btc-importance-v2-populations --dry-run
cai universe validate btc-importance-v2-populations --execute-readonly
```

The validator admits `1,557,941,780` output-defined addresses as completeness
evidence and `1,531,420,608` positive-value addresses as the
`btc_importance_v2` economic-policy denominator. Their difference,
`26,521,172`, is the zero-value-only output-defined population. The older
`1,557,951,354` address-scale count is a separate historical query result and
must not be substituted for either admitted population.

`--execute-readonly` reads two exact mode-`0600` receipts, validates their
checksums and cross-references, and reports Strict V2-S capacity. It makes no
network or provider calls and writes nothing. Even an accepted result keeps
`candidate_materialization_allowed=false`; it permits a separately reviewed
materialization design only.

## Strict V2-S Candidate Materialization Cost Checkpoint

The Strict V2-S candidate query is a separate, address-level contract. It
reuses the fixed `btc_candidate_statistics_v2` policy CTEs and selects only the
`1,090,411` expected coarse-union addresses in deterministic P0, P1, edge, and
coarse-other tiers. Its result schema contains no transaction hash, block hash,
provider payload, or consumer decision.

Strict V2-S is frozen as the current BTC bootstrap materialization policy. It
does not require a V3 before addresses are delivered. Long-term policy review
happens only after the immutable address artifact exists and is audited.

The cost checkpoint remains the mandatory first boundary:

```bash
cai universe probe bigquery-strict-v2-s-materialization --dry-run

cai universe probe bigquery-strict-v2-s-materialization --live-dry-run \
  --expected-query-sha256 \
  5cb4990e01b4983910d0d813b67e148b985111108e6a26a251fadf95b18506d3 \
  --expected-result-schema-sha256 \
  ae5e08ff63b55f9bce3f5bbd17f858f2a29ec3da85223fd2f3c6675043883683 \
  --monthly-processing-budget-bytes 2400000000000 \
  --reserve-bytes 250000000000
```

The live checkpoint first validates the two immutable population receipts,
their exact `output_defined` and `positive_value` counts, and the expected
Strict V2-S tier counts. It then performs exactly three read-only BigQuery
operations: transaction-table metadata, current-month successful query usage,
and one dry run with no billable execution cap. Source schema or checksum
drift blocks before the cost estimate.

`checkpoint_passed` means only that the fixed query compiled, its estimated
bytes are at most `650,000,000,000`, and the operator-supplied monthly budget
still preserves its requested reserve after a hypothetical execution. Both
`dry_run` and `checkpoint_passed` keep
`candidate_materialization_allowed=false`, return zero candidate rows, make
zero provider calls, and write no path. Creating a destination table or
running the billed source scan requires a separate reviewed authorization.

After that authorization, the one-shot command must repeat every pinned value
from the accepted checkpoint. `--execute-once` creates one private expiring
destination table and one mode-`0600` receipt. It sets
`maximum_bytes_billed=650000000000`, uses `WRITE_EMPTY`, disables automatic
retry, and cannot submit a fallback query. Because BigQuery can vary the
physical dry-run estimate for an actively growing source partition, execution
allows at most `1,000,000,000` bytes of drift from the accepted baseline. The
actual preflight estimate is recorded in the receipt; query/schema drift,
larger byte drift, cap failure, or monthly reserve failure still blocks before
destination creation:

```bash
cai universe execute bigquery-strict-v2-s-materialization \
  --execute-once \
  --authorization-id btc-v2s-bootstrap-959187-one-shot \
  --acknowledge-billed-execution \
  --destination-table-id \
  cai-btc-universe-20260724.cai_private.btc_strict_v2_s_candidates_959187 \
  --expected-query-sha256 \
  5cb4990e01b4983910d0d813b67e148b985111108e6a26a251fadf95b18506d3 \
  --expected-result-schema-sha256 \
  ae5e08ff63b55f9bce3f5bbd17f858f2a29ec3da85223fd2f3c6675043883683 \
  --expected-source-schema-sha256 \
  7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7 \
  --expected-dry-run-bytes ACCEPTED_DRY_RUN_BYTES \
  --expected-successful-query-jobs ACCEPTED_SUCCESSFUL_QUERY_JOBS \
  --expected-month-to-date-billed-bytes ACCEPTED_MONTH_TO_DATE_BYTES \
  --maximum-bytes-billed 650000000000 \
  --monthly-processing-budget-bytes ACCEPTED_MONTHLY_BUDGET_BYTES \
  --reserve-bytes ACCEPTED_RESERVE_BYTES \
  --expected-candidate-rows 1090411 \
  --destination-expiration-hours 168
```

Use `--dry-run` with the same arguments to inspect the execution contract
without network or filesystem writes. The executor creates the fixed
`cai_private` dataset in `US` when absent and rejects any public access entry.
If destination preparation fails before query submission, preserve the sealed
receipt and rerun the same arguments with
`--resume-after-preparation-failure`; this is permitted only when the receipt
proves `execution_calls=0`. If submission outcome is unknown, use
`--reconcile-existing-job`. Never remove the receipt, change the deterministic
job id, or resubmit through a different command.

Once the receipt is `completed`, publication reads only that destination table,
recomputes every tier/mask/score/hash locally, rejects any duplicate or invalid
address, and atomically publishes deterministic Parquet partitions:

```bash
cai universe publish bigquery-strict-v2-s-candidates \
  --dry-run \
  --campaign-id btc-v2s-bootstrap-959187 \
  --destination-table-id \
  cai-btc-universe-20260724.cai_private.btc_strict_v2_s_candidates_959187 \
  --source-execution-receipt COMPLETED_RECEIPT_PATH \
  --expected-execution-receipt-sha256 COMPLETED_RECEIPT_SHA256 \
  --expected-result-schema-sha256 \
  ae5e08ff63b55f9bce3f5bbd17f858f2a29ec3da85223fd2f3c6675043883683
```

Replace `--dry-run` with `--publish-once` only after the completed receipt and
destination metadata pass review. Local extraction may be retried from the same
completed destination table before expiry; the source query may not.

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
does not by itself authorize executing the approximately 638 GB query.

The one-shot execution receipt is stored under
`CAI_UNIVERSE_ROOT/executions/<authorization-id>.json`. It contains only
checksums, aggregate counters, quality evidence, byte accounting, and execution
status. It never contains an address, transaction hash, provider token, raw
upstream error, or source payload. Removing or editing a receipt to force a
rerun is outside the supported workflow.
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
3. Enrich only due addresses from the explicit candidate queue or active
   conflicts. A prediction-only address with explicit entity membership is
   identity-covered and does not consume an `address_enriched` request.
   Address-level enrichment remains available for conflicts, wallet-role or tag
   requirements, and addresses with no explicit member relationship. It uses
   the live-validated BTC `address_enriched/{address}/all` profile with tags,
   predictions, and clusters enabled; response budgets and TTLs bound the
   richer payload.

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

### V2-S Entity Fanout Bootstrap

The V2-S bootstrap is a bounded, one-time entity-membership expansion. It reads
the checksum-verified Arkham canary, merges its provider entity IDs with the
local entity IDs, and deduplicates exactly. The cost probe and later batches use
only `entity_predictions/{entity}`. They never call discovery, entity detail,
or address enrichment, and each entity has at most one transport attempt in the
campaign. A campaign attempt is recorded even when the provider returns an
error, preventing silent retries and repeated point spend.

The route is not a complete entity-member export: the gateway contract exposes
no pagination parameters and returns at most about 1,000 addresses, commonly
ordered by USD balance across chains. Coverage is asserted only for explicit
BTC addresses present in the response. No unreturned address inherits entity
membership by inference.

Plan the first ten unique entities without network or database writes:

```bash
PYTHONPATH=src python -m crypto_address_identity coverage-sync entity-fanout \
  --canary-root data/canaries/btc-v2s-arkham-canary-v1 \
  --campaign-id btc-v2s-bootstrap-959187 \
  --request-limit 10 \
  --dry-run
```

After reviewing the plan and securely loading the token, run the ten-entity
probe. Later invocations automatically exclude terminal cached entities and all
entities already attempted by this campaign. `--request-limit` bounds entities
in the run, not requests per minute; even a larger bootstrap batch remains
paced by the shared 25/minute rolling ceiling:

```bash
PYTHONPATH=src python -m crypto_address_identity coverage-sync entity-fanout \
  --canary-root data/canaries/btc-v2s-arkham-canary-v1 \
  --campaign-id btc-v2s-bootstrap-959187 \
  --request-limit 10
```

After a healthy probe, use `--request-limit 500` to consume the remaining
deduplicated bootstrap entities in one paced run. Authentication, payment,
rate-limit responses, three consecutive provider failures, or the response byte
budget trip a fail-closed circuit breaker. No failed entity is automatically
retried inside the campaign.

Build the local intersection only after the entity fanout is complete:

```bash
PYTHONPATH=src python -m crypto_address_identity coverage-sync build-v2s-state \
  --candidate-campaign-root data/universe/campaigns/btc-v2s-bootstrap-959187 \
  --canary-root data/canaries/btc-v2s-arkham-canary-v1 \
  --output-root data/coverage/btc-v2s-bootstrap-959187
```

The checksum-pinned snapshot contains every V2-S candidate exactly once and
uses this precedence:

- `direct_enriched`: a validated address-level provider response exists.
- `needs_direct_enrichment`: an active conflict or explicit address-level
  request exists, even when entity membership is present.
- `entity_membership_covered`: one or more explicit provider entity memberships
  exist and no stronger direct requirement applies.
- `local_evidence_covered`: valid local entity-control evidence exists.
- `needs_direct_enrichment`: no explicit membership or usable local evidence
  exists.

The snapshot stores provider entity IDs only in the ignored local artifact.
CLI and audit output expose aggregate counts, checksums, and paths rather than
address or entity lists.

### V2-S Address Enrichment Closure

Repeated transient entity failures are not silently retried forever. After one
separately authorized retry campaign has completed, freeze an all-502 campaign
into the append-only exhaustion ledger:

```bash
PYTHONPATH=src python -m crypto_address_identity coverage-sync \
  finalize-entity-retries \
  --campaign-id btc-v2s-bootstrap-959187-transient-502-retry-20260725 \
  --dry-run
```

Review the aggregate count, then repeat without `--dry-run`. The command selects
only attempts recorded as HTTP 502; successful or differently failed entities
in a mixed campaign are reported as `non_502_attempts` and are never frozen.
Exhausted entity IDs are excluded from both the bootstrap fanout and the regular
coverage-sync entity queue. Reopening one requires a new reviewed policy;
changing or deleting the append-only rows is forbidden.

Build the direct-address queue from one exact coverage snapshot and the original
V2-S candidate campaign:

```bash
PYTHONPATH=src python -m crypto_address_identity coverage-sync \
  build-v2s-address-queue \
  --candidate-campaign-root data/universe/campaigns/btc-v2s-bootstrap-959187 \
  --coverage-snapshot-root data/coverage/btc-v2s-bootstrap-959187/<snapshot> \
  --output-root data/enrichment/btc-v2s-bootstrap-959187/queues
```

The local ignored queue contains only P0/P1 rows still marked
`needs_direct_enrichment`. Active conflicts and explicit address-level requests
form the `urgent` cohort. Remaining rows form `p0` and `p1`. Ordering is fixed by
cohort, V2 score, current UTXO value, lifetime received value, and canonical
address. The queue manifest pins both source manifests and its own Parquet
checksum.

Each live campaign is permanently bound to one queue manifest, one cohort, and
one point limit. The attempt reservation is written before the provider call,
so a process crash cannot cause the same campaign to pay for an address twice:

```bash
PYTHONPATH=src python -m crypto_address_identity coverage-sync \
  address-enrichment \
  --queue-root data/enrichment/btc-v2s-bootstrap-959187/queues/<queue> \
  --campaign-id btc-v2s-959187-urgent-20260725 \
  --cohort urgent \
  --request-limit 500 \
  --campaign-point-limit 500 \
  --dry-run
```

Securely load the token and repeat without `--dry-run` only after reviewing the
aggregate plan. The runner uses the validated
`address_enriched/{address}/all` profile, zero transport retries, the shared
25/minute ceiling, the 50 MiB run budget, a three-failure circuit breaker, and
content-addressed raw storage. It reports counts and checksums, never addresses
or provider payloads.

Successful responses append Tier C evidence through the existing evidence
service. Newly observed provider entity IDs become deduplicated entity seeds.
Run a new entity-fanout campaign once, rebuild the coverage snapshot and queue,
then continue with the remaining P0 cohort and finally P1. Newly covered
non-explicit addresses are skipped at dispatch time even if they remain in an
older queue; urgent conflict/explicit rows still require direct enrichment.

Do not query `edge` or `coarse_other` through this bootstrap command. They
require a separate marginal-yield decision after P0/P1 completes.

### Daily Local Incremental Task

The optional macOS LaunchAgent is for low-frequency maintenance after bootstrap;
it is not the mechanism used to enumerate the V2-S universe. It runs one
execute-mode coverage-sync batch each
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
