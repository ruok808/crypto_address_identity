# BTC Address Universe Bootstrap and Enrichment Campaign Design

## Status

Proposed design for review. This document replaces a seed-list-first bootstrap
with an address-universe-first campaign. It does not authorize a live provider
campaign, install a new scheduler, change a consumer, or promote a provider
label into an alert or suppression decision.

## Decision

Build the first BTC label library in two distinct planes:

1. A free or locally controlled **chain-universe plane** screens every
   provider-enrichable Bitcoin output script and computes address importance.
2. A quota-controlled **identity-enrichment plane** sends only selected,
   deduplicated addresses to 0xRouter/Arkham, expands returned entities and
   high-value graph neighbors, and repeats until the selected graph converges.

Existing `quant_crypto` addresses, official evidence, and local watchlists are
calibration anchors and mandatory-priority candidates. They are not the source
of the address universe and do not define the campaign's coverage boundary.

The existing daily `coverage-sync` LaunchAgent is a later maintenance task. It
must be paused before a bootstrap campaign so that it cannot compete for the
shared request window or create duplicate provider observations.

## Goal

Produce a reproducible, importance-weighted BTC address identity snapshot that:

- locally screens the broad Bitcoin address universe without paid lookups;
- selects economically important and structurally important addresses using a
  versioned, inspectable policy;
- spends provider points only once per address and query profile;
- expands newly identified entities and relevant chain neighbors without
  treating graph heuristics as ownership evidence;
- records every source, score, request, byte, point, expansion edge, conflict,
  and stop decision; and
- leaves a reusable universe/campaign framework for Ethereum, BSC, Solana, and
  Zcash while keeping their chain-specific identity semantics separate.

## Non-Goals

- Claiming that every historical Bitcoin output is an address or belongs to a
  legal entity.
- Claiming coverage of addresses that can be generated mathematically but have
  never appeared in the accepted chain. "BTC address universe" in this design
  always means subjects observed on-chain through the pinned cutoff.
- Sending every historical dust, one-time, or provably unspendable output to a
  paid provider.
- Treating common-input, change, cluster, or proximity heuristics as confirmed
  ownership.
- Treating a missing Arkham label as evidence that an address is unknown.
- Exhausting points for its own sake after the economically relevant queue has
  converged.
- Changing `quant_crypto` alert thresholds, ownership semantics, or suppression
  decisions as part of the campaign.

## Core Principle: Screen Everything, Enrich Selectively

"Every address is processed" means every standard provider-enrichable output
script in the accepted chain source receives a deterministic local screening
record. It does not mean every historical output gets a provider request.

The campaign maintains three scopes:

| Scope | Meaning | Paid request |
| --- | --- | --- |
| `script_universe` | Every unique output script seen in accepted chain data. | Never. |
| `address_universe` | Standard mainnet scripts that map unambiguously to one canonical address. | Never by default. |
| `enrichment_queue` | Deduplicated addresses selected by mandatory cohorts, importance score, or bounded expansion. | Yes, once per required profile. |

Nonstandard scripts, bare multisig, `OP_RETURN`, and outputs that map to more
than one address remain in `script_universe` for accounting but are never
silently converted into a provider address. Bare P2PK scripts retain the public
key/script identity; the system does not invent a P2PKH address for them.

## Source Strategy

### Source A: Local Bitcoin Core, Canonical Long-Term Source

The canonical long-term path is a locally controlled archival Bitcoin Core node
plus a deterministic sequential block parser:

- pin a finalization height and hash;
- read blocks in height order;
- parse every transaction output and input prevout;
- derive standard script/address types locally;
- maintain an append-only block manifest and reorg rollback boundary; and
- independently materialize the current UTXO set.

Bitcoin Core exposes `getblock` with full transaction data and supports
`dumptxoutset` for a serialized UTXO snapshot. `dumptxoutset` alone is not the
historical address universe: it covers current unspent outputs, so historical
high-flow and now-empty addresses still require block history.

Official references:

- https://bitcoincore.org/en/doc/31.0.0/rpc/
- https://bitcoincore.org/en/doc/26.0.0/rpc/blockchain/dumptxoutset/

This path has no per-query data fee, but it has real disk, bandwidth, initial
sync, and operational costs. It is therefore the authority and incremental
maintenance source, not necessarily the fastest first-day bootstrap source.

### Source B: BigQuery `crypto_bitcoin`, Preferred Fast Bootstrap

The preferred first-day bootstrap is the public
`bigquery-public-data.crypto_bitcoin` dataset when a read-only query project is
available and the source passes freshness, schema, and dry-run cost gates.

The official dataset exposes Bitcoin-like `inputs` and `outputs` tables and was
designed for address-level value-flow analysis. BigQuery's on-demand free tier
currently includes the first 1 TiB of query data processed per month, but that
allowance is account-wide and subject to change. Every production query must
therefore use a dry run and `maximum_bytes_billed`; the design never assumes a
query is free merely because the dataset is public.

Official references:

- https://cloud.google.com/blog/products/data-analytics/introducing-six-new-cryptocurrencies-in-bigquery-public-datasets-and-how-to-analyze-them
- https://cloud.google.com/bigquery/pricing

Bootstrap acceptance gates:

1. Table schema is captured and checksum-pinned in the campaign manifest.
2. Maximum block time/height is fresh enough for the declared campaign cutoff.
3. The cutoff block hash matches an independent Bitcoin Core or explorer
   checkpoint.
4. Dry-run bytes are below the campaign's account-safe limit.
5. The query selects only required columns and uses partitions where available.
6. Exported candidate rows have deterministic checksums and row counts.

If any gate fails, the source is not used for that campaign.

BigQuery is accepted only for the fields its probed schema actually exposes.
If the public table has normalized address arrays but not raw `scriptPubKey`
bytes, it may bootstrap `address_universe` and candidate features, but it
cannot satisfy the script-level completeness claim. `script_universe`
completeness remains pending until Bitcoin Core or another independently
accepted raw-script source has replayed the same cutoff.

### Source C: Bulk Dumps, Conditional Fallback

Blockchair publishes daily Bitcoin blocks, transactions, inputs, outputs, and
addresses dumps intended for bulk analytics. It may be used only after the
exact files, access terms, license, coverage, checksums, and update semantics
are validated. It is not assumed to be free or canonical by this design.

Reference: https://blockchair.com/dumps

### Sources Explicitly Rejected for Universe Enumeration

- Per-address explorer APIs: they require knowing the address first.
- Esplora block-by-block public endpoints: useful for bounded validation, not a
  respectful or reliable full-chain bootstrap.
- Existing system watchlists: useful anchors, not a universe.
- Screenshots or notification text: useful incident clues, not reproducible
  source data.
- Provider labels themselves: identity evidence, not chain facts.

## Canonical Chain-Universe Model

Large chain-derived data is stored as partitioned Parquet and queried through
DuckDB. The existing SQLite database remains the evidence/resolver store for
selected addresses. Millions of scripts and chain aggregates must not be
inserted into the evidence database.

### Canonical Identities

```text
script_id  = sha256("bitcoin:mainnet\0" || raw_script_pubkey_bytes)
address_id = sha256("bitcoin:" || normalized_mainnet_address)
output_id  = sha256(block_hash || tx_position || vout_position)
```

`output_id` does not rely only on `txid:vout`, because historical duplicate
transaction identifiers exist. The canonical output lineage includes the
accepted block hash and transaction position.

Base58 addresses remain case-sensitive. Bech32/Bech32m addresses are validated
and stored lowercase. The script-to-address mapping is one-to-zero-or-one;
ambiguous or multi-address source rows never duplicate an output value across
several addresses.

### Parquet/DuckDB Tables

`universe.btc_block_manifest`

```text
source_revision, block_height, block_hash, previous_hash, block_time,
accepted_chain, finalized_at, extracted_at, payload_checksum
```

`universe.btc_output_fact`

```text
output_id, block_height, block_hash, txid, tx_position, vout_position,
value_sats, script_id, script_type, normalized_address, address_id,
provider_enrichable, spent_height, spent_txid
```

`universe.btc_address_feature`

```text
feature_version, cutoff_height, address_id, normalized_address, address_type,
first_seen_height, last_seen_height, last_spent_height,
output_count, spent_output_count, transaction_count,
current_utxo_sats, lifetime_received_sats, lifetime_spent_sats,
max_single_output_sats, max_same_tx_received_sats,
inflow_30d_sats, outflow_30d_sats, gross_flow_30d_sats,
inflow_90d_sats, outflow_90d_sats, gross_flow_90d_sats,
gross_flow_365d_sats, direct_large_counterparty_count,
screened_at, source_manifest_sha256
```

`universe.btc_candidate`

```text
campaign_id, address_id, normalized_address, priority_class,
importance_score, first_selected_wave, current_status,
provider_profile_required, selection_policy_version
```

`universe.btc_candidate_reason`

```text
campaign_id, address_id, reason_code, reason_value, source_kind,
source_reference, parent_address_id, parent_entity_id, created_at
```

`universe.btc_expansion_edge`

```text
campaign_id, from_subject_type, from_subject_id, to_address_id,
expansion_method, confidence_class, transaction_value_sats,
provider_observation_id, source_reference, created_at
```

The campaign ledger and provider observations remain append-only. Candidate
status may be materialized as a current view over immutable status events.

## First-Pass Universe Construction

### Free Bootstrap Query Plan

The fast path must not run a separate full-history query for every cohort.
After a schema probe and dry run, it materializes one compact, campaign-scoped
flow relation from the minimum required input/output columns:

```text
cutoff_height, block_height, block_hash, block_time, txid, tx_position,
io_kind, io_position, value_sats, addresses, script_type
```

Only rows whose address field maps unambiguously to one validated Bitcoin
mainnet address enter address aggregates. Empty, multi-address, malformed, and
nonstandard rows remain in source-accounting counts and never have their value
copied into several addresses.

The materialized relation is reused to derive:

1. lifetime received, spent, current balance, and first/last activity;
2. per-address/per-transaction receipts and maximum same-transaction receipt;
3. exact 30/90/365-day flow windows; and
4. the bounded transaction neighborhood for selected large-value addresses.

The full-history source is scanned once per accepted schema/cutoff. Rolling
queries use date or block partitions. Destination tables are clustered by
canonical address when the source engine supports it. Every query is dry-run
first, has `maximum_bytes_billed`, writes to a campaign-specific immutable
destination, and records source bytes, result rows, and checksums. Query-cache
hits are welcome but are never assumed in the budget.

### Pass 1: Current Capital

Index every standard address represented in the finalized UTXO set. Compute
current balance from actual unspent outputs rather than `received - sent`
shortcuts. This catches cold storage and dormant reserves even when no recent
activity exists.

### Pass 2: Historical Large Value

Scan all historical outputs and calculate both:

- maximum individual output value; and
- maximum value paid to the same address within one transaction.

The second metric preserves the existing BTC whale semantics and catches a
large payment split across several outputs to the same script.

### Pass 3: Rolling Activity

For finalized blocks in the previous 30, 90, and 365 days, compute address-level
inflow, outflow, gross flow, transaction count, and recency. This catches hot
wallets, deposit/withdrawal infrastructure, OTC routing, and high-turnover
addresses whose current balance is small.

### Pass 4: Structural Importance

Create candidate-only graph features:

- counterparties in transactions with at least 100 BTC of same-address net
  value;
- counterparties of mandatory-cohort addresses where the edge carries at least
  10 BTC;
- fan-in/fan-out and consolidation behavior;
- repeated interaction with independently tagged entities; and
- provider-returned entity prediction addresses.

Graph features prioritize provider lookups. They do not assert common
ownership.

## Smart First Provider List

The first paid list is a deterministic union over free chain features, not a
copy of local watchlists and not a single "rich list".

### Eligibility

An address is eligible only when all are true:

- it is a canonical, provider-enrichable Bitcoin mainnet address;
- its chain facts are within the accepted source cutoff;
- it is not provably unspendable or an ambiguous multi-address script;
- no successful fresh request exists for the same address/profile/schema; and
- it has at least one mandatory, scored, calibration, or expansion reason.

### Wave Zero: Capability and Bias Controls

Before the 1,000-address canary, query 10 fixed capability addresses spanning
legacy, SegWit, Taproot, known labeled, known unlabeled, and existing-conflict
cases. The canary then reserves 2% for a deterministic low-score control sample.
That sample estimates selection bias and the label yield outside the high-value
queue; it cannot displace mandatory P0 work beyond its reserved share.

### First Production Wave

After the canary:

```text
first_wave_limit = min(
  due_unique_addresses,
  floor(discovery_point_budget / canary_p95_points_per_address),
  floor(25 * approved_runtime_minutes)
)
```

For an eight-hour segment the rate ceiling makes this at most 12,000 addresses.
The queue fills in this order:

1. Existing conflicts and exact Tier A/B evidence addresses.
2. All feasible P0 addresses, ordered within each cohort by economic magnitude
   and then canonical `address_id`.
3. P1 addresses through diversity quotas:
   - 30% current-capital leaders;
   - 25% historical large-receipt leaders;
   - 20% 30/90/365-day high-turnover addresses;
   - 10% dormant large holders;
   - 10% high-value graph connectors and recent unknown whale counterparties;
   - 5% calibration and deterministic control samples.

One address occupies one slot even if it belongs to every cohort. Unused quota
is reassigned to the largest remaining cohort only after every other cohort has
been considered. If P0 exceeds one segment, the remainder stays P0 and is
processed before P1 in the next segment; it is never silently discarded.

This list is "smart" because the free chain pass considers every observed
provider-enrichable address, while the paid list preserves economic size,
activity, dormancy, topology, and source-calibration diversity.

## Mandatory Cohorts and Importance Score

Weighted scores alone can hide an important address, so deterministic cohorts
are selected before scoring.

### Mandatory P0 Cohorts

An address enters P0 if any condition is true:

| Code | Condition |
| --- | --- |
| `utxo_ge_100_btc` | Current finalized UTXO balance is at least 100 BTC. |
| `same_tx_receive_ge_500_btc` | Any transaction paid at least 500 BTC in aggregate to the address. |
| `gross_90d_ge_1000_btc` | Finalized 90-day gross flow is at least 1,000 BTC. |
| `lifetime_ge_10000_active_365d` | Lifetime received is at least 10,000 BTC and the address was active in 365 days. |
| `official_or_signed_evidence` | Existing Tier A/B evidence names the exact address. |
| `existing_provider_conflict` | Existing active identity evidence is contested and requires resolution. |

The first four are chain-universe cohorts. The last two ensure evidence
continuity but do not define universe coverage.

### P1 Scored Cohort

All remaining provider-enrichable addresses receive an integer score:

| Feature | Points |
| --- | --- |
| Current balance >= 1,000 / 100 / 10 / 1 BTC | 25 / 20 / 12 / 5 |
| Max same-transaction receipt >= 5,000 / 1,000 / 500 / 100 BTC | 25 / 20 / 18 / 10 |
| 90-day gross flow >= 10,000 / 1,000 / 100 / 10 BTC | 20 / 15 / 8 / 3 |
| Last activity <= 30 / 90 / 365 days | 10 / 7 / 3 |
| At least one >=100 BTC edge to a selected address | 10 |
| Provider entity prediction | 15 |
| Exact existing system/watchlist address | 10 |

For each feature family only the highest matching bucket applies. The initial
P1 threshold is `importance_score >= 25`. It is versioned as
`btc_importance_v1`; changing a threshold creates a new policy version and
requires a replay, not an in-place reinterpretation.

### Diversity Quotas

The queue is a union, not a single balance ranking. Before provider dispatch it
reserves capacity for:

- current-balance leaders;
- historical maximum-transfer leaders;
- 30/90/365-day high-turnover addresses;
- high-degree/high-value graph connectors;
- dormant large holders;
- recent unknown whale counterparties; and
- official/provider calibration samples.

No cohort may consume more than 40% of a wave unless every other mandatory
cohort is exhausted. This prevents exchanges from crowding out funds, miners,
custodians, OTC routes, and dormant holders.

## Exact Deduplication Rules

### Chain Deduplication

- One output fact per `output_id`.
- One script subject per raw `scriptPubKey` hash.
- One address subject per validated canonical address.
- Multiple candidate reasons are retained as separate reason rows but point to
  one candidate.
- A multi-address or unparseable script never replicates value into several
  address aggregates.

### Provider Deduplication

```text
request_key = sha256(
  provider_id || chain_key || address_id || query_profile || provider_schema_version
)
```

During one bootstrap campaign, a successful `request_key` is never dispatched
twice. Prior fresh successful observations are reused. HTTP errors remain
retryable under a separate attempt ledger; they do not create a negative label.

Entity detail and prediction calls are keyed once per provider entity and
profile, not once per address that resolves to that entity. Ten enriched
addresses returning the same entity therefore schedule at most one due entity
detail request and one due prediction request. Predicted addresses still need
their own direct address lookup before their label can become Tier C evidence.

Entity detail and prediction calls use the equivalent key with the provider
entity ID. Raw response payloads remain content-addressed by SHA-256, so equal
payloads occupy one physical blob while each request retains its own immutable
observation lineage.

### Concurrency Deduplication

Before dispatch, a transaction atomically changes one candidate from `queued`
to `leased` with a lease expiry. Unique request keys prevent two workers from
spending points on the same address. Expired leases may be reclaimed only when
no completed request exists.

## Provider Query Profiles

Before the campaign, run a 10-address capability probe covering labeled,
unlabeled, conflict, legacy, SegWit, and Taproot addresses.

Candidate profiles:

1. `btc_identity_discovery_v1`: request entity and address-label fields with
   tags/clusters disabled if the gateway proves that response shape is stable.
2. `btc_identity_detail_v1`: request tags, predictions, and clusters only for a
   positive entity/label hit, contested address, P0 address, or explicit audit
   sample.

If the lean profile does not reliably return entity and label fields for BTC,
the validated full `/all` profile remains the only profile. The design does not
assume unsupported query combinations.

Every response records:

```text
http_status, response_bytes, points=max(1, ceil(response_bytes / 100000)),
payload_sha256, schema_fingerprint, parsed_outcome, retry_class
```

The configured gateway limit is 30 requests/minute. Campaign dispatch defaults
to 25/minute with one SQLite-backed global rolling window shared by address,
entity, and prediction calls. No process-local limiter may bypass it.

## Canary and Budget Calculation

The account currently has 249,598 points with a 30-day validity window. The
campaign does not guess an address capacity from that number.

### Stratified Canary

Run exactly 1,000 unique addresses, stratified across the mandatory and scored
cohorts. Record:

- response-byte p50/p90/p95/max;
- points-per-address p50/p90/p95;
- entity, label, tag, and empty-field hit rates;
- schema failures, 4xx, 5xx, and retry rates;
- conflict rate against existing evidence; and
- predicted-address yield per positive entity.

Canary hard cap: 5,000 points. Stop immediately on credential errors, schema
drift, a rolling-limit violation, or projected cap breach.

### BTC Budget

After the canary:

```text
usable_points       = current_points - cross_chain_reserve - retry_reserve
safe_address_count  = floor(discovery_budget / canary_p95_points_per_address)
time_address_count  = floor(25 * available_minutes)
wave_address_count  = min(queue_size, safe_address_count, time_address_count)
```

Initial defaults:

- cross-chain reserve: at least 50% of the point balance;
- retry/detail reserve: 10% of the BTC allocation;
- BTC bootstrap hard cap: 100,000 points;
- one execution window: at most 8 hours; and
- address dispatch: at most 25/minute.

At 25/minute, 1,000 lookups require at least 40 minutes, 10,000 require about
6 hours 40 minutes, 20,000 require about 13 hours 20 minutes, and 36,000 require
24 hours. The rate limit, not CPU, prevents a claim that an arbitrary queue can
always finish in a few hours.

Bootstrap is therefore a continuous, checkpointed campaign rather than a
once-daily drip feed. An execution segment may run for up to eight hours, commit
its immutable ledger, pass quality review, and resume immediately in the next
approved segment. With one-point responses, 36,000 addresses/day is a hard
theoretical maximum at 25/minute; 100,000 addresses need at least 66 hours 40
minutes. Larger responses consume the point budget sooner. The first 10,000
high-priority addresses can still close in one working-day window.

## Expansion Loop

Each wave executes these steps:

1. Lease the highest-priority deduplicated addresses within cohort quotas.
2. Run the required provider profile exactly once.
3. Append observations and evidence; rebuild no resolver yet.
4. Collect new provider entity IDs and prediction addresses.
5. Add high-value local graph neighbors with explicit heuristic provenance.
6. Canonicalize and deduplicate every new address.
7. Recompute only affected candidate scores.
8. Start the next wave if mandatory work or qualified expansion remains.

### Allowed Expansion Methods

| Method | Queue effect | Identity effect |
| --- | --- | --- |
| Provider entity prediction | Enqueue valid BTC addresses. | Tier C provider evidence only after direct address lookup. |
| Same transaction >=100 BTC edge | Enqueue important counterparty. | None. |
| Common-input heuristic | Enqueue only if non-CoinJoin gates pass. | Local Tier E context only. |
| Change/peel-chain heuristic | Enqueue when value and recurrence gates pass. | Local Tier E context only. |
| Existing Tier A/B exact address | Force P0 lookup/audit. | Existing evidence remains authoritative for its scope. |

Common-input expansion is disabled for likely CoinJoin/PayJoin or ambiguous
multi-party transactions. Conservative exclusion signals include many inputs
and outputs, repeated equal-value outputs, no dominant value path, and known
mixing patterns. Exclusion from expansion is not an assertion that a
transaction is a CoinJoin.

Provider-returned cluster IDs are recorded only if present. They do not expand
addresses unless a separately validated endpoint returns explicit members with
stable provenance.

## Convergence and Stop Conditions

Mandatory P0 candidates are never skipped because of a low average yield. The
campaign stops at the first applicable hard condition:

1. All P0 and selected P1 candidates are terminal and no qualified expansion
   remains.
2. The BTC point cap is reached or the next reserved request would exceed it.
3. The wall-clock window is reached.
4. Provider authentication, schema, or rate-limit quality gates block the run.
5. Chain-source integrity or finalized-tip invariants drift.

For optional P2 graph expansion, convergence is declared only when two
consecutive waves both have:

- fewer than 0.5% new unique qualified addresses relative to addresses queried;
- fewer than 0.2% new positive entity/label hits; and
- no new mandatory P0 address.

The campaign may resume from its immutable ledger without repeating a provider
request.

## Chain and Data Quality Gates

Blocking chain gates:

- source coverage cannot be tied to a finalized height/hash;
- block continuity or previous-hash validation fails;
- duplicate output IDs occur within the accepted chain;
- negative or non-integer satoshi values occur;
- transaction output totals violate valid transaction structure;
- a standard-address row fails local checksum validation;
- a multi-address source row would double count output value;
- source manifest or export checksum changes after acceptance; or
- reorg depth crosses the campaign's finalized boundary.

Blocking provider gates:

- missing or invalid token;
- response schema cannot be associated with the requested BTC address;
- address echo or chain key mismatches;
- request rate exceeds 25 in any rolling minute;
- reserved or actual points exceed the campaign cap;
- the same successful request key is dispatched twice; or
- raw payload hash cannot be verified.

Warnings:

- source freshness is within the accepted bootstrap window but not real-time;
- an address maps to conflicting provider and local evidence;
- a heuristic expansion is CoinJoin/change ambiguous;
- a response is structurally valid but has no entity/label; and
- optional detail fields are absent.

## Source Reconciliation

For the first bootstrap, BigQuery and Bitcoin Core have different roles:

- BigQuery can generate the first shortlist quickly.
- Bitcoin Core validates a deterministic sample, current UTXO facts, the cutoff
  block, and later becomes the incremental authority.

At minimum, reconcile 1,000 stratified addresses and 100 large transactions
across sources. Required exact matches are address/script parsing, satoshi
values, block height/hash, and same-address transaction aggregates. Any
systematic mismatch blocks provider enrichment.

## Execution Sequence

### Phase 0: Freeze and Preflight

1. Pause the daily coverage-sync LaunchAgent after confirming it is inactive.
2. Snapshot the current point balance without printing the token.
3. Pin the current identity database, raw store, and resolver snapshot hashes.
4. Create a campaign ID and immutable policy/source manifest.

### Phase 1: Source Probe

1. Probe BigQuery schema, freshness, and dry-run bytes.
2. Probe Bitcoin Core availability and finalized tip.
3. Choose and record the accepted bootstrap and validation sources.
4. Abort if no source meets integrity and cost gates.

### Phase 2: Universe Build

1. Build the script universe through the cutoff height.
2. Materialize current UTXO, historical large-value, rolling activity, and
   structural feature tables.
3. Mark every standard address with `screened_at`, feature version, and source
   manifest hash.
4. Publish cohort counts and data-quality results before any paid call.

### Phase 3: Candidate Build

1. Materialize all mandatory P0 cohorts.
2. Score the remaining address universe.
3. Form the diversity-quota union and exact deduplication ledger.
4. Import existing evidence/watchlists only as priority/calibration reasons.

### Phase 4: Provider Canary

1. Run the 10-address profile capability probe.
2. Run the 1,000-address stratified canary under the 5,000-point cap.
3. Compute p95 cost and select the first production wave size.
4. Review schema, conflicts, and hit rates before continuing.

### Phase 5: BTC Bootstrap Campaign

1. Dispatch P0, then P1, under cohort quotas.
2. Append entity/prediction and local graph expansion candidates.
3. Repeat waves until convergence or a hard stop.
4. Do not rebuild a consumer snapshot during active waves.

### Phase 6: Resolution and Audit

1. Rebuild claims/conflicts once from the completed evidence ledger.
2. Export a checksum-pinned resolver snapshot.
3. Report source coverage, screened address count, selected/query counts,
   cohort coverage, provider hit/conflict rates, exact points, expansion yield,
   duplicate requests prevented, and unresolved queues.
4. Run read-only consumer replay; require zero unapproved alert/action changes.

### Phase 7: Maintenance Handoff

1. Configure Bitcoin Core or the accepted chain source for incremental blocks.
2. Resume a low-frequency maintenance task only after its discovery and fanout
   profiles are separated from the bootstrap campaign.
3. Re-run the same canary and budget calculation before a new provider snapshot
   or schema version is activated.

## Multi-Chain Reuse

The reusable campaign interfaces are:

```text
ChainUniverseAdapter
  -> source_manifest()
  -> enumerate_subjects(cutoff)
  -> compute_features(cutoff)
  -> canonicalize_subject()
  -> validate_finality()

ImportancePolicy
  -> mandatory_cohorts()
  -> score()
  -> diversity_cohort()

ProviderCampaign
  -> capability_probe()
  -> discover()
  -> detail()
  -> expand()
  -> meter_points()
```

BTC uses output scripts and UTXO/value-flow features. Ethereum and BSC use
accounts/contracts/token-transfer activity; Solana separates wallet, program,
mint, and token accounts; Zcash includes transparent addresses only. Shared
campaign mechanics do not imply shared address or ownership semantics.

## Acceptance Criteria

- Every standard address in the accepted BTC universe has one deterministic
  screening record for the campaign cutoff.
- No output value is double counted through multi-address source fields.
- Every selected address has all selection reasons and source provenance.
- No successful provider request key is dispatched twice.
- The global rolling rate never exceeds 25/minute.
- Point accounting exactly follows `max(1, ceil(response_bytes / 100000))` and
  never exceeds the approved cap.
- Provider failures do not become negative evidence.
- Expansion heuristics never become ownership claims.
- The final resolver remains `provider_default + local_override +
  conflict_first` and exposes all conflicts.
- Consumer replay reports zero alert, threshold, cursor, or suppression changes
  unless separately reviewed.
- The remaining point balance and the reserved capacity for later chains are
  reported explicitly.

## Implementation Boundary

Implementation should be a new reviewed plan and must begin with source probes,
fixtures, and tests. It should not simply increase the frequency of the existing
`coverage-sync run`, because that command performs discovery on every invocation
and was designed as a bounded maintenance batch rather than a convergent
universe bootstrap campaign.
