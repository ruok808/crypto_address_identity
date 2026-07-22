# BTC-First Address Identity Development Specification

## Status

Proposed. This document narrows the multi-chain architecture in
`docs/designs/2026-07-22-multichain-address-identity-design.md` to the Bitcoin
implementation phase. It does not authorize a live provider sync, a production
consumer mutation, an alert-policy change, or a remote scheduler.

## Goal

Build a local, evidence-first Bitcoin address identity service. It ingests
0xRouter/Arkham observations and independently sourced evidence, resolves them
conservatively, and exports a versioned read-only view for the existing
`quant_crypto` BTC transfer and BTC whale products.

The system must answer, for one BTC address and one point in time:

1. What did each source assert about entity, label, or wallet role?
2. What independent evidence supports or conflicts with that assertion?
3. Is the result resolved, ambiguous, unattributed, stale, or unsupported?
4. May a consumer use it for display, lookup enrichment, or not at all?

## Scope

### Included

- Bitcoin mainnet Base58 and Bech32 address subjects only.
- Canonical SQLite evidence ledger and resolver database.
- Content-addressed raw 0xRouter response retention outside Git.
- Candidate queue, discovery/detail fetch policy, quota accounting, and safe
  fetch outcomes.
- Append-only imports of existing local labels and audited official/public
  evidence.
- Claim construction, conflict detection, resolver snapshots, and read-only
  exports.
- A versioned adapter contract for `btc_transfer_worker`, `btc_whale_worker`,
  and the BTC whale alert dispatcher.
- Dry-run and replay tools that do not alter a `quant_crypto` lake, manifest,
  cursor, threshold, or alert state.

### Excluded

- Ethereum, BSC, Solana, or Zcash provider sync, evidence import, or consumer
  integration.
- Provider-wide Arkham dumps, automatic clustering, or inferred ownership.
- A resident scheduler, remote service, automatic production fetch, or direct
  data_center publication.
- BTC alert suppression, delay, threshold changes, or monitor enrollment.
- Storage of tokens, authorization headers, private keys, signed requests, or
  consumer secrets.

The schema retains a multi-chain registry, but `bitcoin` is the only enabled
chain in this phase. Every other chain must return `unsupported_chain` before an
HTTP request is constructed.

## Core Decisions

| Decision | Specification |
| --- | --- |
| Canonical write store | SQLite with transactional migrations and one writer. |
| Analytics use | DuckDB may query exported snapshots later. It cannot write evidence, claims, or resolutions. |
| Raw payloads | Compressed, content-addressed files outside Git. The database stores only hashes and safe metadata. |
| Provider authority | 0xRouter/Arkham is Tier C seed evidence, never ownership truth by itself. |
| History | Observations and evidence are append-only. Claims and resolutions are versioned, never overwritten. |
| Resolver posture | Fail closed for identity promotion; fail open for consumer collection and alerting. |
| Consumer coupling | Explicit, versioned, read-only export. Consumers have no provider credentials or database write access. |
| Runtime cadence | Operator-invoked CLI only in this phase. |

## Runtime Configuration

All configuration comes from environment variables or an external secret
manager. No committed file contains a token.

| Setting | Default | Requirement |
| --- | --- | --- |
| `CAI_DATABASE_PATH` | `data/identity/address_identity.sqlite3` | Writable SQLite path. |
| `CAI_RAW_PAYLOAD_ROOT` | `data/raw/0xrouter` | Restricted, non-Git response storage. |
| `CAI_EXPORT_ROOT` | `data/exports` | Immutable resolver-snapshot root. |
| `CAI_ENABLED_CHAINS` | `bitcoin` | Only `bitcoin` is allowed in this phase. |
| `CAI_0XROUTER_BASE_URL` | `https://0xrouter.app` | HTTPS origin only. |
| `CAI_0XROUTER_TOKEN` | unset | Required only for a non-dry-run fetch. Never logged or persisted. |
| `CAI_0XROUTER_REQUESTS_PER_MINUTE` | `20` | Integer 1 through 30. The tenant ceiling is 30. |
| `CAI_0XROUTER_RESPONSE_BYTES_BUDGET` | `10485760` | Per-run received-response budget. |
| `CAI_HTTP_TIMEOUT_SECONDS` | `30` | Positive integer. |
| `CAI_DISCOVERY_TTL_HOURS` | `168` | Positive integer. |
| `CAI_DETAIL_TTL_HOURS` | `720` | Positive integer. |
| `CAI_MAX_DETAIL_CANDIDATES_PER_RUN` | `100` | Positive bounded count. |

Rate reservation is persisted in SQLite. A process-local limiter is not enough:
two concurrent processes must not together exceed the configured rate.

## Bitcoin Subject Contract

Accepted subjects are Bitcoin mainnet P2PKH (`1`), P2SH (`3`), and valid
Bech32/Bech32m SegWit (`bc1`) addresses. The normalizer checksum-validates each
address and rejects testnet, regtest, malformed, mixed-case Bech32, and
unsupported witness encodings. Valid Base58 spelling is preserved; Bech32 is
stored lower case.

```text
chain_key       = bitcoin
canonical_chain = bitcoin:mainnet
address_id      = sha256("bitcoin:" + normalized_address)
```

An address identifier establishes neither UTXO ownership, entity control,
wallet role, nor relation to a similarly named address.

## Storage Contract

SQLite uses foreign keys, WAL journal mode, a busy timeout, and checksummed
migrations. Resolver and export commands must open the database read-only when
they do not need a write transaction.

| Table | Contract |
| --- | --- |
| `schema_migration` | Migration id, checksum, and applied time. Changed historic checksums block startup. |
| `chain_registry` | Seeded `bitcoin` registry row with normalizer version and enabled flag. |
| `address_subject` | One immutable row per normalized BTC `address_id`. |
| `candidate_request` | Append-only candidate provenance: address, reason, priority, source reference, requested time. |
| `candidate_attempt` | Each selection, skip, reservation, and terminal candidate outcome. |
| `ingestion_run` | Mode, budgets, counters, timestamps, and terminal status for one CLI run. |
| `request_reservation` | Atomic rolling-window rate slot and HTTP dispatch result. |
| `source_observation` | Provider request or evidence import outcome with payload hash and schema fingerprint. |
| `raw_payload_object` | SHA-256, restricted relative path, compression, byte count, and retention status. |
| `identity_evidence` | Append-only source assertion for a subject. |
| `identity_claim` | Versioned compatible-evidence grouping; new facts supersede rather than update. |
| `conflict_set` / `conflict_member` | Stable conflict ids and append-only membership. |
| `identity_resolution` | Immutable materialized resolution revision. |
| `resolver_snapshot` | Export identity, manifest hash, as-of time, and record counts. |

All timestamps are UTC ISO 8601 strings ending in `Z`. Transfer amounts, UTXO
values, prices, and alert thresholds do not belong in this identity database.

### Observation Fields

```text
observation_id, source_id, source_version, source_kind, endpoint_template,
query_profile, requested_at, completed_at, http_status, outcome,
response_bytes, payload_sha256, schema_fingerprint, chain_key, address_id,
ingestion_run_id
```

`endpoint_template` records a route class rather than a secret-bearing raw URL.
`outcome` is `success`, `dry_run`, `http_error`, `transport_error`,
`rate_limited`, `budget_exhausted`, `malformed_payload`, `unsupported_chain`,
or `skipped_fresh`.

### Evidence Fields

```text
evidence_id, address_id, observation_id, assertion_type,
candidate_entity_id, candidate_entity_name, candidate_label,
candidate_wallet_role, provider_entity_id, provider_tag_id,
source_authority, evidence_tier, verification_method, source_url,
artifact_sha256, license_ref, independence_group, asserted_at, observed_at,
effective_from, effective_to, expires_at, evidence_status, imported_by
```

`assertion_type` is exactly `entity_control`, `address_label`, `wallet_role`,
`address_kind`, or `relationship`. Missing provider fields produce no negative
evidence and no `unknown` assertion.

| Tier | Source rule | Claim effect |
| --- | --- | --- |
| `A` | Cryptographic proof whose named verifier returns `valid`. | Eligible for reviewed entity-control evidence. |
| `B` | Official or regulator publication with immutable artifact reference. | Eligible for reviewed entity-control evidence. |
| `C` | 0xRouter/Arkham observation. | `unreviewed_external` only. |
| `D` | Reviewed explorer or public research. | Corroboration only. |
| `E` | Local heuristic. | Analytical context only. |

A Tier A import with an invalid or unsupported verifier outcome is rejected. A
source without license, source reference, verification method, independence
group, or effective-time boundary is rejected.

## Candidate Queue and 0xRouter Ingestion

### Candidate Handoff

Candidate imports use NDJSON. Each record has the following contract:

```json
{
  "chain_key": "bitcoin",
  "address": "<validated BTC address>",
  "reason": "whale_counterparty",
  "priority": 80,
  "source_reference": "external-event-id-or-audit-reference",
  "requested_at": "2026-07-22T00:00:00Z"
}
```

Allowed reasons are `known_watchlist`, `whale_counterparty`,
`transfer_counterparty`, `official_evidence`, `manual_review`, and `replay`.
Repeated candidates append provenance but do not multiply fetch work when an
address is fresh.

`quant_crypto` supplies a validated handoff file or invokes the candidate CLI.
It never writes identity tables or calls 0xRouter directly.

### Fetch Profiles

Discovery requests the 0xRouter `address_enriched/{address}/all` route with
tags disabled, entity predictions enabled, and clusters disabled. The parser
reads only the `bitcoin` branch and validates its echoed address.

Detail enables tags only when there is no detail observation, the discovery
payload changed, a high-priority reason is approved, active evidence is
contested or near expiry, or an operator requested review. `clusterIds`, if
returned, remain raw provider extensions and never create a relationship or
entity merge.

### Dispatch and Parsing

1. Validate and normalize before quota reservation.
2. Choose discovery or detail from TTL and policy.
3. Reserve a rolling-window slot and byte budget transactionally.
4. Send one HTTP request with the token held only in memory.
5. Persist the raw response by SHA-256 before claim parsing.
6. Persist observation and normalized Tier C evidence in one transaction.
7. Persist safe failure outcome on error. Never create negative identity
   evidence.

Do not retry HTTP 429 in the same run. A retry after transport failure, if
implemented, uses a bounded backoff and a new request reservation.

For a populated `bitcoin` response:

- `arkhamEntity.id` is `provider_entity_id`; its name is not a local entity id.
- `arkhamLabel.name` becomes an `address_label` candidate.
- Each `populatedTags` element becomes an `address_label` candidate with tag id
  and original label text.
- Free text such as hot, cold, deposit, or withdraw never becomes a
  `wallet_role` claim in this phase.
- `isUserAddress` is extension metadata, not ownership evidence.

Missing root address, root chain mismatch, or invalid required field shapes
produce `malformed_payload` and no evidence.

## Evidence, Claims, and Resolution

Existing `quant_crypto` labels are imported as evidence rather than copied over
or treated as automatic winners. Each import writes an `import` observation
before evidence rows. Entity control, address label, and wallet role remain
separate assertions.

The claim builder groups active evidence by subject, assertion type, and
canonical asserted value. It creates a revision with:

```text
claim_id, address_id, assertion_type, asserted_value, entity_id, claim_status,
evidence_strength, corroboration_count, independence_count, effective_from,
effective_to, reviewed_at, reviewer_ref, supersedes_claim_id
```

Tier C-only claims are always `unreviewed_external`. Tier A/B claims may become
`accepted` only with an explicit review record. The builder does not infer an
ownership probability.

Active claims for the same subject and assertion type conflict when their
canonical values or entities differ. The resolver creates or extends a
`conflict_set` and returns `ambiguous`, even if one source seems more plausible.
The sole exception is explicitly approved equivalent aliases with matching
independence and effective-time boundaries. This phase creates no aliases
automatically.

Resolver states are `resolved`, `ambiguous`, `unattributed`, `stale`, and
`unsupported`. Operational tiers are `none`, `discovery_only`, `lookup_only`,
and `lookup_usable`. It never emits `monitor_eligible` or
`suppression_eligible` during BTC-first implementation.

## Export and Consumer Contract

`cai export resolver --chain bitcoin --as-of <utc>` creates an immutable
directory:

```text
manifest.json
resolutions.ndjson
evidence_summary.ndjson
```

The manifest includes schema and resolver versions, as-of time, row counts, and
SHA-256 for every file. It contains no token, raw request, provider header, or
secret path. Consumers pin an explicit manifest hash, never a mutable latest
pointer.

Each resolution record includes:

```text
chain_key, normalized_address, address_id, assertion_type, state,
operational_tier, accepted_entity, entity_candidates, wallet_role_candidates,
conflict_set_id, evidence_summary, resolved_at, resolution_version,
freshness_status
```

The `quant_crypto` BTC adapter validates the manifest and hashes before building
a local lookup index. Its output fields are:

```text
identity_lookup_status, identity_state, identity_resolution_version,
identity_resolved_at, identity_entity_display, identity_wallet_role_display,
identity_operational_tier, identity_conflict_set_id
```

`identity_lookup_status` is `found`, `not_found`, `ambiguous`, `stale`,
`unsupported`, or `snapshot_invalid`. All except `found` are attribution
caveats only. They cannot block collection, quality gates, publishing, cursor
advancement, notification delivery, or existing ownership-semantics logic.

Historical raw lake rows are immutable. A later enrichment replay writes a
separately versioned derived audit output and never rewrites existing parquet.

## CLI

All commands support structured JSON output and redact secret-bearing exception
text.

| Command | Required behavior |
| --- | --- |
| `cai init-db` | Create/migrate schema and seed disabled non-BTC chain rows. |
| `cai candidates import --file <ndjson> [--dry-run]` | Validate and queue BTC candidates; dry-run writes nothing. |
| `cai fetch run [--dry-run] [--limit N]` | Apply TTL, quota, byte budget, and parse rules. Dry-run needs no token and writes no state. |
| `cai evidence import --file <ndjson> [--dry-run]` | Validate local, official, or provider evidence and Tier A verifier results. |
| `cai resolve rebuild [--as-of <utc>] [--dry-run]` | Create claim/resolution revisions without overwriting history. |
| `cai resolve show --chain bitcoin --address <address>` | Return redacted structured resolver result. |
| `cai export resolver --chain bitcoin --as-of <utc> [--dry-run]` | Produce checksummed immutable export. |
| `cai audit coverage --chain bitcoin --since <utc> --until <utc>` | Report candidates, observations, tiers, conflicts, and states. |
| `cai replay quant-crypto-btc --input <ndjson> --snapshot <manifest>` | Run read-only enrichment replay and report non-interference. |

No command starts a timer, changes a `quant_crypto` deployment, or contacts a
consumer service.

## Quality Gates and Safety Invariants

### Blocking State Mutation

- Invalid BTC address or non-BTC enabled-chain request.
- Migration checksum mismatch or failed foreign-key check.
- Provider chain/address mismatch or malformed successful payload.
- Missing mandatory evidence provenance or invalid evidence tier.
- Tier A evidence without a valid named verifier result.
- Manifest or export checksum failure.

### Recorded Non-Blocking Outcomes

- HTTP 429, timeout, DNS/transport error, or other provider non-success.
- No provider entity, label, or tag fields.
- Fresh candidate skip, quota exhaustion, or byte-budget exhaustion.
- Unreviewed, stale, or conflicting evidence.

These outcomes create audit evidence but never mean unknown ownership or a
negative label.

### Invariants

1. One HTTP dispatch creates at most one observation and raw object hash.
2. Reprocessing the same payload does not duplicate semantically identical
   evidence.
3. Evidence and resolution revisions never rewrite raw payloads, observations,
   or prior snapshots.
4. Resolver exports are valid only when all manifest checksums match.
5. Consumer lookup failure cannot alter a collector success/failure result.
6. Logs, JSON, exports, fixtures, and database rows contain no secret material.

## Test Requirements

### Unit Tests

- Base58 and Bech32 acceptance/rejection matrix.
- Non-BTC input rejected before provider dispatch.
- Shared rolling 20/minute reservation and exact byte-budget accounting.
- Discovery/detail selection by TTL, priority, conflict, and review conditions.
- Parser fixtures for populated response, no-label response, field drift, chain
  mismatch, and echoed-address mismatch.
- Provider labels/tags create Tier C evidence but not wallet-role claims.
- Tier A import rejects invalid and unsupported verifier results.
- Claim supersession, conflict set, and conservative ambiguous resolution.
- Snapshot manifest checksum validation and secret-redaction tests.

### Integration Tests

- Candidate import, fixture fetch, evidence import, resolution build, and export
  have deterministic database and manifest hashes.
- Equivalent repeated fetches remain idempotent at evidence semantics level.
- 429 and budget exhaustion record safe outcomes without negative claims.
- Conflicting local and provider labels remain ambiguous.
- BTC transfer/whale replay preserves event id, amount, direction, threshold,
  quality decision, and alert decision.
- A corrupt manifest yields `snapshot_invalid` caveats but replay completes.

## Acceptance Criteria for the Implementation Plan

- Only `bitcoin` may be fetched or persisted as provider identity data.
- Every source assertion has source, time, tier, verification, license, and
  independence provenance.
- Arkham-only labels stay `unreviewed_external` and cannot drive suppression.
- Conflicts remain auditable and resolve to `ambiguous`.
- Snapshot exports are checksum-verifiable and consumer-pinned.
- BTC replay is read-only and proves no business-decision change in existing
  BTC workers.
- No test, log, doc, fixture, export, or state record exposes a credential.
- No ETH, BSC, Solana, or Zcash address is fetched or persisted in this phase.

## Later Chain Gates

Ethereum, BSC, Solana, and Zcash require separate accepted addenda covering
normalization, provider capability, official evidence sources, address-kind
semantics, consumer mapping, fixture set, cost budget, and replay acceptance.
Their required implementation order is Ethereum, BSC, Solana, then Zcash.
