# BTC-First Address Identity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` or an equivalent task-by-task execution workflow. Keep the checkbox state current as each task is verified.

**Goal:** Implement the BTC-first address-identity service defined in `docs/specs/2026-07-22-btc-address-identity-development-spec.md`: a local evidence ledger, conservative resolver, immutable export, and read-only `quant_crypto` BTC replay adapter.

**Architecture:** Use SQLite as the canonical append-only observation/evidence store and resolver revision store. Keep raw 0xRouter payloads as content-addressed restricted files. Treat 0xRouter/Arkham data as Tier C evidence only. Export checksum-pinned snapshots for consumers; do not call a provider from a `quant_crypto` worker. Only Bitcoin is enabled. Ethereum, BSC, Solana, and Zcash remain disabled interfaces, not active ingestion paths.

**Tech stack:** Python 3.13, SQLite standard library, Pydantic settings/models, httpx, pytest, an audited BTC address-validation implementation or dependency, NDJSON, SHA-256, and optional DuckDB only for later read-only analysis.

## Guardrails

- [x] Work only in `/Users/barry/Documents/GitHub/crypto_address_identity`; preserve the existing design/spec documents and any unrelated changes.
- [x] Run global memory discovery before implementation, any real 0xRouter call, schema migration, or cross-project replay work.
- [x] Create an implementation branch before code changes. Stage only project files related to this plan.
- [x] Never commit or log `CAI_0XROUTER_TOKEN`, authorization headers, private keys, signed payloads, or secret-bearing URLs.
- [x] Do not modify `quant_crypto` source, runtime state, lake, manifest, cursor, thresholds, timers, or alert policies in this plan.
- [x] Do not enable Ethereum, BSC, Solana, or Zcash, even for a live provider probe.
- [x] Do not make a live or paid provider request until fixture tests, static checks, and the explicit live-fetch gate are accepted.

## Task 1: Bootstrap the Project and Test Harness

- [x] Add `pyproject.toml` for Python 3.13, package discovery, pytest settings, and the minimum runtime/test dependencies.
- [x] Add `src/crypto_address_identity/__init__.py`, package version metadata, and a `cai` module entry point.
- [x] Add `.gitignore` entries for `data/`, SQLite journal files, raw payloads, exports, local virtual environments, coverage output, and local secret files.
- [x] Add `conf/env/address_identity.env.example` containing non-secret defaults from the development specification. Keep `CAI_0XROUTER_TOKEN` documented as environment-only with no value.
- [x] Add a minimal README describing the BTC-only phase, local-only default posture, and the prohibition on direct consumer/provider coupling.
- [x] Add test fixture helpers that build deterministic temporary runtime roots and never rely on a real token or network.
- [x] Run the baseline test command and verify the new package imports with Python 3.13.

## Task 2: Implement Typed Configuration and BTC Chain Registry

- [x] Add Pydantic settings for every `CAI_*` field in the spec, including strict positive bounds and the hard upper bound of 30 requests/minute.
- [x] Make `bitcoin` the sole default enabled chain; reject all other values during config validation for this phase.
- [x] Add a static multi-chain registry model containing disabled placeholders for Ethereum, BSC, Solana, and Zcash, without provider clients or fetch paths for them.
- [x] Implement Bitcoin mainnet address normalization and `address_id` generation.
- [x] Use an audited validation dependency or a narrowly scoped, test-vector-backed implementation for Base58Check, Bech32, and Bech32m validation. Record the selected implementation and its test vectors in documentation.
- [x] Add tests for valid P2PKH, P2SH, Bech32, and Bech32m forms, plus invalid checksums, mixed-case Bech32, testnet, regtest, unsupported witness forms, and non-BTC chain rejection before dispatch.

## Task 3: Build SQLite Migrations and Transaction Boundaries

- [x] Implement a checksummed, ordered SQLite migration runner with a startup failure on a changed historical migration.
- [x] Create schema migrations for `schema_migration`, `chain_registry`, `address_subject`, `candidate_request`, `candidate_attempt`, `ingestion_run`, `request_reservation`, `source_observation`, `raw_payload_object`, `identity_evidence`, `identity_claim`, `conflict_set`, `conflict_member`, `identity_resolution`, and `resolver_snapshot`.
- [x] Enable foreign keys, WAL, a bounded busy timeout, and explicit transaction helpers.
- [x] Enforce immutability with inserts and superseding revisions rather than in-place evidence, claim, resolution, or observation updates.
- [x] Add idempotency indexes for canonical subjects, payload objects, semantically identical evidence, and export manifests.
- [x] Seed the Bitcoin registry as enabled and all non-BTC chains as disabled.
- [x] Add migration tests for fresh database creation, repeat startup, foreign-key enforcement, historical migration checksum mismatch, and read-only resolver connection behavior.

## Task 4: Add Candidate Queue, Run Ledger, and Quota Reservation

- [x] Define typed NDJSON candidate input with the allowed reason enum, bounded priority, source reference, and UTC requested time.
- [x] Implement candidate import that validates/normalizes BTC addresses, creates the subject if needed, and records every provenance request without duplicating fetch work.
- [x] Add `ingestion_run` lifecycle states for dry run, completed, partial, blocked, and failed runs with safe counters only.
- [x] Implement `candidate_attempt` records for selected, skipped-fresh, invalid, rate-limited, budget-exhausted, and completed outcomes.
- [x] Implement transactional rolling-window request reservations shared across processes through SQLite.
- [x] Implement per-run response-byte budget accounting that records actual received bytes and never treats an unknown response size as free.
- [x] Add tests for duplicate candidate provenance, freshness selection, priority ordering, concurrent-style reservation contention, exact 20/minute boundary behavior, and byte-budget exhaustion.

## Task 5: Add Restricted Content-Addressed Raw Payload Storage

- [x] Implement SHA-256 addressed payload paths below `CAI_RAW_PAYLOAD_ROOT`, with atomic write/rename and optional deterministic compression.
- [x] Persist only payload hash, safe relative path, compression, byte count, and retention metadata in SQLite.
- [x] Create a safe observation metadata builder that never accepts request headers, token values, or a raw secret-bearing URL.
- [x] Deduplicate identical raw payload objects by hash while retaining a separate observation for each attempted request.
- [x] Add tests for atomic persistence, duplicate object storage, missing object detection, safe path validation, and secret-redacted exception serialization.

## Task 6: Implement the 0xRouter BTC Provider Client and Parser

- [x] Implement an httpx provider client with HTTPS-origin validation, timeout, bounded retries for transport errors only, and no automatic same-run retry after HTTP 429.
- [x] Construct only the approved address-enriched route, passing the token in memory through the configured header without persisting it.
- [x] Implement discovery profile parameters: tags disabled, entity predictions enabled, clusters disabled.
- [x] Implement detail profile parameters: tags enabled only after policy selection; clusters remain disabled.
- [x] Parse only the `bitcoin` response root and validate the echoed address and chain identity after local normalization.
- [x] Convert `arkhamEntity`, `arkhamLabel`, and `populatedTags` into separate Tier C evidence candidates while preserving provider ids and original label text.
- [x] Store `isUserAddress` and unknown provider keys as raw extension metadata only; do not create ownership or wallet-role evidence from them.
- [x] Treat missing Bitcoin root address, wrong chain, or malformed required shapes as `malformed_payload` with no evidence insert.
- [x] Add fixture-only tests for populated results, empty labels, tag variants, schema drift, root mismatch, non-200, timeout, 429, and response-size accounting. Do not call the live provider in these tests.

## Task 7: Implement Fetch Orchestration and Safe CLI Output

- [x] Implement profile selection: discovery first; detail only for no prior detail, changed discovery hash, approved high-priority reason, contested/near-expiry evidence, or explicit review request.
- [x] Implement the fetch transaction sequence: validate, select, reserve, dispatch, raw persistence, parse, observation/evidence persistence, and terminal attempt result.
- [x] Make every non-success outcome auditable but non-negative: no labels/entities/tags, 429, network error, timeout, fresh skip, and budget exhaustion cannot create an unknown-owner claim.
- [x] Add `cai candidates import --file ... [--dry-run]` and `cai fetch run [--dry-run] [--limit N]`.
- [x] Return structured JSON containing status, run id, candidate counts, profile counts, request counts, byte counts, outcome counts, and written paths. Never include token values, headers, or raw payload bodies.
- [x] Prove dry-run validates inputs and selects work without writing database, raw-payload, evidence, or export state and without requiring a token.
- [x] Add CLI tests for exit codes, JSON shape, dry-run non-mutation, malformed input, token-missing execute mode, and secret redaction.

## Task 8: Implement Evidence Import and Proof-Verification Boundary

- [x] Define versioned NDJSON evidence-import records requiring source authority, tier, verification method, source URL, artifact hash, license reference, independence group, effective period, and importing actor.
- [x] Add importer support for existing local labels, official address lists, regulator/public evidence, and provider evidence. All sources create an immutable import observation first.
- [x] Define a pluggable proof-verifier interface returning `valid`, `invalid`, or `unsupported`, with a named verifier/version and audit output.
- [x] Gate Tier A evidence on a `valid` verifier result; reject invalid and unsupported Tier A inputs. Permit a reviewed lower-tier import only when its declared source contract permits it.
- [x] Keep `entity_control`, `address_label`, `wallet_role`, `address_kind`, and `relationship` as separate evidence assertions.
- [x] Add fixtures for signed-proof result states, official source metadata, legacy local labels, missing provenance, duplicate evidence, and expired evidence.

## Task 9: Implement Claims, Conflicts, and Conservative Resolution

- [x] Build claims from compatible active evidence grouped by subject, assertion type, and canonical asserted value.
- [x] Implement claim statuses: `unreviewed_external`, `accepted`, `contested`, `rejected`, `deprecated`, and `expired`.
- [x] Require an explicit review record before Tier A/B evidence can yield `accepted`; Tier C-only claims remain `unreviewed_external`.
- [x] Detect incompatible active entity or assertion values and create stable conflict sets with append-only member rows.
- [x] Resolve every active conflict as `ambiguous`, regardless of heuristic source ranking. Support only explicitly approved entity aliases; do not auto-create aliases.
- [x] Materialize resolution states `resolved`, `ambiguous`, `unattributed`, `stale`, and `unsupported`.
- [x] Restrict this phase's operational tiers to `none`, `discovery_only`, `lookup_only`, and `lookup_usable`; do not emit monitor or suppression eligibility.
- [x] Add deterministic tests for claim supersession, evidence expiry, same-value corroboration, conflict creation, non-winner selection, approved alias handling, and resolver reproducibility as of a supplied UTC time.

## Task 10: Export Checksum-Pinned Resolver Snapshots

- [x] Implement `cai export resolver --chain bitcoin --as-of ... [--dry-run]`.
- [x] Create immutable export directories containing `manifest.json`, `resolutions.ndjson`, and `evidence_summary.ndjson`.
- [x] Include schema/resolver version, UTC as-of time, row counts, and SHA-256 checksums in the manifest.
- [x] Record the export in `resolver_snapshot`; prevent accidental replacement of a path with a different manifest hash.
- [x] Implement a snapshot reader that validates every checksum before use and returns `snapshot_invalid` rather than raising secret-bearing errors to a consumer.
- [x] Add tests for deterministic ordering, manifest validation, corrupt/missing export files, concurrent export path collision, and no raw-payload or secret references in output.

## Task 11: Build the Read-Only `quant_crypto` BTC Adapter and Replay Tool

- [x] Add an adapter that loads a pinned, verified resolver snapshot into a local read-only lookup index.
- [x] Map lookup result to `identity_lookup_status`, `identity_state`, `identity_resolution_version`, `identity_resolved_at`, entity/role display fields, operational tier, and conflict id.
- [x] Keep ambiguous, stale, unsupported, not-found, and snapshot-invalid results as attribution caveats only.
- [x] Implement `cai replay quant-crypto-btc --input ... --snapshot ...` against documented NDJSON transfer/whale fixture contracts, without importing `quant_crypto` code or writing any lake data.
- [x] Assert that replay leaves event ids, amounts, directions, thresholds, quality decisions, alert decisions, and existing ownership-semantics decisions unchanged.
- [x] Emit a separately versioned audit result for replay, never a historical parquet rewrite.
- [x] Add tests for found, ambiguous, stale, not-found, snapshot-invalid, and conflicting-label enrichment plus explicit non-interference assertions.

## Task 12: Operational Documentation and Data Hygiene

- [x] Update README with local setup, configuration, fixture-first workflow, and the explicit no-live-fetch default.
- [x] Add `docs/btc_identity_operations.md` covering candidate intake, evidence imports, review decisions, snapshots, retention, backup, and incident triage.
- [x] Add `docs/btc_identity_evidence_format.md` with NDJSON examples containing only placeholder addresses and non-secret source references.
- [x] Document the 30 requests/minute tenant limit, 20/minute default, byte budgets, lack of global provider dump, and prohibition on interpreting empty data as unknown ownership.
- [x] Document that consumer integration is enrichment-only and must be separately accepted after replay.
- [x] Add a retention utility or documented procedure for raw objects that preserves hashes and observation auditability when content expires.

## Task 13: Verification and Independent Review

- [x] Run targeted tests for chain normalization, SQLite/storage, provider parser, queue/quota, evidence/resolver, export, CLI, and consumer replay.
- [x] Run the full test suite with `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider`.
- [x] Run Python compile checks for all package and CLI modules.
- [x] Run `git diff --check` and a repository secret-pattern scan that checks staged/new project files without printing matching secret values.
- [x] Review the diff specifically for: no non-BTC fetch paths, no live network calls in tests, no consumer write coupling, append-only behavior, explicit checksum validation, and the absence of token leaks.
- [x] Record exact test commands and results. If the full suite is time-boxed, report partial progress precisely instead of claiming a full pass.

## Task 14: Explicit Live-Fetch Gate and BTC Consumer Handoff

- [ ] Stop after local verification and independent review. Do not run a live provider call automatically.
- [ ] Present the bounded live-fetch proposal: candidate count, reason distribution, 20/minute ceiling, requested byte budget, retention location, and expected output paths, without exposing a token.
- [ ] After explicit approval only, run a small dry-run first, then one bounded BTC-only non-dry-run fetch.
- [ ] Audit observation outcomes, raw-object hashes, evidence tiers, conflicts, resolver snapshot checksum, and secret-redaction results.
- [ ] Perform a read-only `quant_crypto` BTC replay from a pinned snapshot and confirm no business-decision changes.
- [ ] Treat any actual `quant_crypto` code integration, deployment, or alert-policy change as a separate implementation plan and approval.

Local implementation intentionally stops at this gate. A real provider fetch, any
`quant_crypto` replay against production-shaped inputs, and all consumer rollout
work require a separate explicit approval.

## Test Commands

The exact package command may be finalized in Task 1, but the implementation
must support these checks:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/bitcoin tests/storage tests/providers
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/evidence tests/resolver tests/consumers
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider
python -m compileall -q src/crypto_address_identity
git diff --check
```

## Verification Record (2026-07-22)

- `PYTHONDONTWRITEBYTECODE=1 /Users/barry/.pyenv/versions/3.13.7/bin/python3 -m pytest -q -p no:cacheprovider tests/providers` -> `19 passed`.
- `PYTHONDONTWRITEBYTECODE=1 /Users/barry/.pyenv/versions/3.13.7/bin/python3 -m pytest -q -p no:cacheprovider` -> `80 passed`.
- `PYTHONPYCACHEPREFIX=/private/tmp/cai-pycache /Users/barry/.pyenv/versions/3.13.7/bin/python3 -m compileall -q src/crypto_address_identity` -> passed.
- `git diff --check` -> passed. The repository has no initial commit yet, so a separate repository-wide trailing-whitespace scan was also clean.
- Repository scans found no `TODO`/`FIXME` markers and no non-empty `CAI_0XROUTER_TOKEN` assignment. Scan output did not print any candidate secret values.
- No live 0xRouter request, paid request, raw provider payload, or `quant_crypto` mutation was performed.

## Acceptance Criteria

- BTC is the only enabled and fetchable provider chain.
- SQLite holds append-only observations/evidence and versioned claims/resolutions.
- Every identity assertion is source-attributed, time-bounded, license-aware,
  verification-aware, and assigned an independence group.
- Arkham-only evidence remains `unreviewed_external` and cannot affect alert
  suppression or monitor enrollment.
- Conflicts remain auditable and resolve conservatively to `ambiguous`.
- Every consumer snapshot is immutable and checksum-verifiable.
- The adapter and replay tool cannot mutate `quant_crypto` data or alter BTC
  worker business decisions.
- Tests and operational output contain no credential, request header, private
  key, signed request, or unredacted provider error containing a secret.
- No ETH, BSC, Solana, or Zcash sync is implemented or executed.
