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
- Tier C: 0xRouter/Arkham source observation. It stays
  `unreviewed_external`.
- Tier D/E: public research or local heuristic corroboration/context.

Every evidence row preserves source URL, artifact hash when applicable, license,
independence group, timestamp, effective interval, and verification method.
Different entity assertions for the same address form a conflict set. The
resolver returns `ambiguous`; operators must not select a winner by source rank
alone.

## Snapshot Export and Consumer Replay

Build a resolver revision, then export a pinned snapshot:

```bash
PYTHONPATH=src python -m crypto_address_identity resolve rebuild --as-of 2026-07-22T00:00:00Z
PYTHONPATH=src python -m crypto_address_identity export resolver --chain bitcoin --as-of 2026-07-22T00:00:00Z
```

The export contains `manifest.json`, `resolutions.ndjson`, and
`evidence_summary.ndjson`. A consumer verifies every file checksum and pins a
manifest hash. It does not follow a mutable latest pointer.

The BTC replay adapter is read-only. It adds identity caveat fields but must not
change event ids, amounts, directions, thresholds, quality decisions, alert
decisions, or ownership-semantics decisions. A real `quant_crypto` integration
requires a separate implementation plan and approval.

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
4. Keep conflicts as `ambiguous`; do not patch historical exports or consumer
   rows manually.
5. Escalate a plan to promote a label into monitor or suppression behavior to a
   separately reviewed consumer policy change.
