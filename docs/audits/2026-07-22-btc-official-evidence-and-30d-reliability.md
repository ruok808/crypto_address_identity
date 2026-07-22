# BTC Official Evidence And 30-Day Provider Reliability Audit

## Decision

`0xRouter` / Arkham is suitable as a **Tier C candidate-entity seed** for the
BTC-first identity ledger. It is not a normalized wallet-role source and may
not by itself promote an address, alter an ownership decision, or suppress a
BTC whale alert.

The audit imported 50 independently verified OKX signed-PoR records as Tier A
evidence, then measured gateway output against that exact address set. A
separate, deterministic 30-day BTC whale sample measures operational coverage
only; it has no address-level ground truth and must not be read as precision.

## Official Signed Evidence Seed

The 2026-06-19 official [OKX PoR archive](https://static.okx.com/cdn/okx/por/chain/por_csv_2026061900_V3.zip)
was downloaded directly and recorded with ZIP SHA-256:

`1fd7764c9beafcf34141eaffd0352d1653040bb0cd30a1d8d1c2cd9ae408d139`

The public CSV mixes aggregate rows with detailed address-audit rows. The
importer therefore selects only BTC P2SH 2-of-3 rows containing the public
message `I am an OKX address`, two compact signatures, and a redeem script.
For each selected row it verifies all of the following locally:

1. `HASH160(redeem_script)` encodes to the disclosed P2SH address.
2. Both compact Bitcoin-message signatures recover compressed public keys.
3. Both distinct recovered keys occur in the published 2-of-3 redeem script.

The first deterministic 50 eligible rows all passed. Their source ZIP is held
only in the gitignored content-addressed raw store; the ledger retains the
public source URL, ZIP hash, verifier name, and `valid` result, not raw
signatures or messages.

## Gateway Ground Truth

The same 50 Tier A addresses were fetched through `address_enriched` using a
discovery-only audit profile at no more than 20 requests per minute.

| Metric | Result |
| --- | ---: |
| Official Tier A BTC addresses | 50 |
| Gateway HTTP successes | 50 |
| Gateway entity-name support | 46 / 50 (92.0%) |
| Entity matches where gateway named an entity | 46 / 46 (100.0%) |
| Entity conflicts | 0 / 46 |
| Primary `arkhamLabel` support | 46 / 50 (92.0%) |
| Supplementary `populatedTags` support | 20 / 50 (40.0%) |
| Formal `wallet_role` support | 0 / 50 |
| Referenced raw payloads with active metadata | 50 / 50 |

This is a selected official-reserve panel, not a representative sample of all
Bitcoin addresses. It supports preserving an Arkham entity name as an audit
candidate when present. It does not establish recall, cluster accuracy outside
the selected source class, entity ownership for unlabelled addresses, or a
wallet-role contract.

## Contested Labels

Two BTC labels remain contested between an older public explorer label and an
Arkham/0xRouter entity candidate:

| Address suffix | Explorer candidate | Gateway candidate | Official result | Decision |
| --- | --- | --- | --- | --- |
| `3MgEA...rP5pgd` | OKEx | Gemini | Not present in OKX 2026-06-19 or 2026-07-07 PoR | contested, lookup-only |
| `3FM9v...UxhbJ3` | OKEx | VanEck | Not present in OKX 2026-06-19 or 2026-07-07 PoR | contested, lookup-only |

The 2026-07-07 archive was independently integrity-checked against its current
official HTTP MD5 and stored under ZIP SHA-256
`cd007043bc9dc81acdf4457a98cdb8d4afd3821023702352824d30d550354dd2`.
It contains 3,329 signed BTC rows; neither disputed address appears in it.

Non-membership in a point-in-time PoR disclosure is not proof that OKX never
controlled an address. The reviewed Gemini and VanEck official materials did
not provide a direct address-level signed proof for either address. Therefore
none of the three entity claims is promotable from this audit. Future promotion
requires a direct signed ownership proof, an official disclosed address list,
or two independently operated address-level sources followed by review.

## 30-Day Historical Coverage Panel

The production BTC whale outbox was read only. The window contained 4,515
unsuppressed `internal_candidate` rows and 577 distinct output addresses. A
deterministic, stratified 60-address sample was exported into the ignored local
analysis directory. Its input SHA-256 was:

`0f0d08eceea9e22d2722f95c89f326ad4d8d1f28faeff521e626dad39182b7d0`

| Metric | Result |
| --- | ---: |
| Sampled distinct output addresses | 60 |
| Gateway HTTP successes | 60 / 60 |
| Entity-name support | 22 / 60 (36.7%) |
| Primary `arkhamLabel` support | 18 / 60 (30.0%) |
| Supplementary `populatedTags` support | 0 / 60 |
| Fully empty usable attribution | 35 / 60 (58.3%) |
| Formal `wallet_role` support | 0 / 60 |
| Officially comparable addresses | 0 |
| Raw payload references with active metadata | 60 / 60 |

This panel measures how often the provider can enrich the selected historical
outputs. Its entity precision is explicitly **not assessed** because no sampled
address overlaps independent Tier A truth. Empty output is not evidence of
unknown ownership. The panel also does not justify suppression, delayed email,
or a change to ownership semantics.

## Operational Note

The generic queue can intentionally upgrade high-priority official evidence
from discovery to detail. That is valid for normal enrichment but wasteful for
a bounded accuracy audit. The identity CLI now supports
`cai fetch run --profile discovery` for an explicit, freshness-respecting
discovery-only audit pass; it does not bypass request limits or provider
evidence tiering.

The resolver also treats source-specific entity IDs as evidence attributes, not
as a distinct entity when their normalized entity names agree. Provider tags
are retained as multi-valued Tier C evidence but do not compete with a primary
address label in the single-value resolver. The final checksum-pinned BTC
snapshot from this audit has manifest SHA-256
`877f6cfbb1b99ff461df0b34cecf950eb4feccf4c468094dcc02ce2b91a61bc9`;
all 132 retained raw payload objects verified `active`.

## Next Decision

Keep all Arkham/0xRouter outputs as append-only Tier C evidence. Use the
verified OKX seed as a continuing calibration panel. Before any consumer
integration, collect direct official evidence for disputed Gemini/VanEck claims
and obtain a second source class that can corroborate BTC entity assignments.
