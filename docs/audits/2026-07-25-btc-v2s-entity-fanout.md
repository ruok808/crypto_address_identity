# BTC V2-S Entity Fanout And Coverage Audit

Date: 2026-07-25

## Scope

This audit covers the first cost-pinned entity fanout over the immutable BTC
Strict V2-S candidate artifact. It uses Arkham/0xRouter entity predictions only
as an identity-membership source. It does not enumerate or disclose entity IDs,
addresses, raw provider payloads, or credentials.

The execution made no discovery, entity-detail, or address-enrichment requests.
Provider failures were not retried in the campaign and were not interpreted as
negative identity evidence.

The available `entity_predictions/{entity}` contract has no pagination
parameters and returns at most about 1,000 addresses, commonly ordered by USD
balance across chains. This audit therefore treats only explicit returned BTC
addresses as members. It does not claim complete entity-cluster enumeration.

## Pinned Inputs

- V2-S campaign: `btc-v2s-bootstrap-959187`
- V2-S candidate rows: `1,090,398`
- V2-S source manifest file SHA-256:
  `ac61fe60646846b4bbdf4920000eba45ed416f6adb6806cf7cf51eb9d476dd5d`
- V2-S declared manifest SHA-256:
  `a003ca6761f74bdafacac289b6c04d499e322e43e449e2d400bedfe2a353b5d4`
- Canary: `btc-v2s-arkham-canary-v1`
- Canary ledger SHA-256:
  `53c4d1aecc258e8ee249d55f12ecbc05ac1df7222b88b6e2798bdd8074602561`

## Entity Census And Cost

| Metric | Result |
|---|---:|
| Canary unique provider entities | 124 |
| Existing local unique provider entities | 49 |
| Exact-deduplicated merged entities | 151 |
| Previously terminal-cached entities | 6 |
| Campaign-attempted entities | 145 |
| Duplicate entity requests in campaign | 0 |
| HTTP successes | 108 |
| HTTP 502 responses | 37 |
| Parsed entities with BTC members | 19 |
| Successful entities with no BTC members | 89 |
| Unique provider membership addresses inserted | 477 |
| Response bytes | 3,657,727 |
| Estimated 0xRouter points | 148 |

The 37 errors were structured gateway `origin_bad_gateway` responses marked
retryable with a retry-after hint. The cost-pinned campaign deliberately did
not retry them. They remain source-unavailable observations, not empty entity
membership results. All 151 merged entities are accounted for by the six prior
terminal cache entries plus 145 unique campaign attempts.

## Local V2-S Intersection

Snapshot:
`20260725T085857Z-ac61fe606468`

| Coverage metric | Result |
|---|---:|
| Candidate rows | 1,090,398 |
| Unique candidate addresses | 1,090,398 |
| Direct-enriched state | 1,009 |
| Entity-membership-covered state | 459 |
| Local-evidence-covered state | 0 |
| Needs-direct-enrichment state | 1,088,930 |
| All prediction addresses intersecting V2-S | 467 |
| Prediction addresses outside V2-S | 339 |
| Active conflicts intersecting V2-S | 65 |
| Explicit address requirements intersecting V2-S | 130 |
| Local entity evidence intersecting V2-S | 92 |

Eight prediction-member addresses are already direct-enriched, leaving 459
addresses that can skip address-level enrichment when only entity identity is
required.

The zero `local_evidence_covered` state does not mean local evidence was lost.
All 92 matching local-evidence addresses were selected by a higher-precedence
state: 23 were already direct-enriched and 69 retained an explicit/conflict
direct-enrichment requirement.

`needs_direct_enrichment` is a coverage classification, not an automatic
provider queue. Of those rows, 1,088,825 have neither an active conflict nor an
explicit address-level request. The coverage worker does not enqueue this full
set. It selects only explicit candidate requests and active conflicts.

## Integrity Assertions

- Snapshot row count equals the source manifest row count.
- Snapshot distinct-address count equals its row count.
- Snapshot manifest self-hash is valid.
- Snapshot Parquet SHA-256 matches the manifest.
- Unknown coverage states: `0`.
- Missing coverage reason codes: `0`.
- Membership state without membership evidence: `0`.
- Membership addresses returned to direct enrichment without an active conflict
  or explicit requirement: `0`.

Published local artifact checksums:

- Coverage Parquet SHA-256:
  `8ad6c0526e9a784adac54ad1bbdccc93eed4ce6b6581237b26d434fbd8167d36`
- Coverage manifest semantic SHA-256:
  `a143047d42c5ac9300a644e8db4258fe7a5a244457f2dcc3d4f89f0d7424e093`

The runtime Parquet, manifest, SQLite state, and raw provider responses remain
outside Git.

## Decision

The fanout path is useful but bounded: it prevents 459 unnecessary direct
enrichment calls in the current V2-S universe and preserves 467 explicit local
membership intersections. It does not turn a small seed set into broad Arkham
coverage because the available entity-prediction route returned BTC members for
only 19 successfully queried entities and cannot be paginated.

Address-level enrichment remains appropriate only for active conflicts,
explicit label or wallet-role requirements, and selected high-value addresses
without explicit membership. The 37 transient provider failures may be retried
only through a separately named, separately bounded campaign; they must not be
silently retried by this completed campaign.
