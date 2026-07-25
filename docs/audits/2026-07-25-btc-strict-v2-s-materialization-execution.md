# BTC Strict V2-S Materialization Execution Audit

## Decision

The BTC bootstrap phase freezes Strict V2-S as the current materialization
policy. This execution delivered the actual address list. A future policy
version is not a prerequisite for using or auditing this artifact.

## Authorized Contract

- Job ID:
  `cai_btc_v2s_1e506e3613154fba0ce0484b7acb67580f4e7587`
- Destination table:
  `cai-btc-universe-20260724.cai_private.btc_strict_v2_s_candidates_959187`
- Maximum bytes billed: `650,000,000,000`
- Expected source candidate subjects: `1,090,411`
- Query SHA-256:
  `5cb4990e01b4983910d0d813b67e148b985111108e6a26a251fadf95b18506d3`
- Result schema SHA-256:
  `ae5e08ff63b55f9bce3f5bbd17f858f2a29ec3da85223fd2f3c6675043883683`
- Source schema SHA-256:
  `7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7`
- Automatic query retries: `0`

## Execution Result

Exactly one paid source query completed under the authorized Job ID. No second
source scan was submitted.

- Status: `completed`
- Candidate materialized: `true`
- Source candidate subjects: `1,090,411`
- Preflight dry-run bytes: `638,140,048,581`
- Total bytes processed: `638,140,048,581`
- Total bytes billed: `638,140,284,928`
- Automatic retries: `0`
- Provider requests / points: `0 / 0`
- Execution receipt:
  `data/universe/executions/btc-v2s-bootstrap-959187-one-shot.json`
- Execution receipt SHA-256:
  `89db0c684df807f3c979e3b8b605b710ccab5959061ad6341d5356d4619b1240`

Two pre-query preparation failures did not submit the paid query. The first
blocked on dry-run estimate drift. The second found that the fixed private
destination dataset did not yet exist. Recovery created the private US dataset,
reran all fixed-contract gates, and used the same authorization and Job ID.

## Population Reconciliation

The BigQuery source field contains script subjects, not exclusively Bitcoin
addresses. The completed table contains 13 checksum-pinned non-address
subjects. They are excluded from the address artifact and retained in the
manifest audit.

| Population | Total | P0 | P1 | Edge | Coarse other |
|---|---:|---:|---:|---:|---:|
| Source candidate subjects | 1,090,411 | 21,736 | 2,143 | 133,730 | 932,802 |
| Excluded non-address subjects | 13 | 1 | 0 | 0 | 12 |
| Published valid addresses | 1,090,398 | 21,735 | 2,143 | 133,730 | 932,790 |

Exclusion reason:
`bigquery_nonstandard_script_subject`

Checksum-pinned exclusions:

| Subject SHA-256 | Tier |
|---|---|
| `032e9c22a9ec9d745f8a68f34b09e8829dd2f9f125957bab67b24ae0e0406130` | coarse_other |
| `3322674915fec459789a17c6319b1a792520c6457ca45492d6eb472924d31681` | coarse_other |
| `33ab7f786da0686675237bb637836dcb1e2f1cc393ac975a0b3d5af47fb4cd61` | p0 |
| `5a8fe3dca85ce9a3e62396763dbe9652e5ed470e9d3a01acacd7c87d83c44247` | coarse_other |
| `5bfefbdddf2df75f6c1ebf39018307da2018c8e8b18b4238011c613ee0438759` | coarse_other |
| `688f6fd58c4b144963080c0601cd5081d28b342305183c92a8ac7ba3e426a817` | coarse_other |
| `8370d0502790ac3612cba5e97dd318cb0c2f19d8673e7cc100937b1e68b63700` | coarse_other |
| `9d6a8731f49a8fe78a90c7d4998f022aa0eb32ec1095a1cbbe80e6915ee14c7f` | coarse_other |
| `aebc0175dd84568868e78ef08ca06116e3ac6b50e92e0f7328ffbf1576a037f4` | coarse_other |
| `ba910f56d01aa4388f58ad831aeafd1cef66eeb4d336456adb92a482ddf251c4` | coarse_other |
| `bbde01cfed08a454817783bf5754dcee268acc154163c05a64e2c444743b1e7d` | coarse_other |
| `d160286788996581958568c73a26ccfc92e0db94f155bb1a68d686db6f9351bd` | coarse_other |
| `f0eb840b34715d2e74083dec7597b2aaabd068515cf1dbe4b1f926a6b1bb42ca` | coarse_other |

The publisher accepts only this exact hash-to-tier set. Any new invalid
subject, missing exclusion, tier mismatch, malformed row, duplicate address,
or count drift blocks publication.

## Published Artifact

- Code commit: `f84ae3a`
- Campaign:
  `data/universe/campaigns/btc-v2s-bootstrap-959187`
- Manifest SHA-256:
  `a003ca6761f74bdafacac289b6c04d499e322e43e449e2d400bedfe2a353b5d4`
- Parquet files: `256`
- Manifest file records: `257`
- Approximate campaign size: `97 MiB`
- BigQuery Storage reads used for publication: `2`
- Additional BigQuery SQL queries: `0`

## Independent Verification

The final artifact was reopened independently and fully scanned.

- Manifest checksum: match
- File checksum failures: `0`
- Schema or partition failures: `0`
- Published rows: `1,090,398`
- Unique addresses: `1,090,398`
- Invalid or non-canonical Bitcoin mainnet addresses: `0`
- Duplicate address or row-checksum failures: `0`
- Execution receipt checksum: match
- Published tier counts: exact match

The unrelated untracked audit
`docs/audits/2026-07-23-btc-third-strong-evidence-search.md` was not staged,
modified, or included in this work.
