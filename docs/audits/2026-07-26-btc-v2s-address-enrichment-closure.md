# BTC V2-S Address Enrichment Closure Audit

Date: 2026-07-26

Campaign: `btc-v2s-bootstrap-959187`

Cutoff height: `959187`

Status: **accepted for the approved urgent/P0/P1 closure scope**

## Scope

This audit closes the bounded BTC V2-S address enrichment campaign. It verifies:

- checksum-pinned candidate, coverage, and queue lineage;
- urgent, P0, P0 recovery, and P1 campaign accounting;
- deterministic retry handling without repeated paid requests;
- entity fanout 502 exhaustion;
- final coverage state and zero urgent/P0/P1 queue;
- no overlap with the resident coverage-sync LaunchAgent.

The report contains aggregate evidence only. It does not include addresses, entity
identifiers, raw provider payloads, tokens, or secret configuration.

## Pinned Inputs And Outputs

| Artifact | Identifier | Checksum |
| --- | --- | --- |
| Candidate manifest file | `btc-v2s-bootstrap-959187` | `ac61fe60646846b4bbdf4920000eba45ed416f6adb6806cf7cf51eb9d476dd5d` |
| Candidate semantic manifest | `btc-v2s-bootstrap-959187` | `a003ca6761f74bdafacac289b6c04d499e322e43e449e2d400bedfe2a353b5d4` |
| Final coverage snapshot | `20260726T050439Z-ac61fe606468` | `ed5ae7464eb5ae5dc4436892486cf33845a8c41c3dae7f74cf7e9966a25e7042` |
| Final coverage manifest file | `20260726T050439Z-ac61fe606468` | `32c6c73567031d326af72e0088793bf3d124267b18266447b84f432b43ed6fe4` |
| Final coverage parquet | `btc_v2s_coverage_state.parquet` | `36dbaddbd0abdc5346cbaeb6a2d3e238c6a2d5fcc24175a1db5baf8675ff3ac8` |
| Final queue | `20260726T050443Z-32c6c7356703` | `49b15ac7b4ec0a60b7284cd2eb2dea8c412e10e6247fc83d0d6f1ec48a13d4cb` |
| Final queue manifest file | `20260726T050443Z-32c6c7356703` | `ec6cc356d4f83c829bd476b52ffa08af177353314b3f05b740712201e819ebf7` |
| Final queue parquet | `btc_v2s_address_queue.parquet` | `03dc093a9fd54cc33caf29a6af7208a1b58aab38ca573a7b3523c1eb532d57ad` |

Both final manifests pass their semantic checksum validation. File size and SHA-256
checks pass, and the final queue pins both the candidate manifest and final
coverage manifest file checksums.

## Provider Accounting

| Workload | Observations | Successes | Failures | Estimated points | Response bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Address enrichment | 22,974 | 22,969 | 5 | 22,969 | 10,959,929 |
| Entity fanout | 288 | 174 | 114 | 293 | 5,112,258 |
| **Total** | **23,262** | **23,143** | **119** | **23,262** | **16,072,187** |

The five address-enrichment failures were transport errors during a local network
interruption. One additional P0 address had an attempt reservation without a
provider observation. The independent P0 recovery campaign therefore contained
exactly six addresses and completed 6/6 successfully with six estimated points.
No unresolved P0 recovery item remains.

Entity fanout produced 114 HTTP 502 observations across 77 unique entities,
including the previously approved one-time retry campaign. All 77 unique entities
have immutable `transient_retry_exhausted` records. There are zero unresolved
failed entities, and no further retry is planned.

Parser audit:

- address malformed payloads: `0`;
- entity prediction malformed payloads: `0`;
- entity membership rows: `1,165`;
- entity seeds: `230`.

## Campaign Closure

| Campaign | Attempted | Successful observations | Remaining planned |
| --- | ---: | ---: | ---: |
| Urgent | 105 | 105 | 0 in final queue |
| P0 primary | 20,936 | 20,930 | 0 |
| P0 one-time recovery | 6 | 6 | 0 |
| P1 | 1,928 | 1,928 | 0 |

No-call idempotence dry-runs were repeated for the P0 primary, P0 recovery, and P1
campaigns. Each returned:

- `status=dry_run`;
- `planned_addresses=0`;
- `requests=0`;
- `estimated_points=0`.

The P0 primary dry-run also accounted for two addresses newly covered outside its
attempt ledger. The P0 recovery dry-run classified the 20,930 primary successes
as terminal and left no recovery work.

## Final Coverage

The final coverage parquet contains exactly `1,090,398` rows and
`1,090,398` distinct normalized addresses. Invalid coverage states: `0`.

| Coverage state | Rows |
| --- | ---: |
| `direct_enriched` | 23,978 |
| `entity_membership_covered` | 464 |
| `local_evidence_covered` | 0 |
| `needs_direct_enrichment` | 1,065,956 |

Additional aggregate evidence:

- candidate-intersected provider prediction addresses: `477`;
- provider prediction addresses outside the candidate universe: `359`;
- active conflict intersection: `65`;
- explicit direct-requirement intersection: `130`;
- local-evidence intersection before precedence resolution: `7,630`.

The final address queue contains exactly zero rows, zero duplicate subjects, and
zero invalid cohorts:

- urgent: `0`;
- P0: `0`;
- P1: `0`.

`needs_direct_enrichment` is not a failed backlog for this closure. It consists of
candidate tiers outside the explicitly approved urgent/P0/P1 paid campaign. No
edge or `coarse_other` address was submitted by this supervisor.

## Runtime And Non-Overlap

The detached supervisor completed naturally and no supervisor process or screen
session remains.

The supervisor paused from local time `03:07:17` through `03:45:17`, covering the
resident LaunchAgent window. The LaunchAgent is currently not running and its
last exit was `EX_CONFIG (78)`. No overlapping provider campaign was observed.
The LaunchAgent configuration issue is outside this bootstrap closure and was not
changed here.

## Acceptance Decision

The urgent/P0/P1 address enrichment scope is complete and idempotent:

- final queue is empty;
- all recoverable address failures were recovered exactly once;
- all entity 502 failures are exhausted and will not be retried;
- checksum and row-level aggregate quality gates pass;
- no duplicate supervisor remains;
- provider calls are no longer required for this campaign.

Future enrichment of the remaining `1,065,956` candidates must use a new,
explicitly budgeted campaign and its own immutable ledger. It must not reuse or
mutate the closed campaign records documented here.
