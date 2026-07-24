# BTC Importance V2 Existing-Job Reconciliation Plan

## Goal

Safely finish the fixed July 24 BTC importance v2 aggregate execution after
its BigQuery job completed but the original client stalled while reading the
result. The recovery must not submit, retry, or bill another query.

## Tasks

- [x] Preserve the existing `started` receipt and deterministic cloud job id.
- [x] Add a BigQuery adapter method that uses `get_job`, never `query`.
- [x] Bound result retrieval to two aggregate rows and a 120-second timeout.
- [x] Validate the full mode-`0600` started-receipt contract before network use.
- [x] Reuse the existing aggregate quality, checksum, source-count, byte, and
  billing-cap gates.
- [x] Leave the receipt unchanged when the existing result cannot be fetched.
- [x] Add a mutually exclusive `--reconcile-existing-job` CLI mode.
- [x] Add unit, adapter, and CLI tests proving no query submission.
- [x] Stop the stalled original client after code review and merge.
- [x] Reconcile the exact existing job and verify the final immutable receipt.
- [x] Keep materialization blocked pending a dual-population semantics fix; the
  returned P0, coarse-union, and edge-frontier counts pass their sizing limits.

## Fixed Boundaries

- Authorization:
  `btc-importance-v2-20260724-quota-recovery-one-shot`
- Job:
  `cai_btc_importance_v2_b2bf4b71772b68d2d2e7f7ec2303745a48d69d74`
- Query SHA-256:
  `47b0b8977cc1443578bc3daf3f90a2cf5e0e48ae758a4b7a133d3caa7d301e74`
- Schema SHA-256:
  `7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7`
- Expected processed bytes: `637999682243`
- Maximum billed bytes: `650000000000`
- Automatic retries: `0`
- Candidate materialization: disabled

The reconciled result is intentionally `quality_blocked` because the query's
positive-value address population does not equal the pinned output-defined
population. See
`docs/audits/2026-07-25-btc-importance-v2-recovery-execution.md`.
