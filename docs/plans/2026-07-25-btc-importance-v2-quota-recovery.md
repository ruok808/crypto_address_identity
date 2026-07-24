# BTC Importance V2 Quota-Recovery Plan

**Goal:** Recover the single aggregate-only BTC importance v2 census after the
original BigQuery Sandbox quota rejection without reusing or editing the
original authorization.

## Fixed Evidence

- [x] Preserve the failed authorization
  `btc-importance-v2-20260724-one-shot` and its mode-0600 receipt.
- [x] Pin the failed receipt SHA-256 to
  `80fe04a3ca6be426f4fbb1c2c5705674b54059589d49e91e731449afd771b661`.
- [x] Independently verify the failed cloud job has reason `quotaExceeded`,
  total processed bytes `0`, total billed bytes `0`, and no cache hit.
- [x] Confirm `cai-btc-universe-20260724` is linked to the active Billing
  Account with available promotional credit.

## Recovery Boundary

- [x] Add exactly one recovery authorization:
  `btc-importance-v2-20260724-quota-recovery-one-shot`.
- [x] Require the fixed prior receipt and cloud-job evidence in the request.
- [x] Validate the prior receipt checksum, mode, status, job id, query/schema
  checksums, byte cap, and no-retry state before any BigQuery network call.
- [x] Keep the query SQL, cutoff `959187`, source baseline `1,557,941,780`,
  input-only diagnostic `3`, and maximum billed bytes `650,000,000,000`
  unchanged.
- [x] Use a new deterministic job id and a new exclusive mode-0600 receipt.
- [x] Keep automatic retries, provider calls, candidate materialization, and
  consumer effects disabled.

## Verification And Execution

- [ ] Run focused and full tests, compile checks, diff checks, staged preflight,
  and an independent review.
- [ ] Merge the reviewed recovery implementation into exact `origin/main`.
- [ ] Run an offline recovery preview and prove it writes nothing.
- [ ] Run one fresh free BigQuery live dry-run and require the fixed hashes,
  exact `637,999,682,243` estimate, reviewed monthly usage, and no blockers.
- [ ] Execute the recovery authorization once with the
  `650,000,000,000`-byte cap and no retries.
- [ ] Validate the result is exactly one identifier-free aggregate row, quality
  allows interpretation, and no candidate artifact was materialized.
- [ ] Preserve both receipts permanently and decide candidate materialization
  only from the accepted aggregate counts.

## Stop Conditions

- Any drift in SQL, schema, cutoff, source baseline, dry-run bytes, monthly job
  count, monthly billed bytes, or prior evidence stops before execution.
- Any recovery receipt, including `started`, `failed`, or `quality_blocked`,
  permanently consumes the recovery authorization.
- There is no authorized third attempt.
