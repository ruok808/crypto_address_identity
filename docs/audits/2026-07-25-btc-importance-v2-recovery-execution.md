# BTC Importance V2 Recovery Execution Audit

## Decision

Do not materialize candidate addresses from this receipt yet.

The fixed aggregate job completed once and stayed below its billing cap. Its
candidate-size metrics pass the approved capacity thresholds, but the receipt
correctly remains `quality_blocked` because the query and checkpoint used two
different source-population definitions.

No second billed query is needed to explain this mismatch. Amend the v2 source
contract to expose output-defined and positive-value populations separately,
then review whether the immutable result can be admitted for candidate sizing.

## Execution Evidence

- Code commit:
  `08a1211c75e2893183d41b120284bb3272a939d3`
- Authorization:
  `btc-importance-v2-20260724-quota-recovery-one-shot`
- Existing cloud job:
  `cai_btc_importance_v2_b2bf4b71772b68d2d2e7f7ec2303745a48d69d74`
- Query SHA-256:
  `47b0b8977cc1443578bc3daf3f90a2cf5e0e48ae758a4b7a133d3caa7d301e74`
- Schema SHA-256:
  `7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7`
- Result rows: `1`
- Actual processed bytes: `637,999,682,243`
- Actual billed bytes: `637,999,775,744`
- Maximum billed bytes: `650,000,000,000`
- Automatic retries: `0`
- Provider requests/points: `0/0`
- Candidate materialized: `false`
- Final receipt mode: `0600`
- Final receipt SHA-256:
  `c3123159ba77e0bcd5ba4735483027899bc451a50c8648784ec4317dfe20a236`

The original result-reading client stalled after the cloud job reached `DONE`.
It was stopped only after the no-resubmit reconciler had passed the full test
suite and entered `origin/main`. Reconciliation used `get_job` against the
same deterministic job id. Month-to-date successful billed jobs changed from
`5` to `6`, and billed bytes changed by exactly `637,999,775,744`, proving
there was one new successful billed execution rather than two.

## Quality Result

- Receipt status: `quality_blocked`
- Blocking reason:
  `candidate_statistics_v2_source_baseline_mismatch`
- Expected output-defined addresses: `1,557,941,780`
- Actual positive-value output addresses: `1,531,420,608`
- Difference: `26,521,172`
- Input-only diagnostic: `3`, matching the pinned warning-only value
- Null values: `0`
- Value-cast failures: `0`

The source baseline and v2 query are not measuring the same population:

1. The accepted v1/source baseline treats an address as output-defined when an
   eligible output row exists, regardless of output value.
2. The v2 query sets `has_output` with
   `COUNTIF(tx_received_sats > 0) > 0`.
3. Therefore the v2 population excludes addresses whose eligible outputs are
   zero-value only.

This source-code comparison fully explains the direction and semantics of the
mismatch. It does not justify editing the immutable receipt or silently
changing its quality status.

## Capacity Metrics

| Variant | P0 | P1 | Edge frontier | Coarse union |
| --- | ---: | ---: | ---: | ---: |
| Strict V2-S | 21,736 | 2,143 | 133,730 | 1,090,411 |
| Balanced V2-B | 22,138 | 2,082 | 133,389 | 1,090,411 |
| Retention V2-R | 21,804 | 2,159 | 133,681 | 1,090,411 |

Against the approved decision rules:

- strict P0 is below `120,000`;
- coarse union is below `5,000,000`;
- edge frontier is below `1,000,000`.

Zero-value-only addresses have no positive received, spent, UTXO, or receipt
magnitude. Under the fixed v2 score they can receive at most the recency
component, which is below P1, edge, and coarse thresholds. The capacity
metrics are therefore consistent with proceeding to candidate materialization
design after, not before, the population contract is corrected and reviewed.

## Required Next Change

1. Define two explicit metrics:
   `output_defined_standard_address_count` and
   `positive_value_standard_address_count`.
2. Keep the former tied to the exact same-cutoff v1/source receipt and use the
   latter as the economic-policy denominator.
3. Add a deterministic reconciliation gate for their difference instead of
   replacing one definition with the other.
4. Preserve the current execution receipt unchanged.
5. Do not rerun the approximately 638 GB census solely to recover counts already
   present in the immutable result.
6. Only after review, design address materialization for the strict V2-S
   coarse union, with P0 prioritized first.
