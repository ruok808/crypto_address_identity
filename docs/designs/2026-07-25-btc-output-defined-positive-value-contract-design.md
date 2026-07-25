# BTC Output-Defined And Positive-Value Population Contract

## Decision

Admit two explicit, checksum-pinned BTC address populations from the existing
July 24 aggregate receipts:

- `output_defined_standard_address_count`: a standard address has at least one
  eligible, value-cast-valid output at the fixed cutoff, including zero-value
  outputs.
- `positive_value_standard_address_count`: a standard address has at least one
  eligible transaction aggregate whose received value is greater than zero.

The positive-value population is the denominator for `btc_importance_v2`.
The output-defined population remains the completeness and reconciliation
population. Neither definition replaces the other.

This contract is an offline admission layer. It does not rerun BigQuery,
materialize addresses, call 0xRouter, or mutate either immutable source
receipt.

## Fixed Evidence

Both receipts use cutoff height `959187`, cutoff time
`2026-07-24T23:59:59.999999Z`, and schema SHA-256
`7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7`.

| Population | Receipt | Receipt SHA-256 | Query SHA-256 | Count |
| --- | --- | --- | --- | ---: |
| output-defined | `btc-candidate-statistics-20260724-v1.json` | `7a657f69f08c8ceb8756ed9e2e37d82b0bd007e843e2cd22a241bc8b9c7cf77b` | `5dbb2c914448837ac43b20e4943abb33130cf2ce9c1b7c2a72eb5ce4d285012c` | 1,557,941,780 |
| positive-value | `btc-importance-v2-20260724-quota-recovery-one-shot.json` | `c3123159ba77e0bcd5ba4735483027899bc451a50c8648784ec4317dfe20a236` | `47b0b8977cc1443578bc3daf3f90a2cf5e0e48ae758a4b7a133d3caa7d301e74` | 1,531,420,608 |

The derived output-defined but non-positive population is exactly
`26,521,172`.

The earlier address-scale result `1,557,951,354` belongs to a separate query
and execution receipt. It remains historical source-scale evidence only. It is
not one of the two admitted populations and must not be used as the v2 policy
denominator.

## Admission Rules

The offline validator must:

1. read only the two fixed receipt filenames;
2. require a regular file with mode `0600`;
3. enforce a 1 MiB maximum receipt size before JSON parsing;
4. match both receipt SHA-256 values exactly;
5. match cutoff, schema, query, execution, byte, provider, and no-materialization
   fields;
6. admit only the known source receipts and their exact known blocker sets;
7. require the v2 receipt's expected source count to equal the admitted
   output-defined count;
8. require
   `positive_value_standard_address_count <= output_defined_standard_address_count`;
9. derive the zero-value-only count by subtraction;
10. validate the Strict V2-S partition against the positive-value population.

An accepted dual-population contract does not rewrite the source receipts'
`quality_blocked` status. It only proves that their known blocker is explained
by an explicit semantic split and that no other blocker has appeared.

## Strict V2-S Capacity Gate

The admitted Strict V2-S aggregate has:

- P0: `21,736`;
- P1: `2,143`;
- edge-upgrade frontier: `133,730`;
- coarse union: `1,090,411`;
- excluded positive-value addresses: `1,530,330,197`.

Design eligibility requires:

- P0 `<=120,000`;
- coarse union `<=5,000,000`;
- edge frontier `<=1,000,000`;
- P0/P1 overlap is zero;
- P0 plus P1 is contained in the coarse union;
- coarse plus excluded equals the positive-value population.

Passing this gate permits a separate materialization design only.
`candidate_materialization_allowed` remains unconditionally false in this
contract.

## Public Interface

Offline preview:

```bash
cai universe validate btc-importance-v2-populations --dry-run
```

Pinned receipt validation:

```bash
cai universe validate btc-importance-v2-populations --execute-readonly
```

Both modes make zero network/provider requests and write no files. A blocked
result must expose machine-readable reasons and must not enable population
interpretation or materialization design.

## Non-Goals

- no BigQuery execution or retry;
- no address, transaction, or block identifier output;
- no candidate table or parquet creation;
- no provider enrichment;
- no consumer integration;
- no alert or suppression behavior change.
