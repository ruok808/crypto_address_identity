# BTC Candidate Statistics One-Shot Execution Audit

## Decision

Do not materialize BTC candidate addresses from this run.

The fixed `btc_importance_v1` census produced a P0 union above the approved
1,000,000-address stop line and a coarse union above the 5,000,000-address
target. The result also failed two source quality gates. The next implementation
phase is therefore a narrower `btc_importance_v2` aggregate design, not
candidate-address export or provider enrichment.

## Execution Contract

- Source revision: `9eda6d2c0530b81554e8cb1e6e9db8ca04c1c754`
- Authorization id: `btc-candidate-statistics-20260724-v1`
- Cutoff height: `959187`
- Cutoff time: `2026-07-24T23:59:59.999999Z`
- Query SHA-256:
  `5dbb2c914448837ac43b20e4943abb33130cf2ce9c1b7c2a72eb5ce4d285012c`
- Source schema SHA-256:
  `7353193a75b43704d273b8bcfc4a0d4d56fc9cdc6623704bb25855a0f439dfb7`
- Maximum bytes billed: `650,000,000,000`
- Expected source-address baseline: `1,557,951,354`
- Automatic query retries: `0`
- Execution calls: `1`
- Result rows: `1`
- Candidate materialization: `false`

Receipt SHA-256:
`7a657f69f08c8ceb8756ed9e2e37d82b0bd007e843e2cd22a241bc8b9c7cf77b`.
The local receipt is mode `0600`, contains no address or transaction identifier,
and is intentionally outside Git.

## Byte Accounting

- Pre-execution dry-run bytes: `637,999,682,243`
- Actual bytes processed: `637,999,682,243`
- Actual bytes billed: `637,999,775,744`
- Post-execution month-to-date billed bytes: `838,768,525,312`
- Post-execution Sandbox balance: `260,743,102,464`
- Successful billed query jobs after execution: `5`

A second invocation with the same authorization id was blocked locally. The
Jobs API totals remained unchanged, proving that it did not submit another
query.

## Aggregate Result

| Metric | Count |
|---|---:|
| Source standard addresses | 1,557,941,780 |
| Source input-only addresses | 3 |
| P0 union | 1,139,341 |
| P1 | 10,109 |
| Edge upgrade frontier | 131,826 |
| Coarse candidate union | 7,135,783 |
| Current capital cohort | 979,046 |
| Historical large-receipt cohort | 6,030,079 |
| High-turnover cohort | 169,232 |
| Dormant-holder cohort | 630,553 |

P0 is about `0.073131%` of source standard addresses. The coarse union is about
`0.458026%`.

The dominant P0 component is lifetime maximum same-transaction receipt of at
least 500 BTC: `1,121,720` addresses, or about `98.4534%` of the P0 union before
overlap reconciliation. The historical large-receipt cohort is about `84.5048%`
of the coarse union. These concentrations show that all-time receipt maxima are
too broad for direct high-priority enrichment.

## Quality Gate

The receipt status is `quality_blocked`:

- `candidate_statistics_source_baseline_mismatch`
- `candidate_statistics_input_only_addresses_present`

The actual source count is `9,574` below the pinned baseline, a difference of
about `0.000615%`. This is consistent with, but not proven solely by this
aggregate, the baseline having been measured through source height `959193`
while the candidate census was finalized at `959187`. A future baseline must be
computed at the same exact cutoff.

The three input-only subjects require aggregate-only source-semantics
classification before a later result can become publishable. Their identifiers
were not returned or stored.

## Next Design Constraints

1. Keep candidate materialization and graph expansion disabled.
2. Pin the source-address baseline to the exact census cutoff.
3. Preserve current UTXO and recent activity as strong importance evidence.
4. Demote an all-time `>=500 BTC` same-transaction receipt from unconditional
   P0 unless it also has recency, retained capital, repeated activity, or an
   independent identity/evidence anchor.
5. Re-estimate P0, P1, edge frontier, and coarse union with aggregate-only
   `btc_importance_v2` SQL before approving any address export.
6. Keep the existing one-shot receipt immutable; do not remove it to rerun the
   billed query.
