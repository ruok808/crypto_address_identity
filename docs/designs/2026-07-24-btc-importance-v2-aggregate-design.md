# BTC Importance V2 Aggregate-Only Design

Date: 2026-07-24 UTC

## Status

Proposed for review.

This document defines `btc_importance_v2` and the aggregate-only census needed
to size it. It does not authorize another billed BigQuery execution, export
addresses, materialize a candidate table, call an identity provider, or change
any consumer.

## Decision Summary

`btc_importance_v1` made any lifetime same-transaction receipt of at least
500 BTC an unconditional P0 reason. The one-shot census at cutoff height
`959187` showed that this rule is too broad:

| V1 metric | Count |
| --- | ---: |
| Source standard addresses | 1,557,941,780 |
| P0 union | 1,139,341 |
| P0 same-transaction receipt >=500 BTC | 1,121,720 |
| P0 addresses supported only by that receipt rule | 1,115,247 |
| P0 union after removing that one rule | 24,094 |
| Historical receipt >=100 BTC cohort | 6,030,079 |
| Coarse candidate union | 7,135,783 |

The same-transaction receipt condition accounts for about `98.45%` of the P0
union. A lifetime maximum is evidence that an address once carried value, but
it is not sufficient evidence that the address is currently important,
reusable, attributable, or worth paid enrichment.

V2 therefore makes these changes:

1. A lifetime single receipt of at least 500 BTC is never sufficient for P0.
2. Receipt-based P0 evidence is time-windowed and must have independent chain
   support: retained capital, repeated whale receipts, or sustained
   non-trigger activity.
3. Lifetime receipt magnitude no longer contributes unconditional importance
   score points. Recent and supported receipt evidence replaces it.
4. The identity evidence layer remains separate. Official evidence, a local
   override, or provider-default identity can prioritize a known address after
   the chain census, but it cannot alter chain facts.
5. A future v2 query returns exactly one aggregate row containing counts and
   overlap matrices. It must not return an address, transaction hash, block
   hash, or provider identity.

The recommended policy is a strict 90-day receipt rule. A balanced 365-day
variant is measured in the same aggregate query for sensitivity analysis, but
it is not the default.

## Why A Higher Lifetime Threshold Is Not Enough

Changing only the lifetime receipt threshold does not fix the semantic defect:

| Lifetime maximum same-transaction receipt | Address count |
| --- | ---: |
| `>=500 BTC` | 1,121,720 |
| `>=1,000 BTC` | 489,610 |
| `>=5,000 BTC` | 97,882 |

The 5,000 BTC count happens to be near the first-wave capacity target, but it
still treats one stale transfer, consolidation output, or change-like event as
permanent address importance. V2 must change the evidence structure, not just
raise the numeric threshold.

## Goals

- Preserve high-value current holders and active high-turnover addresses.
- Remove stale one-time transfer and change-address history from mandatory P0.
- Avoid double-counting the triggering whale receipt as independent turnover.
- Measure exact policy size before any address materialization.
- Keep all thresholds integer-satoshi and cutoff deterministic.
- Reuse one full source scan for all v2 variants and diagnostics.
- Keep provider enrichment, graph expansion, identity resolution, and consumer
  behavior unchanged.

## Non-Goals

- Do not identify address ownership from transaction patterns.
- Do not infer that a large receipt is an external ownership transfer.
- Do not import Arkham or 0xRouter labels in the BigQuery query.
- Do not cluster common inputs or change addresses.
- Do not rank or export individual addresses in this phase.
- Do not reinterpret or overwrite the immutable v1 receipt.
- Do not perform another approximately 638 GB billed query under this design.

## Semantic Boundary

`btc_importance_v2` is a provider-enrichment priority policy. It is not an
identity confidence score, ownership-transfer classifier, whale-alert rule, or
suppression policy.

The chain layer answers:

```text
Is this address economically important enough to prioritize for identity work?
```

The identity layer separately answers:

```text
What entity or wallet role is supported by provider and local evidence?
```

The consumer separately decides:

```text
How should an alert, report, or signal use that resolved evidence?
```

No layer may silently substitute for another.

## Fixed Source Contract

The aggregate query reads only:

```text
bigquery-public-data.crypto_bitcoin.transactions
```

The source boundary, cutoff filters, standard-address semantics, duplicate
transaction-id protection, and integer conversion rules remain the same as the
v1 aggregate query.

The v2 census pins:

- cutoff height: `959187`;
- cutoff time: `2026-07-24T23:59:59.999999Z`;
- expected standard output-address population: `1,557,941,780`;
- expected input-only diagnostic count: `3`.

The standard population is output-defined. Input-only subjects are excluded
from candidate policy. The known count of three becomes a pinned source
diagnostic: an exact match is a warning, while any increase, decrease, or
semantic reinterpretation blocks the v2 result.

The pinned BigQuery population is an execution checkpoint, not independent
proof of complete Bitcoin history. Candidate materialization remains blocked
until a separately designed Bitcoin Core or equivalent independent source
check validates the relevant cutoff and feature semantics.

## Transaction-Level Feature Model

### One Address-Transaction Row

After source projection and single-address filtering, aggregate one row per:

```text
(block_hash, transaction_hash, normalized_address)
```

Each row contains:

- `tx_received_sats`;
- `tx_spent_sats`;
- `tx_gross_sats`;
- `block_timestamp`.

`tx_received_sats` is the sum of all outputs to the same address in the
transaction. This preserves the accepted meaning of "same-transaction
receipt" without treating each output independently.

### Address-Level Features

Aggregate the transaction rows once by address:

- `lifetime_received_sats`
- `lifetime_spent_sats`
- `current_utxo_sats`
- `gross_flow_90d_sats`
- `last_seen_time`
- `max_same_tx_received_lifetime_sats`
- `max_same_tx_received_90d_sats`
- `max_same_tx_received_365d_sats`
- `same_tx_receive_ge_500_btc_lifetime_count`
- `same_tx_receive_ge_500_btc_90d_count`
- `same_tx_receive_ge_500_btc_365d_count`
- `active_tx_90d_count`
- `active_day_90d_count`
- `active_tx_365d_count`
- `active_day_365d_count`
- `last_same_tx_receive_ge_500_btc_time`

Derived features:

```text
residual_gross_90d_sats =
  max(gross_flow_90d_sats - max_same_tx_received_90d_sats, 0)

recent_receipt_retained =
  max_same_tx_received_90d_sats >= 500 BTC
  and current_utxo_sats >= 10 BTC
  and current_utxo_sats * 100 >= max_same_tx_received_90d_sats

recent_receipt_repeated =
  same_tx_receive_ge_500_btc_90d_count >= 2

recent_receipt_sustained_activity =
  active_tx_90d_count >= 3
  and active_day_90d_count >= 2
  and residual_gross_90d_sats >= 500 BTC

sustained_high_turnover =
  active_tx_90d_count >= 3
  and active_day_90d_count >= 2
  and residual_gross_90d_sats >= 1,000 BTC
```

The retained-capital test requires both an absolute floor of 10 BTC and at
least 1% retention relative to the largest recent receipt. The residual-flow
test subtracts the triggering receipt before treating turnover as independent
support. This prevents one 500 BTC receipt from automatically satisfying a
100 BTC turnover companion.

## Recommended V2 Policy

### Chain P0

An output-defined standard address enters chain P0 when any condition is true:

| Code | Exact condition |
| --- | --- |
| `utxo_ge_100_btc` | `current_utxo_sats >= 100 BTC` |
| `sustained_residual_gross_90d_ge_1000_btc` | Residual 90-day gross flow `>=1,000 BTC`, at least 3 active transactions, and at least 2 active days |
| `recent_whale_receipt_supported_90d` | At least one receipt `>=500 BTC` in 90 days and any support condition below is true |
| `lifetime_ge_10000_active_supported_90d` | Lifetime received `>=10,000 BTC`, last activity within 90 days, and either current UTXO `>=10 BTC` or sustained non-trigger activity with residual 90-day gross flow `>=500 BTC`, at least 3 active transactions, and at least 2 active days |

The support conditions for `recent_whale_receipt_supported_90d` are:

```text
recent_receipt_retained
or recent_receipt_repeated
or recent_receipt_sustained_activity
```

An all-time or 365-day receipt of 500 BTC without one of these support
conditions is not P0.

Raw gross flow is not a P0 reason in v2. A single 1,000 BTC receipt therefore
cannot enter P0 by relabeling the same value as 90-day turnover. The turnover
reason uses residual flow plus transaction and active-day support.

### P0 Capacity Budget

The v1 overlap matrix proves that removing the unconditional receipt bit leaves
an upper-bound base union of `24,094` addresses. V2 also tightens turnover and
lifetime-active rules, so its non-receipt base should be no larger than that
without an explainable policy bug.

The first-wave target remains `120,000` chain P0 addresses. Relative to the v1
upper-bound base, the supported receipt rule has at most `95,906` incremental
unique slots before the target is exceeded. The aggregate query must report
that incremental count directly rather than infer it from raw reason totals.

### Identity-Priority Overlay

The following remain separate local overlay reasons:

- exact Tier A or Tier B official/signed evidence;
- an active local override;
- an unresolved provider/local evidence conflict;
- an existing consumer watchlist address;
- a provider-default resolved entity.

These reasons may schedule an address for identity review or enrichment. They
must not be added to `chain_p0_count`, change a chain feature, or be queried
inside public BigQuery.

Already resolved provider-default addresses are lookup-ready and must not be
dispatched again merely because they overlap a chain cohort. A checksum-pinned
local audit may report overlay reason counts and a conservative
`chain_p0 + overlay` upper bound, but the exact deduplicated union belongs to a
later materialization phase.

### P1 Score

V2 removes the v1 lifetime maximum-receipt score buckets. It replaces them with
supported and time-windowed evidence:

| Feature family | Points |
| --- | --- |
| Current UTXO >=1 / 10 / 100 / 1,000 BTC | 5 / 12 / 20 / 25 |
| Residual 90-day gross flow >=10 / 100 / 1,000 / 10,000 BTC | 3 / 8 / 15 / 20 |
| Largest receipt >=500 BTC within 365 / 90 days | 5 / 10 |
| At least two receipts >=500 BTC within 365 / 90 days | 7 / 12 |
| Recent receipt retained | 10 |
| Recent receipt sustained activity | 8 |
| Last activity within 365 / 90 / 30 days | 3 / 7 / 10 |

Only the highest bucket within each row of a feature family applies. Addresses
already in P0 are excluded from P1. The initial P1 threshold remains 25.

The score is designed so:

- a stale lifetime receipt alone contributes zero;
- one unsupported receipt between 91 and 365 days contributes only 5;
- one unsupported recent receipt does not become important solely because the
  same receipt also inflated gross flow;
- retained, repeated, or sustained activity can promote a recent address;
- current capital and genuine residual turnover remain independently useful.

### Edge Frontier

The v2 graph-upgrade frontier is:

```text
not P0
and v2 chain score between 15 and 24
```

Graph evidence is still not computed by the aggregate query. This count only
measures addresses that could cross the P1 threshold after later graph or
identity evidence.

## Receipt Funnel

The aggregate result must expose the full demotion funnel:

1. lifetime receipt `>=500 BTC`;
2. receipt `>=500 BTC` within 365 days;
3. receipt `>=500 BTC` within 90 days;
4. exactly one qualifying 90-day receipt;
5. at least two qualifying 90-day receipts;
6. retained-capital support;
7. sustained-activity support;
8. strict supported-receipt P0;
9. unsupported recent singleton;
10. stale lifetime singleton.

Every later stage must be a subset of the appropriate earlier stage. This
makes accidental broadening machine-detectable.

## Sensitivity Variants

One future aggregate query measures three policy variants in the same source
scan:

### V2-S: Strict 90-Day, Recommended

Use the exact P0 policy above.

### V2-B: Balanced 365-Day, Shadow Only

Start with every V2-S strict supported receipt. Additionally accept a 365-day
receipt when either of these conditions is true:

```text
current_utxo_sats >= 10 BTC
and current_utxo_sats * 100 >= max_same_tx_received_365d_sats
```

or:

```text
same_tx_receive_ge_500_btc_365d_count >= 2
and active_day_365d_count >= 2
```

This variant measures the recall cost of the strict 90-day window. It is not
eligible for automatic selection. Its receipt and P0 unions must be supersets
of V2-S; a smaller count is a blocking reconciliation failure.

### V2-R: Retention-Only, Diagnostic

Keep a lifetime receipt `>=500 BTC` only when the current balance is at least
10 BTC and at least 1% of the lifetime maximum receipt.

This variant isolates whether retention alone creates an acceptable cohort.
It is diagnostic and cannot become policy without review.

The final row must return each variant's P0 union, incremental receipt cohort,
P1 count, edge frontier, coarse union, and overlap bitmask distribution.

## Coarse Candidate Union

V2 does not preserve the v1 rule `lifetime max receipt >=100 BTC` as an
unconditional coarse-candidate reason. That rule alone produced 6,030,079
addresses.

The recommended coarse union is:

```text
chain P0
or v2 chain score >= 15
or current_utxo_sats >= 1 BTC
or residual_gross_90d_sats >= 10 BTC
or max_same_tx_received_365d_sats >= 500 BTC
```

The last condition keeps recent whale-receipt singletons visible for later
ranking without letting all historical recipients dominate the queue.

## Aggregate-Only Result Contract

The future contract version is:

```text
btc_candidate_statistics_v2
```

The query returns exactly one row.

### Source And Integrity Fields

- `contract_version`
- `policy_version`
- `source_cutoff_height`
- `source_cutoff_time`
- `source_standard_address_count`
- `source_input_only_address_count`
- `negative_current_utxo_count`
- `null_value_count`
- `value_cast_failure_count`
- `max_observed_activity_time`
- `query_sha256`
- `schema_sha256`

### Existing Economic Ladders

- current UTXO `>=1/10/100/1,000 BTC`;
- raw gross 90-day flow `>=10/100/1,000/10,000 BTC` for v1 comparison;
- residual gross 90-day flow `>=10/100/1,000/10,000 BTC` for v2 policy;
- recency `<=30/90/365 days`.

### New Receipt Ladders

- lifetime maximum receipt `>=500/1,000/5,000 BTC`;
- 365-day maximum receipt `>=500/1,000/5,000 BTC`;
- 90-day maximum receipt `>=500/1,000/5,000 BTC`;
- qualifying receipt counts `>=1/2/3` for 90 and 365 days;
- retained support;
- repeated support;
- sustained-activity support;
- unsupported recent singleton;
- stale lifetime singleton.

### Policy And Variant Fields

For V2-S, V2-B, and V2-R:

- count for each P0 reason;
- exact P0 union;
- P0 overlap bitmask distribution;
- P1 count excluding P0;
- score histogram;
- edge frontier count;
- coarse union count;
- excluded source count;
- qualified-receipt share of P0.

No output field may contain or encode an address, transaction hash, block hash,
provider entity, raw request, or source row sample.

## Quality Gates

### Blocking

- result row count is not exactly one;
- contract, policy, query, or schema checksum mismatch;
- cutoff height or cutoff time mismatch;
- standard output-address count differs from `1,557,941,780`;
- input-only count differs from the pinned diagnostic count of `3`;
- source activity occurs after the cutoff;
- null value, failed integer cast, or negative finalized balance exists;
- P0 and P1 overlap;
- overlap bitmasks do not reconcile to source and reason counts;
- score histograms do not reconcile to the output-defined source population;
- receipt-funnel subset monotonicity fails;
- a v2 supported-receipt count exceeds its lifetime or window parent;
- V2-B receipt or P0 union is smaller than V2-S;
- V2-S P0 exceeds `1,000,000`;
- any variant coarse union exceeds `5,000,000`;
- edge frontier exceeds `1,000,000`;
- a materialization path, destination table, or identifier field is present;
- query execution requests automatic retry.

### Warnings Requiring Review

- V2-S P0 exceeds `120,000`;
- V2-S coarse union exceeds `2,000,000`;
- supported-receipt addresses exceed 60% of V2-S P0;
- incremental supported-receipt P0 exceeds `95,906`;
- any one P0 reason exceeds 80% of V2-S P0;
- V2-B exceeds V2-S by more than 2x;
- source maximum activity is older than 24 hours at result evaluation;
- the known three input-only subjects remain present.

### Interpretation And Materialization Gates

| Result | Decision |
| --- | --- |
| V2-S P0 `<=120,000`, coarse `<=2,000,000`, all blocking gates pass | Eligible for a separately approved candidate-materialization design |
| V2-S P0 `<=120,000`, coarse `2,000,001..5,000,000` | Tighten coarse policy before materialization |
| V2-S P0 `120,001..1,000,000` | Keep aggregate-only and add deterministic tier ranking |
| V2-S P0 `>1,000,000` | Reject v2 thresholds |
| Edge frontier `>1,000,000` | Defer graph expansion |

Passing these gates permits design work only. It does not itself authorize an
address export or provider campaign.

## Cost And Execution Controls

The v2 SQL should use the same source fields as v1. Added counters increase
compute complexity but should not materially increase scanned bytes. This must
be verified by a free BigQuery dry run.

Execution controls:

- fixed SQL resource and fixed schema;
- checksum-pinned query and schema;
- `maximum_bytes_billed=650_000_000_000`;
- no automatic retry;
- exactly one aggregate row;
- no destination table;
- no query result cache assumption;
- immutable local execution receipt;
- no candidate materialization on the execution path.

The current recorded Sandbox balance is approximately `260.7 GB`, below the
expected full-scan requirement while preserving the approved reserve. A v2
billed execution is therefore not authorized by this design. Implementation
may perform metadata checks and a free dry run only. A future execution needs
separate budget evidence and explicit approval.

## Migration And Compatibility

- Keep `btc_importance_v1`, its SQL, models, tests, and execution receipt
  immutable for audit.
- Add v2 policy, SQL, result model, and quality report under new versioned
  names.
- Never mutate a v1 result into v2.
- Do not change existing resolver precedence:
  `local_override > conflict > provider_default > unresolved`.
- Do not dispatch provider requests from aggregate code.
- Do not change any `quant_crypto` alert or suppression behavior.
- A later candidate table must store `policy_version` and exact feature/reason
  provenance so a v1 and v2 candidate cannot be confused.

## Implementation Sequence After Approval

1. Write an implementation plan for aggregate-only v2.
2. Add tests for exact predicates, correlated-evidence protection, receipt
   funnel monotonicity, result parsing, and quality gates.
3. Add fixed v2 SQL and a one-row result model.
4. Add a dry-run-only CLI and byte-budget checkpoint.
5. Run local tests and the free BigQuery dry run.
6. Review estimated bytes, query plan, and Sandbox balance.
7. Request separate approval before any billed v2 execution.
8. Decide candidate materialization only from a passing aggregate result.

## Acceptance Criteria

- A lifetime single 500 BTC receipt is never sufficient for P0.
- A receipt older than 365 days produces no receipt score by itself.
- A single recent 1,000 BTC receipt cannot enter P0 through raw gross flow.
- A recent 500 BTC receipt is P0 only with retained, repeated, or sustained
  independent chain evidence.
- Gross flow from the triggering receipt cannot satisfy its own support test.
- The v2 aggregate query returns one identifier-free row.
- V1 artifacts remain unchanged.
- No provider request, paid query execution, candidate materialization, or
  consumer behavior change occurs during design and dry-run implementation.
