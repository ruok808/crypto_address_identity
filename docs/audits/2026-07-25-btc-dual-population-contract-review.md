# BTC Dual-Population Contract Review

## Verdict

Approved for interpretation and Strict V2-S materialization design.

Not approved for candidate materialization or provider enrichment.

The implementation admits the existing output-defined and positive-value
aggregate receipts without changing either receipt, rerunning BigQuery, or
silently clearing their original `quality_blocked` status.

## Reviewed Evidence

| Metric | Accepted value |
| --- | ---: |
| Output-defined standard addresses | 1,557,941,780 |
| Positive-value standard addresses | 1,531,420,608 |
| Output-defined but non-positive addresses | 26,521,172 |
| Strict V2-S P0 | 21,736 |
| Strict V2-S P1 | 2,143 |
| Strict V2-S edge frontier | 133,730 |
| Strict V2-S coarse union | 1,090,411 |

The output-defined receipt SHA-256 is
`7a657f69f08c8ceb8756ed9e2e37d82b0bd007e843e2cd22a241bc8b9c7cf77b`.
The positive-value receipt SHA-256 is
`c3123159ba77e0bcd5ba4735483027899bc451a50c8648784ec4317dfe20a236`.
Both files remained regular mode-`0600` files.

The separate historical address-scale result `1,557,951,354` is not admitted
as either population. It remains a reference from a different query.

## Review Findings And Fixes

1. The v2 receipt field is
   `expected_source_input_only_address_count`; the validator and fixture were
   corrected to use that exact field.
2. Receipt reads now use `lstat` and reject symbolic links before parsing.
3. Contract-field comparison now preserves JSON types, so booleans cannot pass
   integer gates through Python equality.
4. Duplicate blocker entries no longer collapse into an accepted blocker set.
5. The historical address-scale expected count and the fixed billing cap are
   explicit v1 receipt fields, preventing the three population concepts from
   being conflated.
6. An unrelated CLI execution test used the wall clock against a fixed 2026
   fixture and had become stale. Its test clock is now frozen; production
   freshness behavior is unchanged.

No remaining code-review blocker was found.

## Verification

Focused contract and CLI tests:

```text
39 passed
```

All universe tests after the final review fixes:

```text
208 passed
```

Full repository tests after the final review fixes:

```text
344 passed
```

The real offline command returned:

- `status=accepted`;
- `allow_population_interpretation=true`;
- `allow_materialization_design=true`;
- `candidate_materialization_allowed=false`;
- `receipt_reads=2`;
- `network_requests=0`;
- `provider_requests=0`;
- `provider_points=0`;
- `written_paths=[]`.

`py_compile` and `git diff --check` also passed.

## Non-Interference

- paid BigQuery executions: `0`;
- live provider requests: `0`;
- 0xRouter points consumed: `0`;
- candidate rows written: `0`;
- identity database changes: `0`;
- resolver or consumer behavior changes: `0`.

The next permitted artifact is a design for Strict V2-S candidate
materialization. Executing that design requires a separate cost checkpoint and
explicit authorization.
