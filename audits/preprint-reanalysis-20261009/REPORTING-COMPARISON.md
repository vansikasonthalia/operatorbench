# Offline comparison of admission reporting

Post-hoc descriptive reporting comparison. Binary pipelines are masks applied offline to saved all-row answers, not new executions of composed pipelines. Cases share documents and responses; they are not independent trials.

Historical saved non-ML labels count as disagreement. Derived binary cases count False map answers after True filter admission; this measures criterion agreement, not correctness. Changing criteria may change the intended task.

Per replicate, retain only rows valid in all four cells, shared by all three derived comparisons. Invalid exclusions are separate from semantic filter exclusions; no invalid answer is recoded.

Historical cases are executed pipelines. The remaining cases reuse binary answers under three offline masks per replicate; do not pool them.

| Case | Eligible | Before D/n | After D/n | New D | Excluded D | Common added / resolved | Saved changes | Paired counts reconstruct ledger |
|---|---:|---|---|---:|---:|---|---:|---|
| historical_r1 | 500 | 36/154 | 4/94 | 0 | 32 | 0 / 0 | 0 | Yes |
| historical_r2 | 500 | 38/156 | 4/94 | 0 | 34 | 0 / 0 | 0 | Yes |
| matched_r1_filter_change | 479 | 2/154 | 0/100 | 0 | 2 | 0 / 0 | 0 | Yes |
| matched_r1_map_change | 479 | 2/154 | 40/154 | 0 | 0 | 38 / 0 | 38 | Yes |
| matched_r1_both_change | 479 | 2/154 | 2/100 | 0 | 2 | 2 / 0 | 2 | Yes |
| matched_r2_filter_change | 479 | 1/153 | 0/100 | 0 | 1 | 0 / 0 | 0 | Yes |
| matched_r2_map_change | 479 | 1/153 | 40/153 | 0 | 0 | 39 / 0 | 39 | Yes |
| matched_r2_both_change | 479 | 1/153 | 2/100 | 0 | 1 | 2 / 0 | 2 | Yes |
| matched_r3_filter_change | 479 | 1/153 | 0/102 | 0 | 1 | 0 / 0 | 0 | Yes |
| matched_r3_map_change | 479 | 1/153 | 40/153 | 0 | 0 | 39 / 0 | 39 | Yes |
| matched_r3_both_change | 479 | 1/153 | 1/102 | 0 | 1 | 1 / 0 | 1 | Yes |
| matched_r4_filter_change | 474 | 1/154 | 0/100 | 0 | 1 | 0 / 0 | 0 | Yes |
| matched_r4_map_change | 474 | 1/154 | 42/154 | 0 | 0 | 41 / 0 | 41 | Yes |
| matched_r4_both_change | 474 | 1/154 | 2/100 | 0 | 1 | 2 / 0 | 2 | Yes |

## Format exclusions before binary masking

The shared eligible cohorts have 479, 479, 479, 474 rows. JSON retains excluded row IDs per cell.

## Constructed ambiguity

Both witnesses start with 1 disagreement among 2 admissions and end with 0 among 1 (input universe 3). One excludes the disagreement; the other resolves it on a retained row and excludes a previously agreeing row. Their rates, counts, and coverage are identical. Paired counts distinguish them. This is a mathematical example, not an empirical failure rate.

## Interpretation

Rates alone omit coverage. Totals plus coverage expose contraction but do not identify the accounting mechanism in general. Common-row paired counts plus totals reconstruct every aggregate ledger term and partition size. The full ledger makes those calculations explicit and additionally retains row IDs, raw transitions and separate validity reporting. This is not a demonstrated improvement in error detection, a new theorem, or evidence of generality.

No blinded diagnosis experiment, detection accuracy, scientific correctness assessment, or model rerun was performed.
