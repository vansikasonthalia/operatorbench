# Recomputed table inputs

Historical table inputs are in `historical/*.json`; these tables show the admission ledger and all matched-control contrasts.

| Replicate | Common | Excluded | New | Saved label changes | Raw label changes | Excluded disagreements |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 94 | 60 | 0 | 0 | 0 | 32 |
| 2 | 94 | 62 | 0 | 0 | 0 | 34 |

## Full admission accounting

| Replicate | Before D/n | After D/n | Never admitted | New-admission disagreements | Excluded disagreements | Common added / resolved | Count change | Identity |
|---|---|---|---:|---:|---:|---|---:|---|
| 1 | 36/154 | 4/94 | 346 | 0 | 32 | 0 / 0 | -32 | True |
| 2 | 38/156 | 4/94 | 344 | 0 | 34 | 0 / 0 | -34 | True |

### Replicate 1 common-row label transitions

| Layer | Before | After | Rows |
|---|---|---|---:|
| saved | ML | ML | 90 |
| saved | theory | theory | 4 |
| raw | ML | ML | 90 |
| raw | theory | theory | 4 |

Raw invalid counts before/after: 0 / 0. Saved invalid counts before/after: 0 / 0.

### Replicate 2 common-row label transitions

| Layer | Before | After | Rows |
|---|---|---|---:|
| saved | ML | ML | 90 |
| saved | theory | theory | 4 |
| raw | ML | ML | 90 |
| raw | theory | theory | 4 |

Raw invalid counts before/after: 0 / 0. Saved invalid counts before/after: 0 / 0.

## Synthetic procedure example

`admission_ledger_example.json` contains constructed records, not experimental observations.

Both configurations admit 5 rows and count 3 disagreements. The ledger has 1 introduced disagreement, 1 removed disagreement, and opposing common-row changes (1 new, 1 resolved). Net change: 1 - 1 + (1 - 1) = 0, despite 2 saved-label and 4 raw-output changes.

| Layer | Invalid before | Invalid after |
|---|---:|---:|
| raw | 2 | 1 |
| saved | 1 | 1 |

## Primary strict analysis

| Replicate | Contrast | Valid pairs | Left only | Right only | Difference % | Holm p |
|---|---|---:|---:|---:|---:|---:|
| 1 | operator_about | 500 | 2 | 5 | 1.40 | 0.875 |
| 1 | operator_primary | 479 | 2 | 16 | 3.76 | 0.010498047 |
| 1 | criterion_filter | 500 | 55 | 0 | 11.00 | 8.8817842e-16 |
| 1 | criterion_map | 479 | 43 | 0 | 8.98 | 2.0463631e-12 |
| 2 | operator_about | 500 | 1 | 5 | 1.20 | 0.875 |
| 2 | operator_primary | 479 | 2 | 15 | 3.55 | 0.016448975 |
| 2 | criterion_filter | 500 | 54 | 0 | 10.80 | 1.5543122e-15 |
| 2 | criterion_map | 479 | 44 | 0 | 9.19 | 1.2505552e-12 |
| 3 | operator_about | 500 | 1 | 5 | 1.20 | 0.875 |
| 3 | operator_primary | 479 | 1 | 12 | 2.71 | 0.020507812 |
| 3 | criterion_filter | 500 | 52 | 0 | 10.40 | 5.7731597e-15 |
| 3 | criterion_map | 479 | 44 | 0 | 9.19 | 1.2505552e-12 |
| 4 | operator_about | 500 | 1 | 4 | 1.00 | 0.875 |
| 4 | operator_primary | 474 | 2 | 14 | 3.38 | 0.020904541 |
| 4 | criterion_filter | 500 | 55 | 0 | 11.00 | 8.8817842e-16 |
| 4 | criterion_map | 474 | 45 | 0 | 9.49 | 6.8212103e-13 |

## Exploratory terminal-period alternative

| Replicate | Contrast | Valid pairs | Left only | Right only | Difference % | Holm p |
|---|---|---:|---:|---:|---:|---:|
| 1 | operator_about | 500 | 2 | 5 | 1.40 | 0.875 |
| 1 | operator_primary | 500 | 2 | 16 | 3.60 | 0.010498047 |
| 1 | criterion_filter | 500 | 55 | 0 | 11.00 | 8.8817842e-16 |
| 1 | criterion_map | 500 | 44 | 0 | 8.80 | 1.0231815e-12 |
| 2 | operator_about | 500 | 1 | 5 | 1.20 | 0.875 |
| 2 | operator_primary | 500 | 2 | 15 | 3.40 | 0.016448975 |
| 2 | criterion_filter | 500 | 54 | 0 | 10.80 | 1.5543122e-15 |
| 2 | criterion_map | 500 | 45 | 0 | 9.00 | 6.2527761e-13 |
| 3 | operator_about | 500 | 1 | 5 | 1.20 | 0.875 |
| 3 | operator_primary | 500 | 1 | 12 | 2.60 | 0.020507812 |
| 3 | criterion_filter | 500 | 52 | 0 | 10.40 | 5.7731597e-15 |
| 3 | criterion_map | 500 | 45 | 0 | 9.00 | 6.2527761e-13 |
| 4 | operator_about | 500 | 1 | 4 | 1.00 | 0.875 |
| 4 | operator_primary | 500 | 2 | 14 | 3.20 | 0.020904541 |
| 4 | criterion_filter | 500 | 55 | 0 | 11.00 | 8.8817842e-16 |
| 4 | criterion_map | 500 | 46 | 0 | 9.20 | 3.4106051e-13 |

## Exploratory gap-block exclusion

| Replicate | Contrast | Valid pairs | Left only | Right only | Difference % | Holm p |
|---|---|---:|---:|---:|---:|---:|
| 1 | operator_about | 500 | 2 | 5 | 1.40 | 0.90625 |
| 1 | operator_primary | 479 | 2 | 16 | 3.76 | 0.010498047 |
| 1 | criterion_filter | 500 | 55 | 0 | 11.00 | 8.8817842e-16 |
| 1 | criterion_map | 479 | 43 | 0 | 8.98 | 2.2737368e-12 |
| 2 | operator_about | 500 | 1 | 5 | 1.20 | 0.875 |
| 2 | operator_primary | 479 | 2 | 15 | 3.55 | 0.016448975 |
| 2 | criterion_filter | 500 | 54 | 0 | 10.80 | 1.6653345e-15 |
| 2 | criterion_map | 479 | 44 | 0 | 9.19 | 1.3642421e-12 |
| 3 | operator_about | 500 | 1 | 5 | 1.20 | 0.875 |
| 3 | operator_primary | 479 | 1 | 12 | 2.71 | 0.020507812 |
| 3 | criterion_filter | 500 | 52 | 0 | 10.40 | 5.7731597e-15 |
| 3 | criterion_map | 479 | 44 | 0 | 9.19 | 1.3642421e-12 |
| 4 | operator_about | 490 | 1 | 2 | 0.61 | 1 |
| 4 | operator_primary | 465 | 2 | 13 | 3.23 | 0.03692627 |
| 4 | criterion_filter | 490 | 53 | 0 | 10.82 | 3.1086245e-15 |
| 4 | criterion_map | 465 | 42 | 0 | 9.03 | 4.0927262e-12 |

Holm across the original 16 contrasts separately within each alternative; no correction across the two post-hoc alternatives or other analyst choices. Wilson intervals are conditional row-level summaries, not population inference.
