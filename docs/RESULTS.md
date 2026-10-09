# Results and evidence

The [manuscript](../paper/reproducibility.tex) is the authoritative account of
methods and interpretation. The files below are saved outputs, not results
recomputed by visiting or cloning this repository.

## 1. Lower disagreement through exclusion

The historical intervention keeps the map instruction fixed and changes the
filter from ML aboutness to ML primary-topic membership.

| Quantity | Replicate 1 | Replicate 2 |
| --- | ---: | ---: |
| Input documents | 500 | 500 |
| Admissions before / after | 154 / 94 | 156 / 94 |
| Disagreements before / after | 36 / 4 | 38 / 4 |
| Excluded documents | 60 | 62 |
| Disagreements on excluded documents | 32 | 34 |
| Common admissions | 94 | 94 |
| Changed saved labels on common admissions | 0 | 0 |

The entire decrease in disagreement counts comes from exclusion. A theory paper
about a learning algorithm can satisfy an ML-aboutness filter and receive a
non-ML primary-topic label. Such criterion disagreement is not automatically
a logical contradiction or an incorrect answer.

Proxy-based recall falls from 87.83% to 61.74% in both replicates. These recall
figures use the 449 rows with author-declared categories, including 115
proxy-positive rows. They are distinct from the 500-row admission analysis.

Evidence: [admission ledger](../audits/preprint-reanalysis-20261009/admission_ledger.json),
[readable tables](../audits/preprint-reanalysis-20261009/TABLES.md),
[paired proxy comparison](../results/f7_precision_paired.json).

## 2. Matched criterion-by-operator control

This control asks the same binary question through `sem_filter` and `sem_map`,
for both aboutness and primary-topic membership. It records 8,000 requests:
500 documents × four cells × four replicates. Native operator prompts still
differ, so this is not an isolated test of a system message.

Among format-valid pairs, the primary-topic contrast shows directional
operator asymmetry after the planned Holm correction. The exploratory
terminal-period parser and gap-block exclusion analyses retain its direction.
Invalid formats remain separate from negative answers.

Evidence: [design](matched_control.md),
[fixed plan](../results/matched_control/n500_r4_s20261007/plan.json),
[summary](../results/matched_control/n500_r4_s20261007/summary.json),
[sensitivity results](../audits/2026-10-08/matched_control_sensitivity.json),
[archive audit](../audits/2026-10-08/matched_control_full_check.json).

## 3. Revision requirements fixed before new executions

The revision evaluation records 2,800 requests on 100 reused documents:
seven cells × four replicates. The questions and intended requirements were
fixed locally before those executions; this was not external preregistration.

- The filter paraphrase violates intended admission equality on row 236 in
  all four replicates.
- The narrower filter excludes rows 37 and 220 and satisfies its declared
  subset requirement.
- Identical-prompt repeats and the map paraphrase preserve the admitted-row
  results in this sample.
- Four invalid map responses occur on filter-rejected rows and remain reported.

Separately implemented core record assertions recover the same exceptions as
the ledger checker in all 16 comparisons. They share parser-derived inputs
and supplemental format/membership checks. This is not evidence of a ledger
advantage, independent correctness assessment or held-out generalization.

Evidence: [original preparation record](REVISION-EVALUATION.md),
[run and analysis guide](REVISION-RUNNER.md),
[saved summary](../results/revision_evaluation/n100_r4_s20261008/summary.json),
[verification report](../audits/2026-10-08/revision_evaluation_check.json).
The preparation record still says “no new outputs”: it is preserved exactly
as it was when bound into the experiment. The saved run documents its later
completion on 8 October 2026.

## 4. Reporting comparison and historical support

The offline reporting comparison reconstructs all aggregate ledger terms
from common-row paired counts and admission totals. Its binary cases are
masks derived from recorded answers, not additional pipeline executions.
Requirements in the earlier prototype demonstration were chosen after
inspecting those cases; that demonstration is distinct from the later
revision evaluation above.

Evidence: [reporting comparison](../audits/preprint-reanalysis-20261009/REPORTING-COMPARISON.md),
[prototype requirement checks](../audits/preprint-reanalysis-20261009/REVISION-CONTRACTS.md).

Historical repeatability, schema conformance, context perturbation, corrected
category proxies, stamp removal and wrapper comparisons support the paper's
appendices. The [result index](../results/README.md) identifies those reports.
Historical field names such as `contradiction` are retained for compatibility;
the paper interprets them as criterion disagreement under a declared rule.

## What the audits establish

The [saved reanalysis report](../audits/preprint-reanalysis-20261009/report.json)
records nine passing checks on the complete local archive. They establish
consistency of saved outputs and calculations, not answer correctness. The
historical evidence still has 178 malformed log lines and 30 runs with
provenance gaps. The [audit index](../audits/README.md) explains the scope.

Raw inputs and request logs are not in this public snapshot. See the
[reproduction guide](ARTIFACT-README.md) before attempting the archive commands.
