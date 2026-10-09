# Saved experimental outputs

These files preserve recorded results and metadata. They are not recomputed by
cloning this repository. Start with the [results guide](../docs/RESULTS.md)
for findings, denominators and limitations.

| Study or analysis | Main saved output |
| --- | --- |
| Filter-criterion intervention | [f7_ablation_summary.json](f7_ablation_summary.json) |
| Proxy precision/recall and paired comparisons | [f7_precision_paired.json](f7_precision_paired.json) |
| Matched binary control, 500 documents × four replicates | [plan and summary](matched_control/n500_r4_s20261007/) |
| Revision evaluation, 100 documents × four replicates | [plan, manifest and summary](revision_evaluation/n100_r4_s20261008/) |
| Individual operator repeatability | [rq1_hosted_summary.json](rq1_hosted_summary.json), [rq1_fleiss_kappa.json](rq1_fleiss_kappa.json) |
| Historical composed pipelines and schema conformance | [rq3_summary.json](rq3_summary.json), [rq3_uncertainty.json](rq3_uncertainty.json) |
| Context perturbation | [rq4_summary.json](rq4_summary.json), [rq4_paired.json](rq4_paired.json) |
| Corrected proxy and cross-list analyses | [f10_f11_frozen.json](f10_f11_frozen.json), [crosslist_reanalysis.json](crosslist_reanalysis.json) |
| Exploratory coding reconciliation | [handcheck_reconciliation.json](handcheck_reconciliation.json) |
| Category-stamp removal and wrapper controls | [stamp_ablation_summary.json](stamp_ablation_summary.json), [wrapper_test_summary.json](wrapper_test_summary.json) |
| Historical environment snapshot | [environment.json](environment.json) |

The `*_meta.json` files describe individual historical runs. Early `n50`,
`preliminary` and smoke-test outputs are retained as historical records only;
they are not additional replicates of the main findings. Some old field names,
including `contradiction`, retain their original spelling. Interpret them using
the current paper's criterion-disagreement definition.

Per-run CSVs containing input text and raw request logs are withheld. These
JSON summaries and metadata alone are insufficient for full reanalysis.
See [availability and licensing](../NOTICE.md) and the
[reproduction guide](../docs/ARTIFACT-README.md).
