# Reviewing the saved evidence

The complete local review ZIP (not part of the initial public release) includes the frozen `data/`, `results/`, and `logs/` directories.
Extract it and enter the `OperatorBench` folder before running these commands.
A source-only Git clone omits the raw logs and needs that evidence supplied
separately. Start with `ARCHIVE-README.md` when using the ZIP.

Run from the repository root with Python 3.10 or newer. The offline analysis
environment needs pandas and NumPy, plus their installation dependencies.
It does not need LOTUS, a model client, an API key, or dataset downloads.

```powershell
python -m venv .artifact-venv
.\.artifact-venv\Scripts\python.exe -m pip install -r requirements-artifact.txt
.\.artifact-venv\Scripts\python.exe -m scripts.reproduce_artifact
```

On Linux or macOS, use `.artifact-venv/bin/python` for the last two commands.
Dependency installation needs network access or cached wheels. The reproduction
command blocks socket connections and performs no model calls.

The default output is `audits/reproduction/`. An optional `--output` must select
a separate directory under `audits/`, outside the dated audits. Generated outputs
can be overwritten on a repeat run; the experiment archives are inputs only.
Do not run dataset preparation, hand-check generation, `capture_environment.py`,
or a paid experiment to review the saved evidence. Those are different workflows.

## Outputs and manuscript coverage

| Output | What it reproduces or checks |
|---|---|
| `historical/comparison.json` and eight accompanying JSON files | Historical repeatability/kappa, raw schema and persistence counts, proxy agreement and precision/recall, context perturbation, uncertainty, exploratory coding, stamp-removal control, and wrapper control |
| `historical_audit/evidence_audit.json` and `EVIDENCE-LEDGER.md` | Frozen row structure, 14 historical headline counts, historical log integrity and the original log manifest |
| `admission_ledger.json` | All four row partitions; before/after counts and rates; all accounting terms and the identity check; both indicator-change directions; raw/saved transition tables and invalid-output IDs. Retains the original seven audit fields. |
| `admission_ledger_example.json` | Explicitly synthetic seven-row example illustrating new/excluded disagreements and opposing common-row changes; not experimental evidence |
| `matched_control_summary.json` | All four replicates, four paired contrasts, strict-parser exclusions, Holm corrections, and repeatability; exact comparison with the saved full-study summary |
| `matched_control_sensitivity.json` | Terminal-period parsing and gap-block exclusion alternatives, with a separate 16-comparison Holm family per alternative |
| `admission_reporting_comparison.json` and `REPORTING-COMPARISON.md` | Post-hoc comparison of four reporting views on two historical revisions and twelve offline binary masks; paired-count reconstruction checks and clearly constructed ambiguity witnesses |
| `revision_contracts.json` and `REVISION-CONTRACTS.md` | Prototype checks under explicit admission and label-preservation requirements; full-input and observed-cohort verdicts are distinguished, with exact exception and unassessed row IDs |
| `TABLES.md` | Readable admission ledger and all 48 primary/alternative matched-control comparisons |
| `manifest.json` | SHA-256 and size of scientific source, documentation, data, results, logs, and dated audit files |
| `report.json` | Pass/fail checks, analysis environment, and historical evidence gaps |

Exit status zero and `report.json` containing `"passed": true` mean the listed
checks completed successfully. Missing outputs, missing numerical fields, changed
counts, failed bindings, or changes to the inventoried files cause a failure.
Historical comparisons deliberately ignore prose changes and added disclosures;
the matched-control summary and saved sensitivities are compared exactly.
A fatal error exits nonzero; the report remains marked as running rather than
retaining a successful report from a previous run.

The manifest covers the current local scientific files. It excludes the generated
reproduction folder, Python environments/caches, downloaded reference papers,
and unrelated personal drafts. It does not establish when historical files were
created. `logs_manifest.json` remains the separate earlier log checksum record.
The ten-row pilot is inventoried but is not pooled into the 500-row control.

The complete ledger is implemented by `harness/admission.py::build_ledger`.
Supply row-ID mappings for the two admitted-row outputs, the complete input
universe, saved/raw label column names, the declared valid labels, and a fixed
disagreement predicate. Invalid output counts are reported separately; that
predicate determines whether invalid labels also count as disagreements.
Never-admitted rows get no inferred labels. An empty admitted set has a null
conditional rate.

`harness/revision.py::check_revision` checks a supplied ledger against explicit
requirements: equal/subset/superset/unrestricted admission sets, preservation
of common saved labels, absence of new common-row disagreement, and valid raw
and saved outputs. It returns `satisfied`, `violated`, or `not_evaluable`, with
per-requirement evidence. A known violation takes precedence over an unassessed
requirement. An empty common cohort cannot certify label preservation or the
absence of new common-row disagreement. The validity rules come from the ledger;
the checker neither reparses responses nor independently validates its inputs.
Row completeness concerns the supplied ledger universe, not historical request
coverage, execution provenance, or semantic correctness.

The prototype demonstration applies requirements chosen after inspecting the
fourteen reporting cases. Strict result preservation requires equal admission
sets and unchanged common labels. A declared filter revision permits narrowing
and preserves common labels; a declared map revision preserves admissions but
permits label changes; a joint revision permits both. For the binary cases,
format-excluded rows remain unassessed. Full-input requirements cannot be
certified; an explicit `require_complete_input=False` permits a verdict limited
to the observed cohort and still records omitted row IDs. These examples do not
establish detection accuracy, prospective validation, or methodological novelty.
For filter-only cases, the common four-cell cohort omits rows even though the
required filter/map answers are valid. The full-input verdict concerns the
supplied view; it does not assert that all original responses are missing.
A separately derived filter-only view could use all 500 rows.

The seven-row procedure example is generated by
`scripts/admission_ledger_example.py`. Both admitted sets have five rows and
three disagreements, yet selection and common-row changes occur in both
directions. Its constructed raw/saved differences also demonstrate why format
validity must be retained separately. The original historical ledger snapshot
in `audits/2026-10-08/admission_ledger_check.json` is not overwritten: the new
reanalysis compares its retained fields with that snapshot.

## Evidence limits

Recomputation validates recorded outputs; it cannot prove the model answers are
correct. The historical archive has 178 malformed JSONL lines and incomplete
execution provenance. Those gaps are reported separately and do not disappear
when the numerical checks pass. The script verifies the matched plan's dataset,
input-window, experiment-source and schedule bindings and all checkpoint/request
checksums. The separate full request-to-row/prompt audit is retained in
`audits/2026-10-08/matched_control_full_check.json`; this command does not repeat
every semantic join from that audit or require an installed historical framework.

The sensitivity analyses were selected after inspecting the results. They retain
the original strict analysis as primary, do not pool replicate p-values, and do
not correct across the two chosen alternatives or other analyst choices.
The reporting comparison reuses recorded answers; its twelve binary cases are
derived masks, not new executions of composed pipelines. Within each replicate,
all three binary cases share the cohort valid in all four original cells.
The excluded invalid row IDs remain separate from semantic filter exclusions.
Common-row paired counts plus admission totals reconstruct all aggregate ledger
terms; the ledger is not claimed to outperform that baseline. The constructed
witnesses demonstrate ambiguity of aggregate counts, not an empirical detection
rate. No diagnostic accuracy, correctness, or generalization test is performed.
For only this comparison, run `python -m scripts.compare_admission_reporting`;
its default outputs are under `audits/reporting_comparison/`.
The manuscript is `paper/reproducibility.tex`. This command does not compile or
check PDF layout, establish submission readiness, or adjudicate correctness.

The reproduction environment is captured anew in `report.json`; it does not
overwrite `results/environment.json` or `requirements.lock.txt`. Those older
snapshots and the matched-control execution plan describe separate environments.

## Public snapshot and complete local archive

The initial GitHub preprint snapshot contains code, the paper source, fixed
protocols, aggregate results and selected audits. It excludes copied dataset
text, result CSVs containing documents and raw request logs while redistribution
terms are resolved; see `NOTICE.md`. It cannot run the complete reproduction
commands by itself. The reproduction instructions in this guide require the complete local archive.

## Creating or verifying the local review archive

The separate revision evaluation protocol is documented in `docs/REVISION-EVALUATION.md`.
`scripts/prepare_revision_evaluation.py` fixes and verifies a local protocol
without obtaining new outputs. Its default files are under
`artifacts/revision-evaluation-20261008/`; they are preparation records, not
additional experimental results or an external preregistration. The execution
runner and its offline preflight, resume and aggregation instructions are in
`docs/REVISION-RUNNER.md`. Implementing it adds no experimental results and
does not change the frozen scientific protocol. The new direct-record baseline is in
`harness/revision_assertions.py`; it does not call the ledger implementation.

The completed revision archive is at
`results/revision_evaluation/n100_r4_s20261008/`. Its 280 checkpoints and
2,800 requests reproduce the saved descriptive summary; the separate audit is
`audits/2026-10-08/revision_evaluation_check.json`. All four replicates show the
same filter-paraphrase exception (row 236), permitted narrowing (rows 37 and
220 excluded), and admitted-row preservation for repeats and the map
paraphrase. Four invalid map responses remain reported on filter-rejected
rows. Separately implemented core assertions agree with the ledger checker on
all 16 comparisons. Both consume shared parser-derived records; supplemental
format and membership checks are computed once and shared by both verdicts.

For that study, run `python -m scripts.run_revision_evaluation --aggregate-only`
with its frozen protocol directory present. It verifies request-to-row joins
and regenerates `summary.json` without model calls. The historical and
matched-control reproduction command above does not aggregate this archive.
The preparation record remains fixed at its earlier status; the copied plan,
run manifest, checkpoints and summary document the subsequent execution.
These results concern new answers on previously studied documents, not a
holdout, correctness assessment or ledger detection advantage. An earlier
review ZIP remains its original snapshot until a new archive is built.

After a successful reproduction, run `python -m scripts.package_artifact`.
It creates `artifacts/operatorbench-review-20261008.zip` and an external
`.zip.sha256` file, checks every compressed payload against its manifest,
and refuses to overwrite an existing ZIP. For a later build, select a new
ZIP filename with `--output artifacts/new-review.zip`.

The ZIP contains scientific code, the paper source, frozen experiments,
dated audits, reviewer instructions, and a copy of the successful reference
reanalysis. Personal drafts, Python environments, downloaded reference papers,
internal reading notes and publication-planning paper notes are excluded.
It includes the frozen revision preparation plan and checksum needed by
`--aggregate-only`. The package is created locally; publishing it is a
separate step and its copied-document contents are not included in the
initial public release.

Verify an existing ZIP with:

```powershell
python -m scripts.package_artifact --verify artifacts/operatorbench-review-20261008.zip
```

This verifies internal file hashes. Compare the external SHA-256 value with
the complete ZIP separately to check the manifest's bytes as well.
