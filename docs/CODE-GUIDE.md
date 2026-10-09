# Code guide

Run modules from the repository root. Existing experiment paths are retained
so that plans, hashes and reports remain interpretable.

## Diagnostic implementation

| Component | Purpose |
| --- | --- |
| [`harness/admission.py`](../harness/admission.py) | Builds row partitions, admission counts, raw/saved validity, label transitions and the disagreement-count identity |
| [`harness/revision.py`](../harness/revision.py) | Checks a ledger against explicitly supplied admission and label-preservation requirements |
| [`harness/revision_assertions.py`](../harness/revision_assertions.py) | Separately implements core checks directly on records |
| [`harness/metrics.py`](../harness/metrics.py) | Repeatability, schema conformance, disagreement, uncertainty and paired context comparisons |
| [`harness/labels.py`](../harness/labels.py) | Loads corrected author-category proxies and prevents accidental use of retired labels |
| [`scripts/admission_ledger_example.py`](../scripts/admission_ledger_example.py) | Runnable synthetic example; no corpus, API key or model calls required |

The ledger takes explicit before/after row mappings and a disagreement rule.
It does not infer the right application criterion or decide which model answer
is correct. Invalid raw output and invalid saved labels are counted separately.

## Experiment and analysis code

| Area | Entry points | Requirements |
| --- | --- | --- |
| Historical operator/pipeline runs | `run_rq1`, `run_rq3`, `run_rq4`, `run_f7_ablation` | Frozen inputs, LOTUS and provider access for execution |
| Historical auxiliary controls | `run_stamp_ablation`, `run_wrapper_test` | Saved outputs for `--aggregate-only`; provider access for execution |
| Matched binary control | `run_matched_control` | Complete matched archive for aggregation; inputs/framework for preflight |
| Revision evaluation | `prepare_revision_evaluation`, `run_revision_evaluation` | Bound preparation inputs and sources; see [runner guide](REVISION-RUNNER.md) |
| Complete saved-output reanalysis | `reproduce_artifact` | Full local archive; no model calls |
| Reporting and requirement comparison | `compare_admission_reporting`, `audit_revision_contracts` | Recorded historical/matched evidence; not new executions |
| Evidence auditing | `audit_call_logs`, `audit_frozen_evidence`, `recompute_evidence_audit` | Relevant saved inputs/logs |
| Local review ZIP | `package_artifact` | Complete verified archive; not a public-release packager |

All entry points in the table are modules under `scripts/`.
The remaining preparation, early pilot and hand-check utilities document the
historical workflow; they do not supply additional evidence for the current
headline findings. They are not quick-start commands.

`configs/pipelines.py` retains the historical criteria. `harness/runner.py`,
`intercept.py`, `logger.py`, `cache.py`, `artifacts.py` and `provenance.py`
handle execution, recording and saved-run validation.

## Environments and frozen sources

- `requirements-artifact.txt`: pandas/NumPy for saved-output analysis.
- `requirements-test.txt`: the public unit-test environment.
- `requirements.txt`: optional LOTUS/model-runner dependencies.
- `requirements.lock.txt` and `results/environment.json`: a historical
  environment snapshot, not proof of every historical run's environment.

The matched/revision plans bind exact source bytes. `.gitattributes` preserves
those bytes across platforms. In particular, the original
[`REVISION-EVALUATION.md`](REVISION-EVALUATION.md) is an immutable preparation
record; its status is historical. Do not update it to describe the completed
run. Current results are explained in [RESULTS.md](RESULTS.md).

Source or protocol changes for a new experiment need a new run identity and
bindings. A newer provider response is not reproduction of the saved response.
