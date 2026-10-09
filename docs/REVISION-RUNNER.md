# Revision evaluation: completed run and runner

The evaluation completed on **8 October 2026** with 280 completed operator
blocks and 2,800 recorded requests. The [results guide](RESULTS.md#3-revision-requirements-fixed-before-new-executions)
summarizes its findings. It uses 100 previously studied documents, not a holdout.

The [preparation record](REVISION-EVALUATION.md) and
[plan](../artifacts/revision-evaluation-20261008/plan.json) remain byte-for-byte
unchanged. They describe the state before execution. Their “preparation only”
wording is not the current run status.

## Public evidence

- [Executed plan](../results/revision_evaluation/n100_r4_s20261008/plan.json)
- [Run manifest](../results/revision_evaluation/n100_r4_s20261008/run_manifest.json)
- [Descriptive summary](../results/revision_evaluation/n100_r4_s20261008/summary.json)
- [Request and result audit](../audits/2026-10-08/revision_evaluation_check.json)

Native prompts, raw request archives and checkpoints are not included in the
public snapshot. The commands below require the complete local archive and
its original inputs. They are not runnable from a public clone alone.

## Offline analysis and preflight

```bash
python -m scripts.run_revision_evaluation --aggregate-only
python -m scripts.run_revision_evaluation --dry-run
```

Aggregation needs only the artifact-analysis dependencies. It verifies the
bound plan, sources, checkpoints, request joins and native prompt hashes,
then regenerates the summary without provider calls.

Preflight additionally needs the pinned LOTUS environment. It constructs the
700 native row prompts using mocked responses and blocks socket connections.
It does not check whether the provider still offers the requested model.

## Execution behavior

The explicit execution command is
`python -m scripts.run_revision_evaluation --execute --yes`, with provider
credentials set in the environment. It makes paid model calls. The saved plan
requests `gpt-4o-mini-2024-07-18`; the runner does not substitute another model.
A new scientific configuration requires a new protocol and run directory.

The runner validates completed blocks before skipping them on resume. Failed
attempts remain recorded and may incur usage when retried. It rejects mismatched
bindings and concurrent execution. Do not delete archived evidence or bypass
hash failures to resume a run.

## Interpretation

Assessability depends on the cells needed by each revision. A map answer is
required where its corresponding filter admits; an invalid answer on a rejected
row is still reported. Unassessed rows are not silently turned into negatives.

Core direct-record assertions and ledger checks use shared parsed records.
Supplemental format and membership checks are shared. Their agreement does not
establish independent correctness or a detection advantage.
