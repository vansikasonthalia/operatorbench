# Planned evaluation of revision requirements

Status: protocol preparation only. No new model outputs have been obtained.
This is a local, prospective fixation of prompts and requirements, not external
preregistration. The previously analysed 500-paper corpus informs this design;
the selected documents are not a holdout. Only new executions and revisions
would be prospective. The old results cannot become prospective evidence by
renaming them or splitting the corpus after analysis.

The next run is deliberately limited to 100 rows selected without labels or
model outcomes, four replicates, and seven all-row binary cells. The plan fixes
the selected row IDs, exact 4,000-character input windows, prompts, requirements,
schedule, model identifier, requested settings, analysis and source hashes.
The CSV reader preserves embedded NULs and newlines before taking the input
window; the eventual runner must use the same bytes bound by the input hash.
The inherited pinned model's current availability has not been checked.

## Conditions and intended requirements

| Revision | Admission requirement | Common saved labels | Shared component |
|---|---|---|---|
| Independently executed identical prompts | Equal | Unchanged | None |
| Filter paraphrase: “Does this paper concern machine learning?” | Equal | Unchanged | Reference map answers |
| Narrower filter: substantive research topic, not only background or a passing mention | Subset | Unchanged | Reference map answers |
| Map paraphrase: “Does this paper concern machine learning?” | Equal | Unchanged | Reference filter answers |

The reference question is “Is this paper about machine learning?” Every cell
uses the existing exact `Answer: True/False` instruction and native operator
wrappers. Paraphrase equality and narrowing inclusion are intended requirements,
not established logical relationships. Exceptions do not automatically imply
incorrect model answers. Identical-prompt repetitions provide a comparison for
variation even without changing the question.

All seven cells query every selected row. Derived pipeline views reuse those
answers. A shared map makes common-label preservation hold by construction;
a shared filter makes membership preservation hold by construction. Those
checks must not be presented as empirical achievements. The run would not be
an execution of composed pipelines or a cross-model/cross-framework evaluation.
Cell order rotates across blocks; this does not promise complete counterbalancing
at document level or eliminate temporal effects.

## Baselines and reporting

Compare conditional rates, counts with coverage, common-row paired counts,
and direct record assertions with the ledger-based checker. The direct baseline
in `harness/revision_assertions.py` uses sets and record comparisons without
calling the ledger or its checker. It receives identical requirements and
information. It can recover the same violations; this plan does not posit a
detection advantage for the ledger. Assess agreement and exception localization,
and report any implementation discrepancy before scientific interpretation.

Apply the shared strict parser without treating invalid answers as negatives.
Determine assessability per revision from its required cells, not a global
seven-cell cohort. Report raw invalid responses and unassessed rows explicitly.
Do not certify a full-input requirement using only valid rows. Where a map
answer is not needed because its filter rejects the row, state that distinction
in the eventual runner's frozen evidence schema. Known format failures remain
visible even where semantic comparisons are not evaluable.

Report every revision and replicate: membership changes, common label changes,
both disagreement-change directions, validity, unassessed IDs and requirement
verdicts. Use descriptive counts; do not pool recurring documents as independent
observations. No significance tests, equivalence margins, correction for a
supposed noise floor, or diagnostic-accuracy score are planned. There are no
independent correctness labels. This study can add empirical evidence about
revision behavior; it cannot establish a new assertion paradigm or method
novelty merely by agreeing with direct assertions.

## Preparation and verification

From the repository root:

```powershell
.\.venv\Scripts\python.exe -m scripts.prepare_revision_evaluation
.\.venv\Scripts\python.exe -m scripts.prepare_revision_evaluation --verify
```

The default directory is `artifacts/revision-evaluation-20261008/`. Preparation
refuses to overwrite it; verification checks the exact plan bytes, dataset,
input windows and source bindings. The hash is a local integrity record, not
proof of an externally witnessed preparation time.

The planned run has 2,800 logical row calls and 280 operator blocks. Configured
delays alone sum to 55.8 minutes; model latency and retries add time. Token
estimates are heuristic and exclude exact framework prompt accounting. No
current prices or spending cap are asserted.

This preparation tool makes no provider calls and does not provide an execution
flag. A runner is still required to bind this plan to installed framework/native
prompt hashes, checkpoints and provider requests. It must record invalid outputs,
missing evidence, partial execution, failed attempts, pauses and usage, and must
refuse silent model or protocol substitutions. A changed scientific protocol
needs a separately identified amendment before its outputs are examined. A
transport-only runner added before execution must be bound in a separate run
manifest without changing the frozen scientific requirements. Any pilot remains
separate and is excluded from the study.
