# Running the frozen revision evaluation

`scripts/run_revision_evaluation.py` executes the protocol prepared under
`artifacts/revision-evaluation-20261008/`. The scientific plan and its bound
sources, including `docs/REVISION-EVALUATION.md`, remain unchanged. This document
describes the subsequently implemented runner. No new model results are implied
by preparing or testing it.

Run these commands from the repository root using the existing `.venv`, which
contains LOTUS and its model client. The minimal artifact environment is enough
for saved-output analysis, but cannot construct native prompts or execute LOTUS.

## Offline preflight

```powershell
.\.venv\Scripts\python.exe -m scripts.run_revision_evaluation --dry-run
```

The default command without flags also performs preflight. It verifies the
frozen protocol, constructs all 700 native row prompts using mocked responses,
checks unambiguous row joins, and prints the run binding and planned workload.
Socket connections are blocked. It needs no API key and writes no experiment
files. A model-client warning about falling back to its local cost map is
expected if that client tries to fetch metadata during import.

Preflight does not check availability of the requested model snapshot.

## Execute or resume

With `OPENAI_API_KEY` already set in the PowerShell session:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_revision_evaluation --execute --yes
```

This command makes paid provider calls. It plans 2,800 logical row calls in
280 operator blocks. Delays alone total 55.8 minutes; model latency and retries
add time. The frozen token estimate is approximately 3,158,400 input tokens
plus a 44,800-token output allowance, not a spending cap. Provider retries and
failed requests can incur additional usage.

Outputs go to `results/revision_evaluation/n100_r4_s20261008/`. Repeating the
same command resumes there. It validates each completed checkpoint, its raw
request log and native prompt-to-row joins before skipping that block. A failed
block is attempted again in full on resume; its earlier requests and attempt
record are retained. Requests from an incomplete block may therefore be billed
again. No failed block is retried automatically by the runner itself.

The requested model is `gpt-4o-mini-2024-07-18`. If that snapshot is unavailable,
execution fails rather than substituting another model. A scientific change
requires a separately identified protocol amendment before its outputs are
examined. An alternate output folder must be a nested run directory under
`results/revision_evaluation/` and does not amend the scientific plan.

Use Ctrl+C to interrupt execution. Ordinary failures and interrupts release the
run lock and retain attempt evidence. A force-killed process can leave
`.run.lock`; verify that no process is still using that run before removing only
that lock file. Keep all binding, checkpoint, attempt and request files. The
runner refuses concurrent execution and mismatched source, environment or
prompt bindings. Resolve such mismatches explicitly rather than bypassing them.

## Evidence and analysis

The run stores `plan.json`, `run_manifest.json` and `native_prompts.json` before
calls. The manifest binds the runner, interceptor, installed framework sources,
Python/package versions, native prompts and requested model settings. Completed
`job_*.json` files retain raw responses, strict-parser answers, framework outputs,
request-to-row joins and usage. `attempt_*.json` and corresponding JSONL logs
retain failures and timestamps as well as successful attempts. Gaps between
attempt timestamps expose interruptions; they are not a separate pause log.

After all blocks complete, `summary.json` reports every revision and replicate,
the reporting views, exact exception and unassessed row IDs, and agreement with
the direct-record assertion baseline. Recompute it without model calls using:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_revision_evaluation --aggregate-only
```

Aggregation requires all planned checkpoints and verifies their bindings and
request joins. It also requires the bound runner source to remain unchanged;
it does not silently reanalyse with modified code.

Assessability uses each revision's required cells. Both filter answers must be
valid, and a map answer is needed only where its corresponding filter admits.
An invalid map answer on a filter-rejected row remains reported but does not
exclude that row from the semantic comparison. Required format failures and
known filter-membership exceptions remain visible even outside the joint
assessable cohort. Invalid answers are never recoded as negatives. Full-input
requirements cannot be certified when required answers are unassessed.

The direct baseline receives the same records and requirements. Any disagreement
with the ledger checker stops aggregation before interpretation. The summary is
descriptive; it establishes neither answer correctness, detection accuracy nor
a ledger advantage over direct assertions. The sample reuses previously studied
documents and is not a holdout. Successful checkpoint usage excludes failed
attempts, whose available usage is retained separately; neither count independently
certifies provider billing or hidden retries.
