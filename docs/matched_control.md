# Matched criterion and operator control

Status: the full study completed on 8 October 2026. All 800 operator blocks
and 8,000 recorded requests passed the archive audit, and the saved summary
reproduced. This is a follow-up to the frozen study, not an externally
registered protocol or a replacement for its archived results.

The experiment asks two questions through both LOTUS operators:

| Criterion | `sem_filter` | `sem_map` |
| --- | --- | --- |
| Aboutness | Is this paper about machine learning? | Same question |
| Primary topic | Is machine learning the primary topic of this paper? | Same question |

Every cell receives the identical answer-format instruction:
`Respond exactly as Answer: True or Answer: False.` Both operators process all
selected rows, including filter rejections. Raw responses and the framework's
parsed output are retained. Analysis uses one shared strict parser: only
`Answer: True` and `Answer: False` are valid, ignoring case and outer
whitespace. Verbose answers, bare booleans, and missing answers are invalid;
they never silently become False or the filter's default True.

The native framework prompts remain in place. Their system messages and
user-message tags differ, so the result is a contrast between operator access
and associated prompt construction, not a system-message-only causal effect.
The new answer format and binary primary-topic question also differ from the
frozen four-label map. Compare conditions within this experiment; do not
combine its observations with historical cells.

## Execution design

Defaults are the 500 frozen documents, the same 4,000-character windows,
four replicates, and `gpt-4o-mini-2024-07-18`. The category stamps remain in
the text. Temperature is explicitly requested as 0.0, output allowance is
16 tokens, and no provider seed is supplied. This does not guarantee
deterministic execution. A seed controls only scheduling.

In each replicate, rows are shuffled and split into blocks of ten. All four
cells process each block before the next block begins. A rotating order
counterbalances the cells' positions; four replicates balance positions
across the planned run. Conditions share the exact same row order inside
each block. Different row order across replicates and concurrent calls
within a block remain parts of the execution setup.

LOTUS caching is disabled. Each completed operator block has a checkpoint
with UTC start/end times, row IDs, raw responses, parsed answers, model
kwargs, request usage, and hashes binding the plan, records, and request log.
The plan records package versions, installed framework source hashes, dataset
and input-window hashes, and the experiment script hash. Checkpoints are
published only after the operator returns every row and request counts match.
An interrupted block can be retried; failed-attempt request logs are retained.
Provider-level retries may incur additional calls or trigger a coverage
failure. No automatic block retry is performed.

Resume skips validated completed blocks. A changed plan requires a new
directory. Run only one process per experiment directory. Pauses and failed
attempts must be disclosed when interpreting timing differences. Request
logs are bound at block level; they do not currently supply an explicit
provider-request-to-row join. The per-row raw outputs come from LOTUS's
`return_raw_outputs` interface.

## Offline checks and cost estimate

From the repository root in PowerShell:

```powershell
.\.venv\Scripts\python.exe scripts/run_matched_control.py --dry-run
.\.venv\Scripts\python.exe scripts/run_matched_control.py --dry-run --limit 10
.\.venv\Scripts\python.exe -m pytest tests/test_matched_control.py -q
```

The default dry run plans 8,000 row calls in 800 operator blocks. The estimate
uses text characters divided by four plus 128 prompt tokens per call, and the
full output-token allowance. At standard GPT-4o-mini prices of $0.15 per
million input tokens and $0.60 per million output tokens, the estimated
charge is about $1.43. These prices were checked on 7 October 2026 against
the [official model page](https://developers.openai.com/api/docs/models/gpt-4o-mini).
This is a heuristic, not a spending cap; real tokenization, retries, and
latency can differ. Provider prompt-cache discounts are not assumed.

Configured delays alone take about 160 minutes for the full plan. A ten-row
pilot plans 160 calls and estimates about $0.03, with roughly three minutes
of delays plus model latency. It is a pipeline check, not scientific evidence
for the full study. Other models require explicit input and output prices.

## Paid execution, when authorized

The default mode is offline and writes no experiment files. Execution requires
both flags below and an API key already set in the shell:

```powershell
.\.venv\Scripts\python.exe scripts/run_matched_control.py --execute --yes --limit 10
.\.venv\Scripts\python.exe scripts/run_matched_control.py --execute --yes
```

Outputs are isolated under `results/matched_control/n10_r4_s20261007` for the
pilot and `results/matched_control/n500_r4_s20261007` for the full run. Frozen
CSVs, summaries, and logs are untouched. The script writes the immutable plan
before the first model call, then block checkpoints and a final summary.

To reanalyse an existing complete experiment without model calls:

```powershell
.\.venv\Scripts\python.exe scripts/run_matched_control.py --aggregate-only --output-dir results/matched_control/n500_r4_s20261007
```

## Analysis and reporting

Report the operator contrast separately for aboutness and primary topic, and
the criterion contrast separately for filter and map, in every replicate.
For each contrast report valid paired rows, invalid exclusions, directional
discordant counts, decision-difference rate with a Wilson interval, and
signed percentage-point change. Exact McNemar tests assess directional
asymmetry; they are not tests that the configurations are identical. With
no discordant pairs, the raw test field is undefined and the multiplicity
adjustment uses p=1. A Holm adjustment covers the full planned family of
four contrasts across all replicates. Report all contrasts regardless of
significance and do not pool repeated rows into independent trials.

Repeatability reports the rows whose answers change across replicates,
restricted to rows with valid answers in every replicate. Intervals and
tests describe this selected corpus under their row-level assumptions;
they do not establish population representativeness or correctness.
Correlated errors across documents or blocks can limit those assumptions.
There is no equivalence margin, automated mechanism verdict, or historical
noise-ceiling threshold. A small or nonsignificant contrast does not prove
the absence of an operator/framing effect. Human adjudication remains needed
to distinguish correct overlapping descriptions from actual errors.

## Completed results and exploratory sensitivities

Per replicate, criterion changes affect 52--55 filter decisions and 43--45
valid map decisions. Operator changes affect 5--7 aboutness decisions and
13--18 valid primary-topic decisions. The primary-topic operator contrast
has more map positives and remains significant after the original family-wide
correction. Aboutness directional asymmetry is not significant; this does not
prove equivalence or a criterion-by-operator interaction. All 89 strict format
failures are primary-topic map responses containing `Answer: False.`.

The original strict analysis remains primary. Two offline alternatives were
chosen after inspecting the completed run: allowing one terminal period, and
excluding the ten documents in the row block spanning the longest execution
gap from all four conditions of replicate 4. Each alternative recomputes the
full 16-comparison family and applies its own Holm adjustment. These
exploratory corrections do not account for choosing the alternatives or other
post-hoc analyst choices.

```powershell
.\.venv\Scripts\python.exe -m scripts.matched_control_sensitivity
.\.venv\Scripts\python.exe -m pytest tests/test_matched_control_sensitivity.py -q
```

The analysis validates checkpoints and request hashes and reproduces original
contrasts before writing `audits/2026-10-08/matched_control_sensitivity.json`.
It makes no model calls and preserves the original experiment files. Both
alternatives retain the primary-topic operator direction. Allowing the period
adds concordant negatives and leaves its discordant counts unchanged.
Excluding the interrupted block leaves 465 valid primary-topic pairs in
replicate 4, with 2 filter-only and 13 map-only positives; its exploratory
adjusted p-value is 0.0369. Neither check proves correctness or rules out other
formatting or temporal effects.
