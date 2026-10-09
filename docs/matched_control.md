# Matched criterion-by-operator control

The full study completed on **8 October 2026**: 500 documents, four cells,
four replicates, 800 operator blocks and 8,000 recorded requests.

| Criterion | `sem_filter` | `sem_map` |
| --- | --- | --- |
| Aboutness | Is this paper about machine learning? | Same question |
| Primary topic | Is machine learning the primary topic of this paper? | Same question |

All cells use `Respond exactly as Answer: True or Answer: False.` Both
operators process every selected row. The strict parser accepts only these
answers, ignoring case and outer whitespace. Invalid formats are excluded
pairwise and reported separately, never recoded as negatives.

Native framework prompts still differ. The contrast concerns operator access
and associated prompt construction, not a system-message-only causal effect.
The binary task is separate from the historical four-label map.

## Design and evidence

Each replicate shuffles rows into ten-row blocks. All four cells process a
block before advancing, with rotating cell order. LOTUS caching is disabled.
The requested model is `gpt-4o-mini-2024-07-18`, with temperature 0.0, a
16-token output allowance and no provider seed. These settings do not guarantee
determinism.

- [Fixed plan and bindings](../results/matched_control/n500_r4_s20261007/plan.json)
- [Primary summary](../results/matched_control/n500_r4_s20261007/summary.json)
- [Request-to-row/prompt audit](../audits/2026-10-08/matched_control_full_check.json)
- [Exploratory sensitivity analyses](../audits/2026-10-08/matched_control_sensitivity.json)
- [Readable primary and sensitivity tables](../audits/preprint-reanalysis-20261009/TABLES.md)

The primary analysis corrects across four contrasts and four replicates using
Holm's procedure. Sensitivities were chosen after inspecting the results and
use separate correction families. Replicate p-values are not pooled.
See [RESULTS.md](RESULTS.md#2-matched-criterion-by-operator-control) for interpretation.

## Reanalysis and execution

The public repository omits document text, checkpoints and raw request logs.
With the complete local archive, the following commands perform no model calls:

```bash
python -m scripts.run_matched_control --aggregate-only --output-dir results/matched_control/n500_r4_s20261007
python -m scripts.matched_control_sensitivity --output audits/reproduction/matched_control_sensitivity.json
```

Preflight with `--dry-run` additionally needs the original inputs and pinned
framework installation. Execution requires `--execute --yes` and provider
credentials, and makes paid calls. It is not needed to inspect the public reports.
Old cost estimates are not current price quotations or spending limits.

Resume validates plan/checkpoint/request hashes before skipping a block. A
changed plan needs a separate output directory. Source or input mismatches
must not be bypassed to make the recorded experiment appear reproducible.
