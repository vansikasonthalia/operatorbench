# METHODS — things that MUST appear in the paper

Every item here is a detail that is obvious now and will be invisible in
October. Each is either a reviewer attack surface or a reproducibility
requirement. Written as claims ready to drop into the methodology section.

Last updated: 7 Oct 2026

---

## Model and configuration

- **`gpt-4o-mini` via LiteLLM, hosted OpenAI.** State the sampling configuration
  precisely: the pinned LOTUS constructor declares a default temperature of
  0.0, and the pinned harness source does not override it. The original RQ1 and
  RQ3 manifests are post-hoc or sparse: they do not establish the historical
  runtime package versions or include a per-call observation of the
  provider-applied value. Describe the
  measured flip rates as end-to-end run-to-run variation; this experiment does
  not isolate the source of that variation.

  **Temperature — state it precisely, with its source.** The pinned harness
  does not set temperature. The value relied upon is `lotus.models.LM`'s declared default,
  which in the pinned `lotus-ai==1.2.4` is **`temperature: float = 0.0`**
  (`lotus/models/lm.py`, `LM.__init__`; it is stored into `self.kwargs` and
  forwarded to `litellm.batch_completion`).
  `harness/provenance.py::lotus_temperature_default()` reads that default out of
  the installed package's signature at capture time rather than hardcoding it, so
  the source claim is verifiable against the installed reproduction package;
  it does not establish the package version used for each historical RQ1 or
  RQ3 call.
  **The frozen run records do not provide per-call temperature observations**, so
  the value the provider applied to any individual call is not measured — we
  rely on the framework default as declared in the pinned source.

  Three distinctions to preserve, because an audit caught the paper blurring them:
  1. **The framework default (0.0) is read from the pinned source** — citable.
  2. **What the provider applied to any individual call is not measured.** Do not
     say run metadata verifies temperature; when provenance is captured outside a
     live run, `temperature_observed_on_lm` is `null` and means *unobserved*.
  3. **The F12 ablation's two replicates produced identical raw output on all
     594 logged decisions (500 filter + 94 map; checked 1 Oct against
     `logs/f7_ablation_abstracts_rep{1,2}.jsonl`). That is consistent with
     temperature 0 but does not prove the parameter's value.**
     Cite it as corroboration, never as the source.
- **Runs are replicates, not seeds.** `BACKENDS["hosted"]["seed"]` is `None`,
  so the experiment did not send a seed; the hosted service offers no
  determinism guarantee. `seed` is recorded in the call log as run metadata
  only. Never write "3 seeds" (D-W2-02).
- **Operator-level isolation verified, not assumed.** In LOTUS 1.2.4,
  `sem_filter.__call__` and `sem_map.__call__` default to `strategy=None` (no
  chain-of-thought), `cascade_args=None` (no proxy cascade), `examples=None`
  (no few-shot); the harness passes no overrides. **Gleaning does not exist in
  LOTUS** — it is a DocETL mechanism, relevant only to the DocETL port.
- **Reproduction environment snapshot (28 Sept 2026):** lotus-ai 1.2.4,
  litellm 1.96.0, pandas 2.3.3, Python 3.10.11. This is a global snapshot,
  not per-run evidence for earlier RQ1 or RQ3 calls.

## Data

- **500 rows per domain**, frozen to versioned CSV with SHA-256 manifests,
  never resampled. Reviews are balanced 250/250 by sentiment. Abstracts were
  balanced using the original preparation script's erroneous grouping of
  arXiv label IDs 0--3 as ML; IDs 0 (`math.AC`) and 3 (`cs.SY`) are not ML
  categories. Thus the frozen abstracts are not balanced on the corrected
  author-category proxy (115 ML-declared, 334 non-ML-declared, 51 unknown),
  and proxy estimates describe this selected sample,
  not the arXiv population. The proxy is available for 449/500 rows.
  Reviews `19ed1b1329eaae97`, abstracts `4e273eae5ac4ec16`.
- **Both datasets are truncated to 4,000 characters** before the model sees
  them. The arXiv source contains full papers averaging ~51k characters; the
  limit usually covers the abstract section. Fifteen of 500 reviews exceed
  the limit (median omitted length 802 characters; 10 negative, 5 positive).

## Metrics

- **RQ3 denominator is filter-positive rows only**, frozen in writing before any
  run. Exact wording for the paper: *"We compute the criterion-disagreement rate
  only on rows that passed the sem_filter predicate."* In code and frozen output
  files, `contradiction` remains the historical name for this operational rule.
- **The operational criterion-disagreement predicate is frozen** in
  `configs/pipelines.py`; `contradiction` is the legacy code/artifact name.
  The abstracts rule counts every non-ML output, including invalid labels;
  the reviews rule counts only `positive`. Wider
  reviews definitions (counting out-of-schema labels, or any non-target label) are
  reported as **sensitivity analysis**, never as the headline (D-W2-03).
- **Flip rate is monotonically non-decreasing in replicate count.** Report n
  beside every flip rate; never compare rates computed at different n.
- **Per-replicate proportions use Wilson score intervals at z = 1.96**
  (D-W3-04). For estimands involving repeated rows across replicates (e.g.,
  the pooled admission-weighted disagreement rate in RQ3), a cluster-bootstrap
  interval resamples rows admitted at least once, carrying each row's observed
  replicates together. This interval describes resampling sensitivity conditional
  on the selected sample and observed runs, not population representativeness.
  Persistent disagreement among rows admitted in every replicate is a different
  outcome with its own denominator and Wilson interval. Pooled totals
  across replicates are descriptive event-weighted counts, never given a
  Wilson interval, because the same physical rows recur. Exact wording for the
  paper: *"Per-replicate proportions use Wilson score intervals for a binomial
  proportion at the 95% level; where rows recur across replicates we
  additionally report a cluster-bootstrap interval."* Do not use the normal
  approximation: at 1/500 it returns a negative lower bound, and at 0/154 it
  returns [0, 0], asserting certainty from zero events. Never report a point
  estimate without its interval and n.
- If an event-weighted pooled total is shown descriptively, compute it by summed
  numerator and denominator rather than averaging per-replicate percentages;
  never attach an independent-sample interval to that total.
- **Between-replicate spread (max rate - min rate) is reported alongside the
  per-replicate rates** and is a stability check, not a confidence statement. It exists
  because the hosted study did not set a seed (D-W2-02), so these replicates
  cannot be made identical by seed control, only compared. Abstracts: 0.98 pp
  across n=3; reviews: 0.74 pp across n=3.
- **Persistence is undefined at n=1** — every observed criterion disagreement trivially occurs in
  "every" run. Only report it from 2 replicates.
- **Historical schema violations are measured after `normalize_label()`.** The
  7 Oct audit checked all 30 saved map runs: raw exact-label failure counts
  (whitespace trimmed, case ignored) equal normalized invalid-label counts in
  each run, and every stored normalized label matches the current normalizer.
  Report these as distinct measurements. The agreement of the counts in these
  artifacts does not establish that normalization preserves format compliance
  on arbitrary future responses.

## Execution

- **Reproduction runner default: 10 rows per batch, 12 seconds between batches.**
  The post-hoc or sparse RQ1/RQ3 manifests do not establish the historical
  batch size or delay. Timing was not manipulated as an experimental factor;
  do not claim that changing it cannot affect hosted-service outputs.
- **Rate-limited batches are retried.** When a batch 429s, LOTUS raises and
  the entire 10-row batch is re-sent; the successful attempt's output is what
  gets recorded. This can issue repeated requests for rows in the batch.
  Do not claim absence of selection effects without an analysis of retry
  records; this is a protocol limitation. It materially affected RQ1 abstracts
  replicates 3 and 4, which were heavily throttled.
- **Retry is transport-level only** (HTTP 429, connection errors, timeouts,
  5xx). The model never sees its own prior output; nothing is re-refined. This
  is categorically distinct from gleaning.
- **Tier 1 daily cap** of 2M tokens/day forced runs to be split across days.
  Worth one sentence in the reproduction notes so others can budget: measured
  ~320 tokens/row for reviews, ~1014 for abstracts.

## Limitations to state plainly

1. **Single backend tier.** The local pinned-seed tier was not built (hardware —
   see `FINDINGS.md` G-LOCAL), so serving-stack variance cannot be separated
   from operator-level variation. RQ1 reports observed end-to-end flips and
   does not identify their source.
2. **One model family.** gpt-4o-mini only. No claim about frontier models or
   open-weights behaviour.
3. **Two domains, two operators, one system** (LOTUS). The DocETL port was cut
   (D-W3-03, D-W4-01) and is future work.
4. **The headline is operational criterion disagreement, not logical
   contradiction or error.** `contradiction` survives as the frozen predicate
   and artifact-field name. Aboutness and primary-topic criteria can both be
   satisfied by one input.
5. **Hand-check is limited exploratory evidence.** One coder, not blind to
   the model's prior outputs, no inter-coder agreement, construct validity not
   established, and 35/36 rows coded. Do not present its categories or the
   1/35 GENUINE tally as a validated clear-cut rate.
6. **Sampling and proxy limits.** The frozen abstracts sample was selected
   using an erroneous original label grouping, and author-declared primary
   categories are a proxy for the filter's aboutness predicate. F10/F11 do not
   estimate population-level precision, recall, or prevalence.

## Reproducibility artefacts

- `logs/llm_calls/*.jsonl` — surviving intercepted request records with exact
  prompts, prompt hashes and token usage. Seven files contain 178 malformed
  lines; do not describe this archive as a complete record of every request.
- `logs/{run_id}.jsonl` — operator decisions and raw map outputs, a separate
  layer from provider-request records. The operator logs were deduplicated via
  `scripts/dedup_call_logs.py`; pre-dedup backups remain preserved (D-W2-15).
  All 20 headline runs have complete, matching operator logs, but parsed filter
  decisions cannot replace missing raw provider responses.
- `logs/prompt_templates/template_*.json` — unique prompt structures LOTUS
  constructs, for the appendix.
- `results/*_meta.json` — per-run metadata.
- The original RQ1 and RQ3 manifests are post-hoc or sparse; they do not
  establish the historical package versions or applied per-call temperature.
  RQ3 manifests also omit actual batch delay. Current source defaults must not
  be presented as execution-time observations for those runs.
- Every replicate CSV under `results/` and `results/rq1/`.

### Evidence status after the 7 October audit

| Evidence layer | Established by the audit | Not established |
|---|---|---|
| Saved CSVs | 30 map runs structurally consistent; 14 headline counts match | Independent correctness of labels or representative sampling |
| Offline calculations | Eight analyses reproduce common numerical/boolean fields | Independent validation of each analysis's scientific interpretation |
| Operator logs | All 20 headline runs match saved decisions and outputs | Complete raw request/response reconstruction |
| Request logs | 53 JSONL files inspected, including archived backups; 178 malformed lines in seven current request files | Complete request coverage or a reliable per-row/per-run request join |
| Archive manifest | No missing, extra or changed files relative to `logs_manifest.json` | Parse validity; matching checksums do not repair corrupt lines |
| Historical run provenance | Two of 30 map runs have execution-time provenance blocks; 28 lack them | Historical settings for runs with sparse/post-hoc metadata; provider-applied temperature |
| Current checksums | Audit-time hashes of source evidence; no source files changed during audit/recomputation | Execution-time artifact binding: none of the frozen runs has the new CSV checksum record |

The audit outputs are in `audits/2026-10-07/`. Recomputed summaries were written
to that folder without rewriting frozen results. Future atomic saves, CSV
checksums and evaluation-source hashes are release hardening, not retrospective
evidence about historical execution. The preliminary reviews map log has two
extra IDs relative to its CSV; keep preliminary and smoke artifacts out of
headline claim sources.


---

## Added 14 Sept 2026

### Corrected abstracts labels (`topic_label_v2`)

The frozen `topic_label` is wrong: `prep_datasets.py` guessed
`ml_categories = {0,1,2,3}` for `ccdv/arxiv-classification`, and index 0 is
math.AC while index 3 is cs.SY. Two of four "ML" classes are not ML.

`topic_label_v2` is derived from the authors' declared arXiv primary category,
parsed from the arXiv stamp within the same 4000-char window the operators saw.
Coverage 449/500 (89.8%); the remaining 51 are `unknown` and excluded from every
rate rather than defaulted. On the 449 labelled rows, v1 and v2 disagree on 176
(39.2%).

`prep_datasets.py` was **not** re-run and nothing was resampled (D-09). Row
membership and order are asserted identical; the corrected labels ship as a
separate file. **Disclose the defect and the fix in threats to validity** — it
is the class of error the paper is about, found in the paper's own harness.

### The hand-check sheet displayed the retired label (D-W3-05)

`results/handcheck_abstracts.md` was generated with the F9-defective
`topic_label` printed as each row's "dataset label". On **13 of its 36 rows**
that displayed label disagrees with the corrected author-declared category
(`topic_label_v2`): rows 86, 113, 127, 178, 284, 301, 414 displayed
`machine_learning` for cs.RO / cs.SY / math.OC papers, and rows 164, 221, 228,
347, 430, 491 displayed `other` for cs.NE / cs.LG / stat.ML / cs.AI papers
(recomputed 1 Oct from the sheet and `data/abstracts/abstracts_v1_labels_v2.csv`).
Signal A was coded from the arXiv category on the page, not from the displayed
label; FINDINGS §F12 records rows 164 and 228 as cases where the coded Signal A
(A1) visibly departs from the displayed `other`. The sheet was not regenerated,
so this exposure is disclosed rather than silently fixed.

### The proxy label was visible; a single control found no detectable reliance (F13)

`topic_label_v2` is parsed from the arXiv stamp inside the same 4,000-char
window the operators read (449/449 labelled rows; median position char 80), so
the model could see the category the proxy analyses compare it against. A
control committed before the run (`187ef70`) blanked the stamp with
equal-length spaces and re-ran the frozen abstracts pipeline once (1 Oct 2026).
None of the 648 parseable request records contained a stamp; four malformed
lines prevent a complete request-log audit. The deterministic input control
blanked stamps on all 449 categorized rows. Against `rq3_abstracts_rep1`
there were 3 labelled-row filter flips, below the decision threshold of more
than four but above the largest earlier pairwise difference of two. Proxy
agreement went from 88.64% to 88.86% (McNemar p = 1.0). F10 precision and
recall, the F11 split and the F2 rate all stayed within their intervals.
Wording for the paper: *the category stamp was visible to the model; removing
it did not detectably change any reported measure (one replicate).* Disclose
the exposure and the control together.

### Wrapper control (F14): distinguish the direct contrast from reused cells

The later control asks the baseline aboutness question through `sem_map` with
a True/False answer on all 500 abstracts, comparing it with the frozen
`sem_filter` run. Seven answers differ, all claim-only yes (exact McNemar
p = 0.015625). The reused primary-topic cells differ in output format as well
as operator framing, so their 37 differing rows are a format-plus-wrapper
contrast, not a clean wrapper-effect estimate. Treat the direct comparison as
a scoped operator-framing contrast: it changes operator access and prompt
construction, not solely one system-message string in an otherwise identical
request. One new run does not establish a general causal share or equivalence
of the wrappers. Six malformed request-log lines limit reconstruction, even
though all 500 operator map outputs match the saved CSV.

### Filter precision is agreement with a proxy, not accuracy

`sem_filter` was asked an **aboutness** question ("Is this paper about machine
learning?"); the arXiv category is a **primary-category** declaration. A cs.SY
paper applying neural networks can honestly be both. Report F10 as agreement
with author-declared categories, never as filter error rate.

### RQ4 adds a redundant structured date cue

`submission_month` is parsed from the arXiv stamp in the abstract text. The
injection prepends `[submitted: YYYY-MM]` to the prompt while retaining the
original abstract, so the prose date remains. The intervention therefore tests
whether adding an explicit, normalised date cue alongside the existing prose
mention changes the operators' agreement — it does not isolate schema position
alone, because the original cue is never removed. Write the claim as "adding
a redundant structured date cue did not detectably change agreement" — **not**
as "promoted from prose to a typed column" (the prose cue was never removed)
and **not** as information leakage or prompt injection.

RQ4 runs on abstracts only. The IMDB test split carries no production metadata;
synthesising timestamps would abandon the real-but-irrelevant design. Reviews is
a stated limitation, not an omission.

### Persistence denominator

Persistence is computed over rows admitted in *every* replicate, not over all
admitted rows. Abstracts: 36 ever-contradicted within the 154 always-admitted
rows, against a pooled contradiction count of 110 across three replicates
(36 × 3 + 2 rep2-only events on rows 75 and 215). State the denominator
explicitly or the two numbers appear inconsistent.

### Replicate counts must accompany every rate

Flip rates and "ever disagrees" counts are monotonically non-decreasing in
replicate count; persistence ("disagrees in every replicate") is monotonically
non-increasing. Per-replicate disagreement rates have no such monotonicity.
Always report n beside the rate; never compare rates computed at different
replicate counts.

---

## Label normalisation: a latent nondeterminism, found and fixed

**Disclosed because the paper claims reproducibility, not because a number moved.**

`harness.runner.normalize_label` extracts a label from a verbose model response.
Its documented rule was "search the text for a valid label, last match wins".
The implementation instead iterated `valid_labels` — a **`set`** — and kept the
last label that matched *in iteration order*. Two consequences:

1. For a response naming two valid labels, the winner was decided by set order,
   which is unrelated to the text. On `"not positive, it is negative"` the
   documented rule gives `negative`; the implementation could give `positive`.
2. Python randomises `str` hashing per process unless `PYTHONHASHSEED` is fixed,
   so set order — and the returned label — **could differ between two runs of
   the same script.** Verified directly: across eight hash seeds the old
   implementation returned `negative` on three and `positive` on five.

**Exposure: none.** The 28 Sept audit scanned 2,178 stored raw responses in 14
result CSVs and checked the rewrite old-vs-new on them (0 mismatches). That was
not every stored response: on 1 Oct a re-scan of **all 7,378 raw map responses
across the 28 map CSVs under `results/`** (including all RQ1 replicates) again
found **zero** that name more than one valid label, so the ambiguous branch
never executed and no published figure is affected.

**Fix.** Ordering is now fully determined and never consults set order:
rightmost last occurrence in the text, then longest label, then alphabetical.
Locked by `tests/test_normalize_label.py`, including a test that fails if the
implementation is ever simplified back to a set loop and one that runs the
function under eight `PYTHONHASHSEED` values in separate processes.

**How to describe it in the paper.** A latent nondeterminism in the harness,
found by audit, that could not have affected the reported results and was fixed
before release. That is a reproducibility story in the project's favour — it is
the same class of defect as F9, caught the same way, and it belongs in the
limitations section as evidence the instrumentation was checked rather than
assumed.
