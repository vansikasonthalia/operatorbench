# FINDINGS LEDGER

Every result, with the exact number, where the evidence lives, and which paper
section it serves. Append as things land. **Nothing here is written from memory
— every figure traces to a file.**

Last updated: 7 Oct 2026

> **7 Oct evidence audit:** all 14 headline count checks and eight offline
> analyses reproduce their frozen numerical results. All 20 headline runs have
> matching operator logs. The request archive has 178 malformed lines across
> seven files; 28 of 30 saved map runs lack execution-time provenance blocks.
> Archive checksums match but do not establish parse validity or complete
> request coverage. See `audits/2026-10-07/REVIEW.md` and the reproducibility
> table in `METHODS-DISCLOSURES.md`.

> **Working evidence ledger, not a manuscript-ready narrative.** F1, the
> current F2 framing, F10, F11, and F12 contain the current headline evidence.
> Several later sections preserve the chronology of earlier analyses and
> retractions; consult their status notes before reusing prose. `contradiction`
> in code and frozen artifacts is the historical name for the operational
> criterion-disagreement rule, not a claim of logical inconsistency.

---

## F1 — Low observed run-to-run variation in RQ1 ✅ COMPLETE

Hosted, gpt-4o-mini, pinned LOTUS source default temperature 0.0 (historical package version and provider-applied per-call value not established by the post-hoc manifests), 500 rows, **5 replicates**, both domains.

| Domain | sem_filter flip | sem_map flip | pipeline flip |
|---|---|---|---|
| reviews | 0.80% [0.31, 2.04] | 1.40% [0.68, 2.86] | 1.49% [0.58, 3.77] (n=268) |
| abstracts | 0.20% [0.04, 1.12] | 3.00% [1.83, 4.89] | 1.30% [0.36, 4.61] (n=154) |

Filter pass counts per replicate — reviews `[271, 270, 270, 271, 271]`,
abstracts `[154, 155, 155, 154, 154]`. A one-row spread across five runs.

**Fleiss' κ (added 1 Oct; closes G2).** Replicates as raters, 500 rows each.
`scripts/rq1_fleiss_kappa.py` → `results/rq1_fleiss_kappa.json`; the function
was checked against Fleiss's textbook example (κ = 0.210).

| Domain | filter κ | map κ |
|---|---|---|
| reviews | 0.991 (P̄ 0.996, Pₑ 0.503) | 0.989 (P̄ 0.994, Pₑ 0.445) |
| abstracts | 0.997 (P̄ 0.999, Pₑ 0.573) | 0.979 (P̄ 0.986, Pₑ 0.308) |

The high agreement survives the chance correction, so it is not an artifact of
skewed labels. Replicates are unseeded executions, not independent raters;
report κ beside the flip rate, never instead of it.

**Evidence:** `results/rq1_hosted_summary.json`,
`results/rq1/hosted_{domain}_rep{1..5}_{filter,map}.csv`
**Section:** Results / RQ1. **Role:** characterizes individual output variation;
it does not explain or validate RQ3's disagreement measure.

**Caveats that must ship with it:**
- Flip rate is monotonically non-decreasing in replicate count. Abstracts map
  read 2.00% at n=3 and 3.00% at n=5. Always report n beside the rate.
- The pinned LOTUS source declares a temperature default of 0.0, and the
  harness does not override it. The post-hoc or sparse RQ1/RQ3 manifests do not
  establish historical package versions or contain per-call observations of
  the provider-applied value. Report the declared default, and
  describe the flips as end-to-end run-to-run variation; their source is not
  isolated by this experiment.
- Hosted tier only. The local pinned-seed tier is not built (see G-LOCAL).

---

## F2 — Cross-operator criterion disagreement (RQ3) ✅ COMPLETE at n=3

**Replicate 3 landed 28 Sept. This is the final headline — the run programme for
RQ3 is closed.** All figures below verified independently against the replicate
CSVs, not transcribed from console output.

### ⚠ WHAT THE 23% IS, AND WHAT IT IS NOT — read before writing any claim

**The metric is a DISAGREEMENT rate under a stated operational rule, not a
logical contradiction rate.** This is the single most important framing decision
in the paper and getting it wrong is the fastest way to lose a reviewer.

**The operational rule, stated so it can be argued with:** a row counts as a
disagreement when (a) `sem_filter` admits it under the predicate *"Is this paper
about machine learning?"*, and (b) `sem_map`, under *"Classify the primary
topic"*, returns a label the frozen contradiction predicate treats as
inconsistent with admission. That rule counts **any** non-ML primary-topic label
as a disagreement.

**Why that is not a contradiction in the logical sense.** "This paper is about
machine learning" and "this paper's primary topic is theory" are **jointly
satisfiable**. A paper can be about ML and primarily a theory contribution — and
the single-coder exploratory classification suggests that this is usually what
is happening. So the 23% measures
*the two operators applying different criteria to the same concept*, which is the
actual finding, and calling it a contradiction rate overstates it.

**What the single-coder classification recorded** (35 coded rows of the 36-row
persistence set, `results/handcheck_reconciliation.json`):

| verdict | n | reading |
|---|---|---|
| FILTER-WRONG | 19 | the filter admitted a paper the authors did not declare as ML |
| AMBIGUOUS | 15 | both answers defensible — genuinely two criteria |
| **GENUINE** | **1** | meets a contradiction reading on its own terms |

**So: report 23% as a criterion-disagreement rate under the rule above. One row
was coded GENUINE by the single coder, but that exploratory label is not an
independently adjudicated ground truth.** Where the existing
scripts, column names and result JSONs say `contradiction`, that is the
*operational* sense; say so once in Methods and map the terms explicitly rather
than renaming frozen artifacts.

**And do NOT conclude that the model is never incoherent.** Two reasons, both
fatal to that sentence:
1. **The ledger contains one genuine case.** One is not zero. Report it as one.
2. **The codebook is not ground truth.** One coder, a deterministic matrix whose
   validity was never independently established, and the coder had seen the voided
   LLM-assisted pass. That supports *limited exploratory evidence about the
   character of the disagreements* — nothing stronger. See the limitation in F6.

The defensible claim is about the **specification**, not the model's competence:
these two operators were given different criteria for one concept, the framework
surfaced nothing, and the disagreement rate is large and reproducible.

| Domain | Reps | Filter-positive | Operational disagreements (stored as contradictions) | Rate | 95% CI | Spread |
|---|---|---|---|---|---|---|
| abstracts | **3** | 464 obs / **156 rows** | 110 | 23.71% descriptive | ~~[20.06, 27.78]~~ see retraction | 0.98 pp |
| reviews | **3** | 814 obs / ~272 rows | 18 | 2.21% descriptive | ~~[1.40, 3.47]~~ see retraction | 0.73 pp |

Per-replicate — abstracts 23.38% (36/154), 24.36% (38/156), 23.38% (36/154);
reviews 2.58% (7/271), 2.21% (6/272), 1.85% (5/271).

**Superseded n=2 figures, kept so old drafts can be spotted:** abstracts 23.87%
[19.46, 28.92] on 74/310; reviews 2.39% [1.40, 4.05] on 13/543; asymmetry 10.0x.
**Every one of those must be replaced wherever it appears** — the dossier,
`paper/PAPER-PLAN.md`, and any prose already drafted.

**⚠ RETRACTED 28 Sept, SAME DAY — the pooled interval above is WRONG and must
not be used.** An external audit caught it and the audit is right. I had written
here that "the third replicate tightened the interval without moving the
estimate." That was a mistake, and it is exactly the mistake a reviewer looks
for.

**Why it is wrong.** The earlier findings narrative (above) applied a Wilson
interval to pooled counts across replicates. (`rq3_summary.json` correctly
marks pooled totals as descriptive with "a pooled Wilson interval is invalid";
the error was in the prose, not the script output.) But n=464 is **156
distinct rows counted ~2.97 times each**, on a frozen dataset. Wilson assumes
independent Bernoulli trials. These repeats are repeated and dependent: 154 of 156 rows are admitted in all three replicates,
and 36 of 36 disagreeing rows disagree in all three (F3's 100% persistence
confirms the dependence). The effective sample size is ~154, not 464.

**Recomputed 28 Sept by `scripts/rq3_uncertainty.py` (no API calls). Two
approaches (different estimands; see note below):**

| ABSTRACTS | k/n | rate | 95% interval | width |
|---|---|---|---|---|
| ✗ pooled Wilson (INVALID) | 110/464 obs | 23.71% | [20.06, 27.78] | 7.72 pp |
| ✓ Wilson, distinct rows | 36/154 rows | 23.38% | **[17.39, 30.66]** | 13.27 pp |
| ✓ cluster bootstrap | 156 rows | — | **[17.24, 30.47]** | 13.23 pp |

| REVIEWS | k/n | rate | 95% interval | width |
|---|---|---|---|---|
| ✗ pooled Wilson (INVALID) | 18/814 obs | 2.21% | [1.40, 3.47] | 2.07 pp |
| ✓ Wilson, distinct rows (always) | 5/270 rows | 1.85% | **[0.79, 4.26]** | 3.47 pp |
| ✓ cluster bootstrap | 272 rows | — | **[0.74, 4.05]** | 3.31 pp |

**Bootstrap parameters, for the paper:** B = 20,000 resamples, seed = 20260928,
`scripts/rq3_uncertainty.py`. **Estimand:** the criterion-disagreement rate among
filter-admitted row-observations, with distinct rows as the resampling unit.
**The 36/154 Wilson figure is explicitly conditional on rows admitted in every
replicate** — say so wherever it appears; it is not the rate over all admitted
rows.

The bootstrap resamples distinct rows with replacement, carrying all of a row's
replicate observations together, so it respects the repeated-measures structure
without assuming anything about persistence. Its interval lands within 0.2 pp
of the row-level Wilson interval on both domains; treat this as a robustness
check, not independent validation, since both estimates use the same rows.

These intervals quantify row-level uncertainty within each benchmark setup.
Their non-overlap does not isolate a domain effect: corpus, predicate,
instruction, and label set differ together. The pooled 10.7x ratio is a
descriptive contrast between setups, not an inferential estimate of a domain
effect or a general rate ratio.

**Estimand note — it matters for reviews, not abstracts.** "Always contradicts"
and "ever contradicts" are identical on abstracts (36/154 both ways) because
persistence is 100%. On reviews they diverge: always 5/270 = 1.85%, ever
7/270 = 2.59%. **Say which one you are reporting for reviews**, and prefer
"always", which is the conservative reading.

RQ1 flip rates and RQ3 disagreement rates answer different questions and use
different denominators. Do not compare RQ3 interval bounds with an RQ1
flip-rate estimate as though either were a noise threshold for the other. The
review and abstract intervals describe these two benchmark setups; they do not
establish a general domain effect.

The pooled rate stays as a **descriptive summary only**. Never attach an
independent-sample interval to it. Same correction applies to reviews:
18/814 pooled is 814 observations of ~272 distinct rows.

**Abstracts replicate 3 admitted 154 rows and contradicted on 36 — identical
counts to replicate 1.** Two of three replicates agree exactly; the third differs
by two admitted rows. Say this plainly, it is a stability result.

**The abstracts/reviews ratio is 10.72x** at n=3 (was 9.97x at n=2, 12x in the
Week 1 preliminary). Quote **10.7x** — but **not as a domain effect.**

⚠ **This is a difference between two benchmark setups, not an isolated effect of
domain.** The two setups differ in data source, filter predicate, map
instruction, label set and task simultaneously:

| | abstracts | reviews |
|---|---|---|
| filter predicate | "about machine learning?" | "is this review negative?" |
| map instruction | classify primary topic | classify sentiment |
| label set | ML / systems / theory / other | positive / negative / neutral |
| label-set structure | has a catch-all (`other`) | no catch-all |

Any of those could produce the gap. The honest wording is *"the disagreement rate
differs by 10.7x between these two benchmark setups"*. Criterion ambiguity is a
plausible interpretation; F12 tests one filter-criterion intervention, while
F7 motivates but does not test the wrapper-framing hypothesis.

**What would isolate domain:** matched tasks holding predicate structure and
label set fixed while varying only the corpus. That is a design we did not run.
List it as future work, not as a finding.

**Schema violations needed the same correction, and it is now done.**
`scripts/rq3_uncertainty.py` (extended 28 Sept). My earlier figures — abstracts
"0/464, upper bound 0.82%" and reviews "1.84% [1.12, 3.02]" — used repeated row
observations and were wrong in exactly the way the contradiction interval was.
Row-level replacements:

| | pooled (descriptive) | per-replicate | ever violates | always violates |
|---|---|---|---|---|
| abstracts | 0/464 obs | 0, 0, 0 | 0/154 → **[0, 2.43%]** | 0/154 → [0, 2.43%] |
| reviews | 15/814 obs | 5, 6, 4 | 5/270 → **[0.79, 4.26]** | 3/270 → [0.38, 3.22] |

**The honest abstracts upper bound is 2.43%, not 0.82% — three times wider.**
Never write "zero schema violations"; write "no violations observed in 154 rows
across three replicates, upper bound 2.43%".

**And this sharpens F4 rather than weakening it.** The 15 pooled reviews
violations are only **6 distinct rows** (5 of them among the 270 rows admitted
in every replicate, which is the row-level denominator above), and **3 violate
in every single replicate**. The violations are not spread thinly across the corpus —
they are a handful of specific inputs that break the schema reproducibly, which
is precisely F4's claim, now with a distinct-row denominator behind it.

**⚠ PERSISTENCE MOVED, AND THE DIRECTION IS EXPECTED.** Reviews fell from 6/7
(85.7%) at n=2 to **5/7 (71.4%)** at n=3. Abstracts held at **36/36 (100%)**.
Persistence is **monotonically non-increasing in replicate count** — every extra
replicate is another chance for a row to fail to contradict — exactly the same
caveat class as F1's flip rate being monotonically non-decreasing. **Report n
beside every persistence figure and never compare figures computed at different
n.** The difference between these two benchmark setups is descriptive; it does
not establish an intrinsic domain-level difference in persistence.

The persistence denominator is rows admitted in *every* replicate, which is why
abstracts reads 36/36 and not 36/38 — rows 75 and 215 contradict only in
replicate 2 and are not admitted throughout. State the denominator or the
numbers cannot be reconciled.

**Evidence:** `results/rq3_{abstracts,reviews}_rep{1,2,3}_map.csv`,
`results/rq3_summary.json` (now correctly holds BOTH domains)
**Section:** Results / RQ3 — headline.

**⚠ F12's ablation is paired against replicates 1 and 2 only**, since it ran
before replicate 3. The comparison remains valid — each ablation replicate is
paired to its own baseline — but disclose that the ablation baseline is n=2
while the headline is n=3.

## F3 — Row-level disagreement persistence in the frozen sample

All 5 Week 1 reviews contradictions (rows 93, 124, 154, 215, 349) reappeared in
the 12 Aug run, two days and a code change later, plus 2 new ones (72, 397).

This establishes that the same rows repeatedly met the operational rule in
these frozen runs. It describes repeatability within this selected corpus; it
does not establish a population property or an intrinsic input-level cause.

**Current persistence uses all three frozen replicates.** The earlier two-run
table is superseded; the values below use the three-replicate intersection.

| Domain | Reps | Admitted in every rep | Ever disagreed | Disagreed in every rep | Persistence |
|---|---|---|---|---|---|
| abstracts | 3 | 154 | 36 | 36 | **100%** |
| reviews | 3 | 270 | 7 | 5 | **71.4%** |

Every abstracts row that ever disagreed did so in all three replicates. In
reviews, 5 of 7 ever-disagreeing rows did so in every replicate. This descriptive
difference does not identify a domain effect; the tasks differ in several ways.

**Denominator note that must ship:** persistence is computed over rows admitted
in *every* replicate (154 of the 500), not over all admitted rows. That is why
abstracts shows an ever-contradicted set of 36 while the pooled count over three
replicates is 110. The arithmetic closes exactly:

    36 persistent rows x 3 replicates            = 108
    + 2 events on rows admitted in rep2 ONLY     = 110  (rows 75 and 215)

Rows 75 and 215 are precisely the two rows rep2 admitted and rep1/rep3 did not,
so on the always-admitted 154 the "always contradicts" and "ever contradicts"
sets are *identical* at 36. State this, or a reader will try to reconcile 36
against 110 and conclude the arithmetic is broken.

F3 has moved from a hand comparison of two runs to a metric with a denominator.

**Evidence:** `results/rq3_uncertainty.json` and the three frozen replicate CSVs.
**Section:** Results / RQ3 — systematicity.

---

## F4 — Observed schema violations in the reviews setup (RQ2) ✅ CONFIRMED

**Lead with per-replicate and distinct-row figures.** The pooled count is
descriptive only — the same rows recur across replicates, which is the project's
own standing warning (D-W3-04), and it applies to this finding too.

**Per replicate (reviews), recomputed on all three frozen replicates:**

| replicate | violations | rate | labels | rows |
|---|---|---|---|---|
| rep1 | 5 / 271 | 1.85% | **100% `mixed`** | 236, 259, 299, 406, 429 |
| rep2 | 6 / 272 | 2.21% | **100% `mixed`** | 236, 259, 299, 397, 406, 429 |
| rep3 | 4 / 271 | 1.48% | **100% `mixed`** | 236, 299, 397, 429 |

Spread 0.73 pp. *(Week 1, a separate earlier run: 6/269, also 100% `mixed`.)*

**By distinct row — the denominator to quote for "how many inputs break it":**

| | n | rows |
|---|---|---|
| ever violate | **6** | 236, 259, 299, 397, 406, 429 |
| violate in **every** replicate | **3** | 236, 299, 429 |

**Pooled, descriptive only: 15 row-observations across 3 replicates, 15/15
`mixed`.** Do not put a Wilson interval on that 15; the effective n is 6 distinct
rows.

Across these three runs, **all 15 violation events produced the same
out-of-schema label, `mixed`**. Six distinct rows appeared at least once and
three appeared in every replicate. This is a descriptive row-concentration
pattern in the frozen review setup; it does not establish why those rows
produce that output or imply a general schema failure mechanism. The Week 1
run is historical and is not pooled with the frozen replicates.

Abstracts show **0 violations in every replicate** (0/154, 0/156, 0/154; 0
distinct rows). Although the abstract schema includes `other` and the review
schema does not, the tasks, predicates, instructions, and corpora also differ;
this comparison does not isolate a label-set effect. Map label distributions
are reported in `results/rq2_schema_violation_analysis.md`.

**Current interpretation:** schema violations and in-schema criterion
disagreements are distinct outcomes. The cross-domain comparison does not show
that schema design causes one outcome rather than the other. A matched
intervention varying only schema structure would be needed to test that claim.

**Evidence:** `results/rq3_summary.json`, the frozen replicate CSVs, and
`scripts/run_rq3.py`. `results/rq2_schema_violation_analysis.md` is a
historical Week 2 analysis and its causal interpretation is superseded.
**Section:** Results / RQ2, and Discussion.

---

## F5 — Row 397: one input, two observed output classes

In the frozen runs, row 397 maps to `positive` in replicate 1 and `mixed` in
replicates 2 and 3. Under the frozen review rule, `positive` is an
in-schema criterion disagreement, while `mixed` is a schema violation.

**What it shows:** under the frozen review setup, one input produced both an
out-of-schema label and an in-schema criterion disagreement across runs. This
illustrates how the frozen rule classifies these outputs differently. It does
not establish that the schema caused the output change or that the headline is
a general lower bound on semantic error.

**Graded bracket (Week 1 reviews):** strict 1.86% → +out-of-schema 4.09% →
any non-negative 9.29%.

**Section:** Discussion — report the bracket as sensitivity analysis. **Never
widen the frozen predicate** (D-W2-03).

---

## F6 — Are persistent disagreements logical contradictions or ambiguous inputs? 🔀 ANSWERED DIFFERENTLY (28 Sept — see F12)

**STATUS 28 Sept: CODED AND RECONCILED. The gate is closed and it failed.**
*(The "NOT CODED 14 Sept" status this section used to carry is superseded.
Everything below the horizontal rule in this section is the pre-coding record,
kept because it states the expectation in advance; read it as historical.)*

**Canonical row set — one denominator, stated once.** The worksheet was built
from the preliminary run, which is not the frozen data. The paper reports on the
**persistence set: the 36 rows that contradict in all three frozen replicates.**
That set equals rep1's set exactly (rep1 == rep3 == the three-way intersection),
so it is the same 36 that F3 reports as 100% persistence and F12 takes from 36
to 4. F3, F12 and the hand-check now share a denominator.

| | rows | |
|---|---|---|
| persistence set — contradicts in rep1 and rep2 and rep3 | **36** | **the denominator** |
| coded on the worksheet | **35** | coverage 97.2% |
| in the set, absent from the worksheet | 1 (row **117**) | uncoded; cannot change the gate |
| on the worksheet, in **no** frozen replicate | 1 (row **86**) | excluded, correctly left uncoded |
| contradicts in rep2 only — not persistent | 2 (rows **75, 215**) | excluded by the stated rule |
| union, "ever contradicts" | 38 | context only, never this denominator |

**Result over the 35 coded rows:**

| verdict | n | % of 35 |
|---|---|---|
| FILTER-WRONG | 19 | 54.3% |
| AMBIGUOUS | 15 | 42.9% |
| **GENUINE** | **1** | **2.9%** |

Signal cross-tab, n = 35 — **this is the table for the paper:**

| | B1 | B2 | B3 | total |
|---|---|---|---|---|
| A1 (ML declared) | 1 | 9 | 2 | 12 |
| A2 (non-ML declared) | 4 | 16 | 3 | 23 |
| **total** | **5** | **25** | **5** | **35** |

Three rows (23, 202, 438) carried no arXiv tag. The codebook's A3 is a
*resolution procedure*, not a terminal category — ML-relevance claim found → A1,
not found → A2 — and all three resolved to A2 with a written justification per
row. They occupy A2 cells; **there is no A3 row**, and any table showing one is
wrong.

**Gate: pre-registered as GENUINE > 18 of 36. Observed 1. Maximum possible if
row 117 codes GENUINE: 2. The gate FAILS, and the outcome is invariant to the
uncoded row** — which is why it is recorded as evaluated rather than pending.

**One of the 35 coded cases was classified GENUINE by the single coder.** This
is a descriptive tally within a selected set of persistent disagreements. Do
not divide it by the full admitted cohort, compare it to the operational
disagreement or RQ1 flip rates, or present it as a population-level rate.

**Coding integrity (assistant's permitted role under D-W2-14 — build the sheet,
check consistency, recompute rates; never adjudicate):** 35/35 coded rows agree
with the codebook matrix. Zero per-row overrides.

### ⚠ THE CODING IS EXPLORATORY EVIDENCE, NOT VALIDATED MEASUREMENT

Internal consistency is not validity. Everything above says the coder applied the
codebook correctly; none of it says the codebook measures semantic contradiction.
Four limitations, all of which must appear in the paper wherever the
GENUINE/AMBIGUOUS/FILTER-WRONG split is used:

1. **One coder.** No inter-rater agreement statistic exists, because there is no
   second rater. κ cannot be reported.
2. **Construct validity never established.** The codebook crosses Signal A
   (author-declared arXiv category) with Signal B (contribution-verb cluster) in
   a fixed matrix. It was designed to be *executable without domain expertise* —
   which is why it replaced the unexecutable Rule M / Rule F rubric — but nothing
   demonstrates that A×B captures logical contradiction. It is a defensible proxy
   for "does the filter's admission look right and is the contribution empirical",
   and that is a weaker construct than the label names suggest.
3. **Not blind.** The coder had seen the voided LLM-assisted pass before coding,
   so anchoring cannot be excluded.
4. **Coverage 35 of 36**, with row 117 uncoded.

**Therefore, two acceptable ways to write this, and no third:**
- **(a) As limited exploratory evidence** — "a single-coder pass under a
  pre-registered codebook suggests most persistent disagreements are criterion
  mismatches rather than logical contradictions" — with the four limitations
  stated. This is the honest option given the time available.
- **(b) Promote it to a validated measurement** — recruit ≥2 independent coders,
  blind them to the model outputs where feasible, report agreement (κ), and argue
  the construct explicitly.

**If the GENUINE/AMBIGUOUS split is asked to carry the paper's central
interpretation, (b) is required.** The way to avoid needing (b) is to keep the
central claim resting on F10/F11/F12 — which use an **external, author-supplied**
proxy and no adjudication at all — and to use the coding only to characterise
*what kind* of disagreement these are. That is the structure the paper should
adopt: the load-bearing evidence needs no human judgement, and the human judgement
is offered as colour, clearly labelled.

**Three corrections to my own earlier reporting of this hand-check, recorded so
the error does not survive into the paper:**
1. I reported **GENUINE = 2**. It is **1** (row 305). Row 86's *unfilled*
   placeholder `GENUINE / AMBIGUOUS / FILTER-WRONG` matched my parser on its
   first alternative.
2. I reported a **codebook violation on row 86** (A2 × B1 coded GENUINE, matrix
   says AMBIGUOUS). No such violation exists. Row 86 is uncoded. Same artifact.
3. I reported a **contradiction between a 36-row cross-tab and a 35-row tally**.
   There is none. The worksheet's printed tab totals 35 and is correct; my
   spurious 32 came from a parser that ignored the `Signal A override` lines.

All three were mine, not hers. The worksheet is internally consistent as coded.

**What changed while it sat uncoded:** F10 and F11 below now carry the
mechanism with no adjudication at all. The coding pass still matters for the
taxonomy and for the record, but the paper no longer depends on its outcome.

**Adjudication rules (Vansika's, stated 12 Aug):**
- Map question: *if ML is primarily the application context rather than the
  method/contribution, classify as NOT ML.* A primary-contribution test.
- Filter question: a looser aboutness test — the filter was asked "is this
  **about** ML?", not "is ML the primary topic?". Holding it to the primary-
  contribution standard would be judging it against a question it wasn't asked.

**Prior from the data:** of all 36 contradictions, 21 carry author-declared
NON-ML arXiv categories, 12 are ML categories, 3 unknown. The tally may lean
AMBIGUOUS or FILTER-WRONG. *(Verified 12 Aug against
`results/rq3_preliminary_abstracts_map.csv`: 21 / 12 / 3 confirmed;
36 of 155 = 23.23% confirmed.)*

**RESOLVED 12 Aug — the sampling concern no longer applies.** An earlier note
here warned that a 20-row worksheet under-represented ML-category rows. The
worksheet was subsequently regenerated **without `--n`, so it now contains all
36 contradictions**. Verified by parsing `results/handcheck_abstracts.md`:
36 rows, composition 12 ML / 21 NON-ML / 3 unknown — identical to the
population, because it *is* the population. Nothing is excluded and there is no
sampling question to defend.

That note also claimed no script generates the worksheet. Incorrect:
`scripts/make_handcheck_sheet.py` does — seeded, committed, arXiv category
surfaced per row. **Regenerating it overwrites any verdicts already entered.**
None were lost this time because none had been entered; do not re-run it once
adjudication has started.

**Second correction, 12 Aug — the rubric was replaced with a codebook (D-W2-16).**
An LLM-assisted pass over all 36 was produced and **discarded** under D-W2-14.
Root cause worth recording: the Rule M / Rule F rubric required judging whether a
contribution was "ML-style", which needs domain expertise the coder does not
have, so the rubric was unexecutable by the only person permitted to execute it.
The replacement reads two observable signals off the page — **A** (author-declared
arXiv category) and **B** (contribution-verb cluster) — and crosses them in a
deterministic matrix with no hesitation clause. Coders need no domain expertise;
that is the standard justification for codebooks and it is what makes the
constraint satisfiable rather than merely stated.

Signal A over the 36: **A1 (ML declared) 12, A2 21, A3 (no tag) 3.**

**Expected outcome, stated in advance so it cannot be fitted afterwards:**
GENUINE requires A1 + B1 (declared ML *and* trains/evaluates something). Most of
the 12 A1 rows look B2/B3 on their verbs — theorems about bandits, DPPs,
ResNets, HMM decoding. Expect GENUINE in the low single digits, i.e. **the gate
almost certainly fails.** Coding proceeds for the record and the taxonomy, not
as a rescue.

**If it leans AMBIGUOUS, that is not a failure** — see F7.

---

## F7 — Prompt framing: observed asymmetry, untested mechanism

> **Update 1 Oct: component (a), the wrapper, has now been tested — see F14.**
> Holding the question fixed, the claim-verification wrapper is slightly more
> inclusive (7 vs 0 rows, one run). The criterion effect is several times
> larger. The text below predates that test.

In the pinned LOTUS source, `sem_filter` is framed as claim verification and
`sem_map` as instruction following. The prompts differ, and the user-authored
criteria in this study also differ: an aboutness predicate versus a
primary-topic classification instruction. This motivates a hypothesis about
how prompt framing and criterion choice may shape outputs; inspecting the
prompts alone does not establish an inclusive/exclusive behavioral bias or
explain the RQ3 rates.

F12 changes the filter criterion while leaving both LOTUS wrappers fixed. It
therefore measures the effect of that criterion intervention in this setup,
not the contribution of LOTUS's wrapper asymmetry. F10/F11 show an association
with an external author-category proxy, not human-adjudicated mechanism
evidence. Do not state that F7 predicts all observed findings or that it
establishes a general mechanism.

**Evidence:** LOTUS 1.2.4 source, intercepted prompt examples, and
`results/lotus_prompt_analysis.md`. **Section:** Background; mechanism status
and limitations must accompany any discussion of the prompt distinction.

---

## F8 — ⛔ RETRACTED WITHIN THE HOUR — the ground-truth labels are wrong

**Do not cite any number in this section.** See F9 below. Kept only as a record
of what was computed and why it was withdrawn.

Computed 12 Aug from the frozen dataset's labels — which turn out to be
mis-derived (F9). Manifest: 250 `machine_learning` / 250 `other` over 500 rows.

**sem_filter** ("Is this paper about machine learning?"), abstracts:

| | admitted | rejected |
|---|---|---|
| truly ML | 95 | 155 |
| truly other | 60 | 190 |

- **Precision 61.29%** — false-admission **38.71%** [31.40, 46.56]
- **Recall 38.00%** — it rejected **155 of 250** genuine ML papers
- F1 ≈ 0.47

**sem_map** on the same 155 filter-positive rows: agrees with the frozen label
**101/155 = 65.16%**. Of the 60 wrongly-admitted rows the map caught 21 (35.0%);
of the 95 correctly-admitted rows it wrongly called 15 non-ML (15.8%).

**The headline contrast, and it needs no hand-check:** F1 measures sem_filter's
flip rate on abstracts at **0.20%** — it returns the same answer essentially
every time. That answer is **wrong 38.7% of the time**. *An operator can be
99.8% reproducible and 61% accurate.* Determinism is not reliability, and a
pipeline that reruns cleanly tells you nothing about whether it is right.

**Evidence:** `data/abstracts/manifest.json` (sha256 verified against the file
on disk, `4e273eae5ac4ec16…`), `results/rq3_preliminary_abstracts_{filter,map}.csv`
**Section:** likely the new headline.

**Caveats to check before this ships:**
- How were the frozen `machine_learning` / `other` labels constructed? If they
  are themselves arXiv-category-derived, precision and recall inherit whatever
  that proxy gets wrong. **Verify before writing.**
- Abstracts are truncated to 4000 chars (D-10). Truncation is a candidate
  explanation for some of the 155 misses and must be ruled in or out.
- Reviews domain not yet computed. Do it — the comparison is free.

---

## F9 — 🐛 THE ABSTRACTS GROUND-TRUTH LABELS ARE WRONG

`scripts/prep_datasets.py` builds `topic_label` from
`ccdv/arxiv-classification` with:

```python
ml_categories = {0, 1, 2, 3}   # cs.AI, cs.CL, cs.CV, cs.LG typically
df["is_ml"] = df["label"].isin(ml_categories)
```

The comment says "typically". It guessed, and the guess is wrong. Verified
12 Aug against the dataset's own viewer on HuggingFace, which shows the class
names beside the indices:

| index | actual class | counted as ML? | correct? |
|---|---|---|---|
| 0 | **math.AC** (Commutative Algebra) | yes | ❌ |
| 1 | cs.CV | yes | ✅ |
| 2 | cs.AI *(added 1 Oct for completeness — re-confirm against the HF viewer)* | yes | ✅ |
| 3 | **cs.SY** (Systems and Control) | yes | ❌ |

Two of the four "ML" classes are not ML. The dataset has 11 classes and does
not appear to contain `cs.LG` as its own class at all.

**Confirmed in our own data.** Every `cs.SY` and `cs.RO` contradiction row
carries `topic_label = machine_learning`: rows 86, 113, 127, 284, 301. Those are
control-theory and robotics papers labelled as machine learning by the harness.

**What this invalidates:** anything using `topic_label` as truth for abstracts.
That is F8 in full — its precision, recall, and "wrong 39%" interpretation are
void. Many rows previously called "missed ML papers" carry math.AC categories,
but that category is still only a proxy and does not establish whether the
aboutness filter was right or wrong.

**What this does NOT touch:**
- **RQ1** — measures agreement between replicates. Never reads `topic_label`.
- **RQ3 contradictions** — filter output vs map output. Never reads `topic_label`.
- **RQ2 schema violations** — label *format*, not correctness.
- The RQ3 operational metric definition, F1–F5, and the hand-check remain
  reportable with their current scope and caveats. The preliminary 23.2%
  single-run figure is superseded; use the frozen per-replicate values in F2.

**Fix without breaking D-09.** Do not re-run `scripts/prep_datasets.py` and do not
resample. The arXiv primary category is present in `abstract_text` itself and is
already extractable (used for Signal A). Derive a corrected label column from
the text, keep the 500 rows and their order identical, ship it as
`topic_label_v2` alongside the original, and document the correction. Row
membership is unchanged, so the freeze holds.

**Also check reviews.** Its labels come from IMDB's own `label` field via an
explicit `{0: negative, 1: positive}` map, which is the documented IMDB
convention — likely fine, but verify rather than assume, given this.

**Section:** threats to validity, and a methods-disclosure entry. This is
exactly the class of defect the project is about, found in the project's own
harness. Worth one honest sentence in the paper.

---

### F7 challenge — corrected proxy evidence (14 Sept)

The original challenge relied on F8's invalid binary labels. In frozen
replicate 1, the filter admitted 101/115 (87.8%) ML-declared rows. This is
agreement with the author-category proxy, not a validated semantic-recall
estimate. F11 reports an association with that proxy; F12 separately tests one
criterion intervention. Neither result isolates LOTUS's wrapper framing, so
F7 remains a hypothesis rather than a demonstrated mechanism.

---

## F10 — sem_filter agreement with author-declared category proxy 🔥 REPLACES F8

Computed 14 Sept from `topic_label_v2` (F9-corrected), which reads the authors'
own arXiv primary category and never touches the broken `topic_label`.

Coverage: 449 of 500 rows carry an arXiv stamp (89.8%). The other 51 are
`unknown` and are excluded from every rate, never guessed.

| | admitted | rejected |
|---|---|---|
| ML-declared | 101 | 14 |
| not ML-declared | 38 | 296 |

- **Precision 72.66%** [64.72, 79.39] — false admission 27.34%
- **Recall 87.83%** [80.60, 92.61]
- F1 0.795

**⚠ CORRECTED 28 Sept — which run these figures are from.** The table and rates
above are computed on `rq3_preliminary_abstracts` (155 admitted), NOT on the
frozen replicates that every other number in this ledger uses. F2's figures
come from the three frozen replicates; F10's 155 comes from the preliminary run. A reviewer comparing
the two denominators will ask. Recomputed on the frozen replicates, no API calls:

| run | admitted (labelled) | tp | fp | fn | precision | recall | F1 |
|---|---|---|---|---|---|---|---|
| preliminary (above) | 139 | 101 | 38 | 14 | 72.66% [64.72, 79.39] | 87.83% [80.60, 92.61] | 0.795 |
| **rep1** | 138 | 101 | 37 | 14 | **73.19% [65.24, 79.88]** | 87.83% [80.60, 92.61] | 0.798 |
| **rep2** | 140 | 101 | 39 | 14 | **72.14% [64.20, 78.90]** | 87.83% [80.60, 92.61] | 0.792 |
| **rep3** | 138 | 101 | 37 | 14 | **73.19% [65.24, 79.88]** | 87.83% [80.60, 92.61] | 0.798 |

*(rep3 row added 1 Oct from `results/f10_f11_frozen.json`.)* Precision spread
across the three frozen replicates: **1.05 pp**. Report per-replicate with the
spread, exactly as F2 is reported — do NOT pool the confusion matrix, because
the same row appears in every replicate and pooling would count it repeatedly.

**And a stability result worth stating rather than burying:** proxy-relative
recall is *identical to the decimal* across all four runs (preliminary and
three frozen replicates). The 101
ML-declared true positives and 14 proxy-relative false negatives never move;
only false positives shift by one or two. This is stability against the stated
proxy, not an invariant estimate of semantic recall.

F10's proxy-relative rates are not directly comparable to F8's void rates,
which used an erroneous binary label. Do not claim this shows the filter was
more semantically accurate; the author-category proxy answers a different
question from the filter's aboutness predicate.

**The caveat that must ship in these words:** the filter was asked an
*aboutness* question; the arXiv tag is a *primary-category* declaration. A
cs.SY paper applying neural networks can honestly be "about machine learning"
and still declare cs.SY. This is agreement with a stated external proxy, **not
filter error**. The category is author-supplied and independent of the retired
binary label, but it remains a proxy for the filter's aboutness criterion.

**Evidence:** `results/filter_precision_abstracts.{md,json}`,
`data/abstracts/abstracts_v1_labels_v2.csv`, `scripts/filter_precision.py`
**Section:** Results — likely the lead result alongside F2.

---

## F11 — Operational disagreements associate with the author-category proxy

Splitting admitted rows by whether the authors declared ML.

**⚠ RECOMPUTED ON THE FROZEN REPLICATES, 28 Sept.** The figures previously here
(11.9% vs 55.3%, ratio **4.7x**) came from the *preliminary* run. An external
audit recomputed from the frozen replicates and the ratio is lower. Source of
truth is now `scripts/f10_f11_frozen.py` → `results/f10_f11_frozen.json`; quote
its output, do not retype from prose.

| rep | ML-declared | rate | 95% CI | non-ML-declared | rate | 95% CI | ratio |
|---|---|---|---|---|---|---|---|
| 1 | 13/101 | 12.87% | [7.7, 20.8] | 20/37 | 54.05% | [38.4, 69.0] | **4.20x** |
| 2 | 13/101 | 12.87% | [7.7, 20.8] | 22/39 | 56.41% | [41.0, 70.7] | **4.38x** |
| 3 | 13/101 | 12.87% | [7.7, 20.8] | 20/37 | 54.05% | [38.4, 69.0] | **4.20x** |

**Quote 4.2–4.4x, never 4.7x.** The finding itself is unharmed and is in fact
more robust than the single preliminary number: the intervals are disjoint in
**every** replicate, and the ML-declared rate is identical to the decimal across
all three.

51 of 500 rows carry no declared arXiv category and are excluded throughout.

Within this selected sample, the operational disagreement rate is higher among
admitted rows whose author-declared primary category is not ML than among
ML-declared rows. This is an association with an external proxy; it does not
adjudicate the filter's aboutness judgment, establish why the model produced a
label, or estimate a population-level effect. The result is consistent with
the criteria tested by F12, but does not by itself establish the prompt-framing
hypothesis in F7.

This is the finding that makes the hand-check optional rather than load-bearing.

**Evidence:** `results/filter_precision_abstracts.md` §"Where the operational
disagreements sit" — reproducible via `python scripts/filter_precision.py`.
**Section:** Results / Discussion — proxy-relative association, with no causal
or population-level interpretation.

---

## RQ4 — design resolved, G1 closed (14 Sept)

### ⚠ WHAT THE RQ4 NULL LICENSES — write exactly this much and no more

The paired analysis (`results/rq4_paired.json`) found **3 discordant pairs in each
replicate** (rep1 b=1/c=2, rep2 b=2/c=1) on paired sets of 138 and 140 rows, with
**exact McNemar p = 1.0 in both**.

**Supportable:** *no detectable change in disagreement status under this
redundant-date-cue manipulation, in this domain, at this sample size.*

**NOT supportable — and an earlier draft of this file claimed it:** that generic
context brittleness is ruled out. Four reasons the null is narrow:
1. **Three discordant pairs** is almost no information. p = 1.0 here means "we
   could not have detected much", not "there is nothing".
2. **The injected date was already present in the text.** Moving it into a column
   is close to a no-op semantically, which makes this a weak perturbation by
   design — that was deliberate (it keeps the manipulation real rather than
   adversarial) but it caps what the null can rule out.
3. **One metadata field, one domain.** Reviews carries no production metadata, so
   RQ4 is abstracts-only.
4. It tests *this* perturbation, not the space of context changes.

Frame it as a **control whose null is consistent with robustness to this perturbation**,
and state the sample-size limitation in the same breath. A reviewer who sees
"rules out generic brittleness" next to three discordant pairs will not trust the
rest of the paper.

G1 blocked RQ4 because `injection_cols` named columns that did not exist.
Resolved:

- **abstracts:** `submission_month` (YYYY-MM) parsed from the arXiv stamp —
  449 of 500 rows. Real author-supplied metadata, not fabricated.
- **reviews:** the IMDB test split carries no production metadata at all.
  RQ4 is **abstracts-only by design**, and reviews ships as a stated limitation.
  Synthesising timestamps would abandon the real-but-irrelevant property that
  makes the finding actionable rather than adversarial.

**The date is already inside the text the operators read.** The injection
prepends `[submitted: YYYY-MM]` but does not remove the arXiv stamp from
the abstract, so both cues are present. The intervention tests whether adding
a redundant structured date cue changes agreement — it does not isolate
schema position alone, because the original prose cue was never removed.
The claim is "adding a redundant structured date cue did not detectably change
agreement" — *not* information leakage and *not* "promoted from prose to a
typed column" (the prose cue was never removed). Write it in those terms.

**Primary metric is the change in criterion-disagreement status under injection**, not the
raw flip rate. Flip rate alone is a known LLM-brittleness result and a reviewer
will say so; contradiction-rate change keeps RQ4 doing its spec §7 job of
explaining RQ3. Flip rates are secondary, reported per operator.

Baseline is the matching RQ3 replicate restricted to the same 449 rows — already
on disk, so only the injected arm costs tokens (~0.91M for 2 replicates).

**Evidence:** `scripts/run_rq4.py`, `scripts/derive_injection_cols.py`,
`data/abstracts/abstracts_v1_injection.csv`
**Status:** executed 14 Sept, 2 replicates. **RE-ANALYSED AS PAIRED 28 Sept —
use the paired numbers, not the originals.**

---

### RQ4 — PAIRED RE-ANALYSIS (28 Sept). The null is now a real null.

`scripts/rq4_paired.py`, no API calls. The original `analyse()` compared the
criterion-disagreement rate on the *baseline's* admitted set against the rate on the
*injected* run's admitted set — **two different row sets.** That mixes a
denominator change with a behaviour change, so the headline number did not
measure what its own docstring claimed. (It also pooled across replicates, which
carries the same independence defect retracted under F2.)

Injection is a within-row manipulation, so the estimand is paired and the
analysis now is too: restrict to rows admitted under BOTH conditions, and test
with **McNemar exact**, where only discordant pairs carry information.

| rep | paired n | disagreements before → after | delta | McNemar b / c | exact p |
|---|---|---|---|---|---|
| 1 | 138 | 33 → 34 (23.91% → 24.64%) | **+0.72 pp** | 1 / 2 | **1.0** |
| 2 | 140 | 35 → 34 (25.00% → 24.29%) | **−0.71 pp** | 2 / 1 | **1.0** |

**The deltas point in OPPOSITE directions across replicates.** That is the
signature of noise, not of an effect, and it is exactly what the unpaired
version concealed — the original reported **+1.07 pp**, larger than either
paired estimate and with a consistent sign, because part of it was the
denominator moving rather than the operators changing.

**Only 3 discordant pairs per replicate.** Everything else is concordant:
32–33 rows disagree under both conditions, 103–104 under neither. The entire
informative content of RQ4 is six rows across two replicates, and McNemar's
exact p is 1.0 both times.

**Admission change, reported separately as it should be:** +3/−0 in replicate 1,
+1/−0 in replicate 2. **Injection never caused the filter to drop a row — only
to add.** Directionally consistent with the claim-verification framing (more
text, more to verify against), though 4 rows total is far too few to claim it.

**Even the discordance is partly row-determined.** Row 21 disagrees under
baseline but not injection in *both* replicates; row 140 does the reverse in
both. Two of roughly three discordant rows per replicate are the same rows —
consistent with F3.

**What RQ4 now licenses, stated at its real strength:** *on rows admitted under
both conditions, adding a redundant structured date cue produced no detectable
change in criterion-disagreement status (McNemar exact p = 1.0, both replicates,
3 discordant pairs each), with per-replicate deltas of opposite sign.* That is a
control whose null is consistent with robustness to this specific perturbation
for F2 — which is the job spec §7 gives RQ4. The paired McNemar test was a
post-hoc reanalysis, not a pre-registered estimand, and the low discordant-pair
count means it has little power to detect a change.

**Evidence:** `results/rq4_paired.json`, `results/rq4_abstracts_rep{1,2}_map.csv`
**⚠ Do not cite `results/rq4_summary.json`'s rate comparison.** It is superseded.

---

---

## F12 — 🔥 SCOPED PREDICATE INTERVENTION. Criterion alignment reduces the operational disagreement count

Run 28 Sept 2026. `scripts/run_f7_ablation.py`, **two replicates**, abstracts,
baselines = rq3 rep1 and rep2. **Pre-registered before the run** with three named
outcomes (H1 collapse / H2 no change / H3 mixed); the result is H1.

⚠ **Scope of the causal claim — narrow it deliberately.** What the ablation
establishes: *changing this filter's criterion from inclusive aboutness to
exclusive primary-topic reduced the disagreement rate in this setup* — 36/154 → 4/94
(rep1) and 38/156 → 4/94 (rep2). The ablated filter admitted fewer rows, and map
labels did not change on rows both filters admitted.

What it does **not** establish: a general property of semantic operators, of LOTUS,
of gpt-4o-mini, or of other frameworks, models or tasks. One intervention, one
predicate pair, one corpus, one model. The intervention also **changes the
question asked** ("about ML" → "ML is the primary topic"), so it is criterion
alignment with a measured recall cost, not a cosmetic rewording — see the
canonical wording below.

The map, the contradiction predicate and the dataset are untouched — all three
imported from the frozen config, never restated.

**⚠ FRAMING CORRECTED 28 Sept. Do NOT write this up as "one word" or as a
wording effect.** An audit flagged it and the audit is right: "is this paper
*about* machine learning?" and "is machine learning the *primary topic*?" are
not two phrasings of one question — **they are two different criteria.**
Aboutness is inclusive, primary-topic is exclusive. Changing the filter from the
first to the second **aligns the filter's criterion with the map's**, so some
reduction in disagreement is close to tautological in direction.

**What is therefore NOT the claim:** that a cosmetic rewording removes 89% of
the incoherence. That reading would be dismantled in review, and correctly.

**What IS the claim, and it survives:** in this dataset, model, and paired
predicate intervention, aligning the filter criterion with the map's criterion
reduced the measured disagreement count from 36 to 4 (88.9%) and 38 to 4
(89.5%), across the two replicates. This is evidence about this
specific intervention, not an estimate of how much of the baseline rate is
generally caused by criterion mismatch. LOTUS's operator API does not check
cross-operator agreement by itself.

**⚠ MY "MORE CORRECT" CLAIM IS RETRACTED (28 Sept).** A second audit objection
landed and it was right: the precision CIs **overlap** ([65.24, 79.88] vs
[75.30, 90.73]), so the gain was never established by non-overlapping intervals,
and recall fell 87.83% → 61.74%, so "more correct" was unqualified and wrong.
Settled with paired tests — `scripts/f7_precision_paired.py`,
`results/f7_precision_paired.json`, both replicates:

Here TP and FP denote true- and false-positive classifications relative to the
author-category proxy; they are not independently adjudicated errors.

| test | rep 1 | rep 2 | reading |
|---|---|---|---|
| **A.** overall agreement with the proxy, McNemar exact | 88.64% → 87.31%, p = **0.497** | 88.20% → 87.31%, p = **0.689** | **no improvement.** Slightly down, not significantly |
| **B.** FP vs TP removal rate, Fisher exact | 64.9% vs 29.7% (**2.18×**), p = **0.0003** | 66.7% vs 29.7% (**2.24×**), p = **0.0001** | **removal IS differential** |
| **C.** absolute ledger | −30 proxy-positive admits to remove −24 proxy-negative admits | −30 proxy-positive admits to remove −26 proxy-negative admits | proxy-positive coverage fell more in count |

**So the tautology defence survives, but only in this narrower form:** the
criterion alignment is *targeted*, not merely restrictive. It removes
proxy-negative admits at **~2.2× the rate** it removes proxy-positive admits
(p < 0.001 in both replicates), which is F11's proxy association confirmed
from a second direction. Overall agreement with the proxy did not improve; it
was flat to slightly lower.

**⚠ THE 2.2× IS CONDITIONAL. State the conditions every time it appears.**
The Fisher test compares removal rates *within the baseline's own admitted set*,
among rows that carry an author-declared category:

| | |
|---|---|
| rows with a known declared category | 449 of 500 — **51 `unknown` rows are excluded** |
| conditioned on | admitted by the **baseline** filter |
| the actual test set | rep1 **138** rows (101 TP + 37 FP) of its 154 admitted; rep2 **140** of 156 |
| the comparison | FP removal rate vs TP removal rate *among those rows only* |

So the defensible sentence is: **"Among labelled rows admitted by the baseline,
the revised criterion removed proxy-negative admissions at about 2.2 times the
rate it removed proxy-positive admissions."** It is *not* a rate over all 500 rows, not over all
admitted rows, and not a statement about rows the baseline never admitted — the
test says nothing about those, because they are absent from it by construction.
Do not generalise the ratio beyond that conditioning.

**CANONICAL WORDING FOR THE PAPER. Use this; do not improvise around it:**

> In two hosted replicates on arXiv abstracts, changing the filter from “about
> ML” to “ML is the primary topic” reduced the operational disagreement count
> from 36 to 4 and from 38 to 4. The change raised observed precision against
> the author-declared category proxy but reduced recall; paired tests found no
> significant change in overall agreement with that proxy. This is a tradeoff
> for this predicate pair and setup, not evidence of a general accuracy gain.

**And the proxy is a proxy.** `topic_label_v2` is the authors' arXiv
*primary-category declaration*, while the baseline filter was asked an
*aboutness* question. It is external and author-supplied, which is its virtue,
but it is not a definitive judgement of what each paper is about. Never call
agreement with it "accuracy" unqualified.

**This makes the paper's design point stronger, not weaker.** The contribution is
not "use the exclusive predicate, it is better." It is: **the two criteria
genuinely differ, choosing between them is a precision/recall tradeoff, and the
operator API does not check cross-operator agreement by itself.**

| rep | condition | filter criterion | admitted | operational disagreements | rate | Wilson 95% CI |
|---|---|---|---:|---:|---:|---|
| 1 | baseline | about machine learning | 154 | 36 | 23.38% | [17.39, 30.66] |
| 1 | intervention | primary topic is machine learning | 94 | **4** | **4.26%** | [1.67, 10.44] |
| 2 | baseline | about machine learning | 156 | 38 | 24.36% | [18.29, 31.66] |
| 2 | intervention | primary topic is machine learning | 94 | **4** | **4.26%** | [1.67, 10.44] |

- Operational disagreements fell by **19.12 pp (88.9%) in rep1** and
  **20.10 pp (89.5%) in rep2**. In both replicates the Wilson intervals are
  disjoint: the intervention's upper bound is 10.44%, below the corresponding
  baseline lower bound.
- Against the author-declared proxy, precision point estimates changed from
  **73.19% to 84.52% (rep1)** and **72.14% to 84.52% (rep2)**; the intervals
  overlap in both. Recall changed from **87.83% to 61.74% in both replicates**,
  so the stricter filter misses more proxy-positive ML papers.

**Three mechanistic facts that make this clean, and each one is a check that
could have failed:**

1. **The ablated admit set is a STRICT SUBSET of the baseline's.** Zero rows were
   newly admitted in either replicate. The changed predicate only removed rows.
2. **Zero new operational disagreements were created.**
3. **All resolved disagreements disappeared through non-admission:** 32/32 in
   rep1 and 34/34 in rep2. The map labels were unchanged on all 94 rows admitted
   under both predicates. These status changes follow from the operational
   rule; they do not adjudicate whether either model judgment was a failure.

**What this establishes, stated at exactly its real strength.** F7 has two
components: (a) LOTUS's own wrappers — `sem_filter` framed to the model as claim
verification, `sem_map` as instruction following (`results/lotus_prompt_analysis.md`);
and (b) the user's predicate, inclusive vs exclusive. **This ablation changed
only (b) and observed operational disagreement counts fall from 36 to 4
(88.9%) and from 38 to 4 (89.5%) in the two replicates.** This is a result for
this exact predicate pair and setup. It does not estimate a general causal share
attributable to criterion mismatch, nor show (a) is irrelevant — testing the
wrapper asymmetry requires patching LOTUS. Do not write this as a general
mechanism effect.

(b) is also the half practitioners control, which makes it the actionable half.

**⚠ THIS REFRAMES F6 AND THE HAND-CHECK — see the note in
`results/handcheck_abstracts.md`.** In the intervention, 32/36 disagreements in
rep1 and 34/38 in rep2 disappeared when the revised predicate declined to admit
those rows. This establishes how the frozen operational measure changed; it
does not classify each resolved case as ambiguity or error.

**Four rows survived the predicate intervention:** 164, 204, 228, and 240. All
map to `theory`; the authors declare `machine_learning` for 164, 204, and 228.
They were selected for exploratory follow-up, not established as genuine
incoherence. The later single-coder results below and in F2 are the current
account; the coding is limited exploratory evidence, not ground truth.


**THE FOUR SURVIVORS, CODED 28 Sept 2026 — and the result is not what I expected.**

Verdicts are Vansika's under the frozen codebook (D-W2-14). Assistant role limited
to checking each verdict against the matrix; all four are internally consistent.

| row | Signal A | Signal B | verdict | authors declared |
|---|---|---|---|---|
| 164 | A1 `cs.NE` | B2 formal | AMBIGUOUS | machine_learning |
| 204 | A1 `cs.AI` | B2 formal | AMBIGUOUS | machine_learning |
| 228 | A1 `stat.ML` | B2 formal | AMBIGUOUS | machine_learning |
| 240 | A2 `math.ST` | B2 formal | FILTER-WRONG | other |

**Zero coded GENUINE among these four.** These were the hardest cases in the
dataset — the only operational disagreements that survive the exclusive
predicate — but a single-coder label cannot establish that none is incoherent.

**All four are B2** (formal contribution). Three carry an author-declared ML
category; row 240 is `math.ST` and author-declared `other`. The three ML-category
examples include a `stat.ML` paper proving regret bounds for kl-UCB++, a `cs.AI`
paper proving query-complexity bounds, and a `cs.NE` paper deriving a deep
network from arithmetic circuits. For those three, subject-area and contribution-
type readings can plausibly differ. Row 240 was coded FILTER-WRONG. These cases
illustrate distinct criteria; they do not establish that both operator answers
are objectively correct.

So there is a THIRD explanation, distinct from incoherence and from generic
ambiguity: the operators are being asked to resolve a real taxonomic tension —
**subject area vs contribution type** — and the abstraction never tells them
which axis the user meant. On these rows the question is underspecified — which
is a claim about the specification, not a clean bill of health for the model.

**Consequence: the original contradiction headline pivots, and that pivot was
pre-registered.** `results/handcheck_abstracts.md` states the gate in advance:
*"GENUINE > 18 of 36 → contradiction headline viable. Below → headline pivots."*
On the four hardest rows the count is 0. **The gate has since been formally
evaluated on the full coded set and it failed: 1 GENUINE, maximum possible 2,
threshold >18** (see F6). The honest claim is no longer "operators are
incoherent".

**The claim it becomes — and note what it does NOT say:** *the disagreement rate
is large and reproducible; in two replicates, the scoped predicate intervention
reduced the counts from 36 to 4 and from 38 to 4; and the single-coder
exploratory classification recorded 34 of 35 coded persistent cases as
FILTER-WRONG or AMBIGUOUS and 1 as GENUINE. The four residual cases selected for
follow-up illustrate a possible tension between subject area and contribution
type; the coding does not establish that as the general source of disagreement.*

That supports a scoped design critique and intervention result.

⚠ **Do not upgrade it to "at no point is the model incoherent."** An earlier draft
of this section said exactly that and it is not supportable:
- the ledger contains **one** genuine case, and one is not zero;
- the classification rests on a **single coder** using a codebook whose validity
  was never independently established, so it is limited exploratory evidence about
  the *character* of these disagreements, not a proof of model coherence;
- "every contradiction has a coherent explanation" is a claim about 35 rows in one
  task with one model, not about semantic operators.

The strong version of this sentence is the most attackable claim in the paper, and
it is not one the evidence needs.

**Done, 28 Sept.** The remaining rows were coded and the cross-tab is filled —
see F6 for the canonical table. The answer to "does ANY row code GENUINE" is
**one: row 305**, A1 × B1. Reported as one, not rounded to zero and not inflated.
Coverage is 35 of the 36-row persistence set; row 117 is the single gap and its
verdict cannot change any conclusion.

**Incidental confirmation of F9:** rows 164 and 228 carry `dataset label: other`
while their arXiv categories are `cs.NE` and `stat.ML`. The broken `topic_label`
column, visible in the sheet, wrong again.

**Evidence:** `results/f7_ablation_summary.json`,
`results/f7_ablation_abstracts_rep{1,2}_{filter,map}.csv`,
`results/f7_ablation_abstracts_rep{1,2}_meta.json`, `results/f7_precision_paired.json`
**Section:** Results — this is the paper's scoped predicate-intervention result,
likely the lead result alongside F2.
**Replicates:** two (rep1 and rep2 baselines). Both give 4 surviving
disagreements out of 94 admitted. **Still open:** there is no third replicate,
and the intervention was not run on reviews. Keep the result scoped to this
abstracts setup and predicate pair.

## G-LOCAL — Two-tier backend not built ⛔ CUT (D-W4-01)

Surface Pro 8 (Intel Iris Xe, 8 GB shared): measured **116 s/call**, full local
RQ1 projects to **323 hours**. Deferred to a cloud GPU (D-W2-08), conditionally
reinstated (D-W3-03), and **cut** on 1 Oct (D-W4-01). The only local artifact is
the 20-row pilot `results/rq1/local_n20_reviews_rep1_filter.csv`, which is not
used by any reported figure.

**Cost:** cannot decompose serving-stack variance from operator variance — spec
§4 called that delta "itself a contribution." It ships as a stated limitation,
not an omission; the observed RQ1 rates do not decompose or isolate that source
of variation.

---

## Surprising things (intro hooks + SOP material)

Per `02-TIMELINE` standing practice. These are the sentences that make people
look up.

1. **A model given three labels produced a fourth label.** All 15 observed
   violation events across three frozen reviews replicates were `mixed`.
2. **Row 397 produced both `positive` and `mixed` across frozen replicates.**
   The frozen rule counts the former as an in-schema criterion disagreement
   and the latter as a schema violation; this does not establish why it changed.
3. **RQ1 found low individual flip rates, while RQ3 measured disagreement on
   about one admitted row in four under its predicate pair.** These are distinct
   measurements; the comparison does not establish a general composition law.
4. **LOTUS documents operator composability, but we found no stated contract or
   built-in check for cross-operator criterion agreement in its paper/docs.**
   Keep any broader novelty claim provisional pending the related-work review.
5. **The filter and map are given different criteria, and the pipeline provides
   no built-in check for that mismatch.** Their wrapper framing also differs
   (claim verification vs instruction following), but this study does not
   isolate its contribution.
6. **The same 36 rows met the operational disagreement rule in all three hosted
   replicates.** This is repeatability on the frozen corpus and setup; it does
   not establish inherent incoherence or explain the source of run-to-run
   variation.
7. **The benchmark's original binary category grouping was wrong, and the
   audit found it.** Two of four IDs grouped as "ML" were commutative algebra
   and control theory. The frozen sample was not reselected, so proxy analyses
   describe this sample rather than the arXiv population.
8. **Operational disagreements were ~4.2–4.4x more frequent among admitted
   rows without an author-declared ML category.** Recomputed per frozen replicate
   (`results/f10_f11_frozen.json`); the
   intervals are disjoint in every replicate. The 4.7x figure in earlier drafts
   came from the preliminary run — do not quote it. This is an association with
   the author-declared proxy categories, not evidence that category status
   causes disagreement.
9. **The scoped predicate intervention reduced the operational disagreement
   count from 36 to 4 and from 38 to 4 across two replicates, with
   selective-removal evidence.** Changing the filter from inclusive "about
   machine learning" to exclusive "machine learning is the primary topic"
   reduced the count in both runs; every resolution came from non-admission
   rather than changed map outputs. Among labelled rows
   the baseline admitted, proxy-negative admits were removed at **~2.2× the rate**
   of proxy-positive admits (p < 0.001, both replicates). This is scoped evidence
   for this intervention, not a general causal share for criterion mismatch.

   **Overall agreement with the proxy did not improve.** It was flat to slightly
   lower (McNemar p ≈ 0.5, 0.7) and
   recall fell 87.83% → 61.74%. The finding is *selective removal with a recall
   cost*, never improved accuracy — see the canonical wording in F12 and use it
   verbatim.

---

## F13 — ⚠ The author-category proxy is visible to the model (stamp exposure)

Checked 1 Oct 2026, no API calls.

**Exposure is total by construction.** `topic_label_v2` is parsed from the arXiv
stamp (e.g. `arXiv:1512.00608v1 [math.AC] 2 Dec 2015`) *inside the same
4,000-char window the operators receive*. So all **449/449** labelled rows show
the model the label F10–F12 compare against. The stamp sits at the top of the
text: median position char **80** (p90 196; 442/449 before char 500). Of the
first 300 abstracts prompts in `logs/llm_calls/llm_calls_rq3.jsonl`, **269**
contain a stamp. That is ~90%, matching the 449/500 coverage. It reached the
model.

**Decisions track the category closely** (RQ1 rep1, categories with n ≥ 8):

| category | n | filter admit | map = ML |
|---|---:|---:|---:|
| cs.LG | 20 | 1.00 | 0.80 |
| stat.ML | 8 | 1.00 | 0.88 |
| cs.NE / cs.CV / cs.AI | 17 / 42 / 24 | 0.88 / 0.86 / 0.75 | 0.82 / 0.83 / 0.54 |
| math.ST / math.OC | 26 / 15 | 0.23 / 0.20 | 0.08 / 0.00 |
| cs.SY / cs.CE | 43 / 14 | 0.14 / 0.14 | 0.05 / 0.14 |
| cs.DS / cs.IT / cs.PL | 27 / 37 / 21 | 0.07 / 0.05 / 0.05 | 0.00 / 0.03 / 0.10 |
| math.AC / math.GR | 42 / 15 | 0.00 / 0.00 | 0.00 / 0.07 |
| no stamp (`unknown`) | 51 | 0.31 | 0.25 |

**What this does and does not show.** The pattern is what tag-reading would
produce, but it is also what reading the content would produce: cs.LG papers
*are* about ML. Observational data cannot separate the two. Two points push
against pure tag-reading: cs.AI is admitted at 75%, not 100%, and cs.SY and
math.ST are admitted at about 20%, not 0%. The model is not simply copying the
tag. But the size of any tag effect is **unidentified**.

**Consequences.**
- **F10/F11/F12 are no longer "external, independent" evidence.** The proxy is
  author-supplied, but it is not independent of the model's input. Remove
  "independent" and "external" from those claims, or run the control below.
- **The hand-check is affected too.** Its Signal A was read from the same stamp.
- **F2 (filter vs map disagreement) is not invalidated.** Both operators saw
  the same stamp. Whether the stamp raises or lowers disagreement is unknown.
- **RQ4 was already framed correctly.** The injected date duplicates
  information that was in the stamp.

### ✅ RESULT (run 1 Oct 2026): P2 — no detectable reliance on the stamp

The decision rules were committed in `187ef70` before the run. Output:
`results/stamp_ablation_summary.json`, `results/stamp_ablation_abstracts_rep1_{filter,map,meta}`.
Of the 648 parseable request records in `logs/llm_calls/llm_calls_stamp_ablation.jsonl`,
**0** contain a stamp. Four malformed lines prevent complete request
reconstruction; the deterministic input audit separately confirms blanking on
all 449 categorized rows. Do not claim the archive verifies every request.

| measure (vs `rq3_abstracts_rep1`) | with stamp | without stamp |
|---|---:|---:|
| filter admitted | 154 | 155 |
| labelled-row filter flips (threshold >4; noise ceiling 2) | — | **3** (rows 75, 76, 177) |
| proxy agreement, paired McNemar | 88.64% | 88.86%, p = 1.0 (1 vs 2 discordant) |
| F10 precision | 73.19% [65.24, 79.88] | 73.38% [65.48, 80.03] |
| F10 recall | 87.83% [80.60, 92.61] | 88.70% [81.62, 93.27] |
| F11 ML-declared disagreement | 13/101 = 12.87% | 12/102 = 11.76% |
| F11 non-ML-declared disagreement | 20/37 = 54.05% | 21/37 = 56.76% |
| F2 operational disagreement | 36/154 = 23.38% | 36/155 = 23.23% |
| F2 paired, rows admitted in both (n=153) | — | 2 vs 1 discordant, McNemar p = 1.0 |
| map labels changed on rows admitted in both | — | 3 (rows 140, 330, 347) |

The 3 flips are below the pre-specified threshold of more than four, but
exceed the largest earlier pairwise difference of two. Row 75 is one of the two rows only
replicate 2 admitted (F2/F3), and the flips go in both directions (two
admitted, one dropped). Of the 3 map-label changes, row 140 is already
unstable: it flips across RQ1 replicates and under RQ4. Rows 330 and 347 had
not changed before. That is 3 changes in 153 rows, and the net effect on
disagreement status is 2 vs 1 discordant (p = 1.0).

**What this licenses.** Removing the author-declared category from the input
produced no detectable change in the filter, the map, the proxy agreement
(F10), the category split of disagreements (F11), or the headline rate (F2).
F10–F12's proxy is author-supplied, and this single control did not detect
reliance under its decision rules. It does not establish independence of
the model's decisions from the category cue. Write it this way: *"the category stamp was
visible to the model; removing it did not detectably change any reported
measure (one replicate, McNemar p = 1.0)."*

**What it does not license.** It does not show "the model ignores the stamp".
This is one replicate, and a small effect below the noise ceiling cannot be
ruled out. Disclose the exposure and the control together; neither replaces
the other.

**Original plan (kept for the record).** The control as it was described
before the run:
`scripts/run_stamp_ablation.py` blanks each stamp with equal-length spaces, so
the window keeps identical surrounding text. It then re-runs the frozen
abstracts pipeline once (~0.66M tokens, well under $1) and compares against
`rq3_abstracts_rep1`. Leak audit, dry run: stamps blanked on 449/449 labelled
rows, 0 unknown rows changed, 0 rows with the category code still in the
window.

The decision rules (P1 reliance / P2 none detected / P3 mixed) are in the
script's docstring and were committed to git before the run. They use a noise
ceiling measured beforehand: across the 8 existing abstracts runs, at most 2
labelled-row filter decisions differ between any two runs. Results go to
`results/stamp_ablation_summary.json`. Unchanged results turn this into a
robustness result; changed results mean F10–F12 must be rescoped.

---

## F14 — The LOTUS wrapper has a small, one-directional effect; the criteria dominate

Run 1 Oct 2026: `scripts/run_wrapper_test.py`, with decision rules committed in
`c8163fe` before the run. Output: `results/wrapper_test_summary.json`,
`results/wrapper_about_instruction_abstracts_rep1_{map,meta}`. It is a 2×2 of
criterion × LOTUS wrapper on all 500 abstracts rows. One new run asked the
filter's question verbatim under the `sem_map` (instruction-following) wrapper,
with a True/False answer. The other three cells come from existing runs. No
off-schema answers.

| | claim-verification wrapper (`sem_filter`) | instruction-following wrapper (`sem_map`) |
|---|---:|---:|
| "about ML?" | 154 yes (rq3 rep1) | 147 yes (**new run**) |
| "ML primary topic?" | 94 yes (F12 rep1) | 123 labelled ML (RQ1 map rep1) |

| comparison | differing rows (threshold > 6) | direction | exact McNemar |
|---|---:|---|---:|
| **W-about** — wrapper, criterion fixed (pre-registered, clean) | **7** | 7 claim-only vs 0 | p = 0.016 |
| W-primary — wrapper, criterion fixed (post hoc; format confounded) | 37 | 4 vs 33 instruction-only | p ≈ 1e-6 |
| C-claim — criterion, filter wrapper (= F12) | 60 | 60 about-only vs 0 | p < 1e-6 |
| C-instruction — criterion, map wrapper | 36 | 30 about-only vs 6 | p = 7e-5 |

**Verdict: Q1, reached through the clean W-about comparison alone.** It only
just crosses the bar: 7 against a threshold of > 6. Two things support it
beyond the threshold. All 7 rows (21, 76, 91, 117, 178, 326, 389) are admitted
in **8/8** baseline filter runs, so they are stable rows, not noisy ones. And
the effect is one-directional, 7 vs 0. Holding the question "is this paper
about ML?" fixed, the claim-verification wrapper says yes where the
instruction-following wrapper says no. That is the direction F7(a)
hypothesised: the filter framing is more inclusive. Six of the 7 rows are
baseline disagreements; the exception is 326, which maps to ML.

**But the wrapper effect is small next to the criterion effect.** Changing the
criterion moves 60 rows under the filter wrapper and 36 under the map wrapper.
Changing the wrapper with the criterion and format held fixed moves 7. When
both operators use the same (instruction) wrapper, the disagreement rate is
**30/147 = 20.41% [14.69, 27.64]**. The cross-wrapper reference using the same
map run is **35/154 = 22.73% [16.82, 29.96]**. Aligning the wrappers removes
about 5 of 35 disagreements; most of the disagreement survives and is
attributable to the criteria.

**How to write it.** *"Holding the question fixed, LOTUS's claim-verification
wrapper admitted 7 rows that the instruction-following wrapper rejected, and
none the other way (one run, McNemar p = 0.016). Most of the measured
disagreement remains when both operators share a wrapper (20.4% vs 22.7%).
In this setup, the criterion mismatch is the main source of disagreement and
the wrapper a smaller, one-directional one."* This upgrades F7(a) from an
untested hypothesis to a small measured effect in one run. It does not
establish a general wrapper effect.

**W-primary is not evidence for the verdict.** It was seen before the rules
were committed, and it varies the answer format (four-way label vs yes/no)
along with the wrapper (see the script's disclosure). Its direction is the
opposite of W-about's, so do not report it as wrapper evidence; it is a
format-plus-wrapper contrast.

**Limits.** This is one new run, and the new cell has no replicate of its own,
so its noise ceiling is assumed from the other cells (max 3). One model, one
corpus, one question pair.

---

## F16 — Cross-listing: about half of F10's "false admissions" are cross-listed ML papers

Computed 1 Oct 2026 with `scripts/crosslist_reanalysis.py` → `results/crosslist_reanalysis.json`.
No LLM calls. Every categorised paper's full category list came from the arXiv
API once and is cached in `data/abstracts/abstracts_v1_crosslists.csv`; all 449
were found. The definition was fixed in the script's docstring before any result
was computed, but it was committed together with the results, not separately
beforehand:
**ml_any = any listed category in {cs.LG, cs.AI, cs.CV, cs.CL, cs.NE, stat.ML}.**

**Sanity check.** The API's primary category differs from the stamp on 43
papers, and every one of them is cs.SY → eess.SY. arXiv now treats cs.SY as an
alias of eess.SY, so these are the same papers, not fetch errors. Caveat: the
API returns the latest version's categories, and some cross-lists may have been
added after the frozen version.

**37 of the 334 "non-ML-declared" papers are cross-listed to an ML category**
(115 → 152 ML under ml_any; no paper loses its ML status). Example: row 1 is
cs.RO primary, cross-listed cs.AI and cs.LG.

| (rep1; reps 2–3 in the JSON) | primary-only proxy | cross-listed proxy |
|---|---:|---:|
| F10 filter precision | 73.19% [65.24, 79.88] | **86.96% [80.32, 91.59]** |
| F10 filter recall | 87.83% [80.60, 92.61] | 78.95% [71.79, 84.67] |
| F11 disagreement, ML | 13/101 = 12.87% | 21/120 = 17.50% [11.74, 25.28] |
| F11 disagreement, non-ML | 20/37 = 54.05% | 12/18 = 66.67% [43.75, 83.72] |
| F11 ratio | 4.20× | 3.81× (rep2 4.00×, rep3 3.81×), disjoint in all 3 |
| F12 revised filter precision / recall | 84.52% / 61.74% | 96.43% / 53.29% |

**What this changes.**
- **F10.** Under the primary-only proxy, about half the filter's apparent false
  admissions (37 → 18 in rep1) are papers the authors cross-listed to ML. The
  primary-only proxy understates agreement with an aboutness reading. Report
  F10 under **both** definitions. The filter's apparent precision depends
  strongly on how "ML" is defined, which illustrates the paper's own point
  that "about ML" and "primarily ML" are different criteria.
- **F11 survives.** The concentration is 3.8–4.0× instead of 4.2–4.4×, with
  disjoint intervals in every replicate. The non-ML group shrinks to 18–20
  admitted rows, so its intervals are wide. Quote both definitions.
- **F12.** The tradeoff becomes starker under the inclusive proxy: precision
  96.4%, recall 53.3%.
- **Hand-check.** Signal A used the primary category only, so some rows coded
  A2 / FILTER-WRONG are cross-listed ML papers. This weakens the FILTER-WRONG
  tally further. Recount against cross-lists before quoting the 19 / 15 / 1
  split.

**Hand-check recount (done 1 Oct, mechanical).** Seven of the 19 FILTER-WRONG
rows are cross-listed to ML: 18, 91, 127, 200, 227, 357, 414. So is row 76,
which was coded AMBIGUOUS. Re-applying the frozen codebook matrix with **only
Signal A changed** (A1 if any listed category is ML) and every Signal B choice
left as the coder made it gives:

| | coded (primary-only Signal A) | recount (cross-listed Signal A) |
|---|---:|---:|
| FILTER-WRONG | 19 | 12 |
| AMBIGUOUS | 15 | 21 |
| GENUINE | 1 (row 305) | 2 (rows 305, 76) |

This is not a new adjudication. Signal A is objective metadata and no coder
judgment changed (D-W2-14 intact). The gate still fails (2 vs > 18). In the
paper, report the coded tally and give this recount as a sensitivity check.
