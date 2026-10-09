"""
OperatorBench -- F14: does LOTUS's operator wrapper change the answer to a fixed question?

WHY THIS EXISTS
---------------
F7 has two components: (a) LOTUS frames sem_filter as CLAIM VERIFICATION and
sem_map as INSTRUCTION FOLLOWING, and (b) the user's two criteria differ
("about ML" vs "primary topic"). F12 changed only (b). Component (a) has never
been tested. Without that test a reviewer can say the study is prompt
sensitivity in gpt-4o-mini, with LOTUS incidental.

THE 2x2 DESIGN (abstracts, all 500 frozen rows)
-----------------------------------------------
                         claim-verification wrapper   instruction-following wrapper
                         (sem_filter)                 (sem_map)
  "about ML?"            baseline filter              NEW RUN (this script)
                         rq3 rep1 (+7 more runs)
  "ML primary topic?"    F12 ablated filter           RQ1 map, label == ML
                         f7_ablation rep1 (+rep2)     rq1 rep1 (+4 more runs)

Reading along a row holds the CRITERION fixed and varies the WRAPPER. Reading
down a column holds the wrapper fixed and varies the criterion. Only the
top-right cell needs API calls: sem_map over all 500 rows with the filter's
question asked verbatim, answer constrained to True/False (~0.5M tokens).

The new cell's instruction keeps the filter's question word for word and adds
only the answer-format sentence the frozen map instruction already uses
("Respond with exactly one word."):
  "Respond with exactly one word, True or False. Is this paper about machine
   learning? Abstract: {abstract_text}"

PRE-REGISTERED DECISION RULES (committed to git before the run -- do not edit)
------------------------------------------------------------------------------
Noise ceilings, measured 1 Oct 2026 before the run, as the most rows that
differ between any two runs of the SAME cell (all 500 rows):
  about/claim 2 (8 runs), primary/instruction 3 (5 runs), primary/claim 0 (2 runs).
One threshold for every comparison: more than 6 differing rows (2x the largest
ceiling, 3).

Primary comparisons (wrapper effect at a fixed criterion):
  W-about:   about/claim (rq3 rep1)        vs about/instruction (new run)
  W-primary: primary/claim (f7 rep1)       vs primary/instruction (rq1 rep1)
Each is a paired comparison on 500 rows with an exact McNemar test.

  Q1 (wrapper matters): at least one W comparison has more than 6 differing
     rows AND McNemar p < 0.05. Report the direction (which wrapper says yes
     more often). F7(a) gains direct support in this setup.
  Q2 (no detectable wrapper effect): both W comparisons have at most 6
     differing rows. The measured disagreement is then attributable to the
     criteria, not the wrappers, in this setup -- strengthening F12.
  Q3 (mixed): anything else. Report the numbers without a mechanism claim.

Secondary, reported whatever the outcome:
  C-claim:       about/claim vs primary/claim (the F12 contrast, restated)
  C-instruction: about/instruction vs primary/instruction
  Same-wrapper disagreement: among rows the about/instruction cell says True,
  how many does the RQ1 map (same wrapper) label non-ML? Compare with the
  baseline 36/154, where the two operators used different wrappers.

Scope: one new run, one model, abstracts only. Q2 means "no detectable wrapper
effect at this sample size", not "the wrappers are irrelevant".

DISCLOSURE -- W-primary was SEEN before this file was committed (1 Oct 2026)
---------------------------------------------------------------------------
The rules above were written first and have not been edited since. But a
synthetic test of analyse(), run before the commit, returned Q1 on a fake new
cell identical to about/claim, which revealed that W-primary alone already
crosses the threshold. Its numbers were then inspected: primary/claim says yes
on 94 rows, primary/instruction on 123; 4 vs 33 discordant (McNemar
p ~ 1e-6), and the same 4-5 vs 33-35 split holds for every pairing of the
F12 reps 1-2 with RQ1 map reps 1-5.

Two consequences:
  1. Only W-about (the new run) is cleanly pre-registered. W-primary, and
     therefore a Q1 verdict reached through W-primary alone, is a post-hoc
     observation. Report it as one.
  2. W-primary is confounded. The map chooses one of four labels while the
     filter answers yes or no, so it varies the answer format as well as the
     wrapper. W-about holds the yes/no format fixed and varies only the
     wrapper, which is why it is the test that counts.
The analyse() verdict is applied as written. When reporting, state which
comparison drove it.

Usage:
    python scripts/run_wrapper_test.py --dry-run
    python scripts/run_wrapper_test.py --batch-delay 12 --patient --yes
    python scripts/run_wrapper_test.py --aggregate-only
"""

import sys
import json
import math
import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from configs.pipelines import PIPELINES, BACKENDS
from harness.metrics import wilson_ci

DOMAIN = "abstracts"
RUN_ID = f"wrapper_about_instruction_{DOMAIN}_rep1"
OUT_COL = "about_ml"
NEW_INSTRUCTION = ("Respond with exactly one word, True or False. "
                   "Is this paper about machine learning? Abstract: {abstract_text}")
VALID = {"True", "False"}

OUT_MAP = ROOT / "results" / f"{RUN_ID}_map.csv"
META = ROOT / "results" / f"{RUN_ID}_meta.json"
SUMMARY = ROOT / "results" / "wrapper_test_summary.json"

ABOUT_CLAIM = ROOT / "results" / f"rq3_{DOMAIN}_rep1_filter.csv"          # admitted rows only
PRIMARY_CLAIM = ROOT / "results" / f"f7_ablation_{DOMAIN}_rep1_filter.csv"  # admitted rows only
PRIMARY_INSTR = ROOT / "results" / "rq1" / f"hosted_{DOMAIN}_rep1_map.csv"  # all 500 rows

NOISE_CEILINGS = {"about_claim": 2, "primary_instruction": 3, "primary_claim": 0}
THRESHOLD = 2 * max(NOISE_CEILINGS.values())   # = 6


def mcnemar_exact(b: int, c: int) -> float:
    """Exact two-sided McNemar p (same method as scripts/f7_precision_paired.py)."""
    n = b + c
    if n == 0:
        return 1.0
    return min(1.0, 2 * sum(math.comb(n, i) for i in range(min(b, c) + 1)) / 2 ** n)


def load_rows() -> pd.DataFrame:
    df = pd.read_csv(ROOT / PIPELINES[DOMAIN]["data_file"], escapechar="\\", index_col="row_id")
    assert len(df) == 500 and list(df.index) == list(range(500)), \
        "frozen abstracts set changed -- STOP, do not proceed (D-09)"
    return df


def admitted(path: Path) -> set:
    return set(pd.read_csv(path, escapechar="\\", usecols=["row_id"]).row_id)


def cells() -> dict:
    """Each cell as the set of row_ids it answers YES for (out of 500)."""
    out = {
        "about_claim": admitted(ABOUT_CLAIM),
        "primary_claim": admitted(PRIMARY_CLAIM),
    }
    pm = pd.read_csv(PRIMARY_INSTR, index_col="row_id")[PIPELINES[DOMAIN]["map"]["output_col"]]
    out["primary_instruction"] = set(pm[pm == "ML"].index)
    if OUT_MAP.exists():
        nm = pd.read_csv(OUT_MAP, index_col="row_id")[OUT_COL].astype(str)
        out["about_instruction"] = set(nm[nm == "True"].index)
        out["_about_instruction_off_schema"] = sorted(nm[~nm.isin(VALID)].index.tolist())
    return out


def compare(a: set, b: set, name_a: str, name_b: str) -> dict:
    only_a, only_b = len(a - b), len(b - a)
    return {
        "yes_counts": {name_a: len(a), name_b: len(b)},
        "differing_rows": only_a + only_b,
        f"yes_only_{name_a}": only_a,
        f"yes_only_{name_b}": only_b,
        "mcnemar_p": round(mcnemar_exact(only_a, only_b), 6),
        "exceeds_threshold": (only_a + only_b) > THRESHOLD,
    }


def analyse() -> dict:
    c = cells()
    if "about_instruction" not in c:
        return {"error": f"{RUN_ID} not on disk -- run it first"}

    w_about = compare(c["about_claim"], c["about_instruction"], "claim", "instruction")
    w_primary = compare(c["primary_claim"], c["primary_instruction"], "claim", "instruction")

    def material(r):
        return r["exceeds_threshold"] and r["mcnemar_p"] < 0.05

    if material(w_about) or material(w_primary):
        verdict = "Q1 -- the wrapper changes answers to a fixed question in this setup"
    elif not w_about["exceeds_threshold"] and not w_primary["exceeds_threshold"]:
        verdict = "Q2 -- no detectable wrapper effect at this sample size; disagreement attributable to the criteria"
    else:
        verdict = "Q3 -- mixed: report numbers without a mechanism claim"

    # Same-wrapper disagreement: about/instruction says yes, map (same wrapper) says not ML.
    adm = c["about_instruction"]
    k = len(adm - c["primary_instruction"])
    base_adm = c["about_claim"]
    k0 = len(base_adm - c["primary_instruction"])
    return {
        "verdict": verdict,
        "threshold_differing_rows": THRESHOLD,
        "noise_ceilings": NOISE_CEILINGS,
        "about_instruction_off_schema_rows": c["_about_instruction_off_schema"],
        "W_about_wrapper_effect": w_about,
        "W_primary_wrapper_effect": w_primary,
        "C_claim_criterion_effect": compare(c["about_claim"], c["primary_claim"], "about", "primary"),
        "C_instruction_criterion_effect": compare(c["about_instruction"], c["primary_instruction"],
                                                  "about", "primary"),
        "disagreement_same_wrapper": {
            "definition": "about/instruction says True, RQ1 map (instruction wrapper) label != ML",
            "k": k, "n": len(adm), "rate": k / len(adm) if adm else None,
            "ci": list(wilson_ci(k, len(adm))) if adm else None,
        },
        "disagreement_cross_wrapper_reference": {
            "definition": "about/claim admits (rq3 rep1), RQ1 rep1 map label != ML -- same map run as above",
            "k": k0, "n": len(base_adm), "rate": k0 / len(base_adm) if base_adm else None,
            "ci": list(wilson_ci(k0, len(base_adm))) if base_adm else None,
        },
    }


def report(res: dict) -> None:
    print("\n" + "=" * 70)
    print("F14 -- wrapper test (abstracts, 2x2 criterion x wrapper)")
    print("=" * 70)
    if "error" in res:
        print("  " + res["error"])
        return
    for key in ("W_about_wrapper_effect", "W_primary_wrapper_effect",
                "C_claim_criterion_effect", "C_instruction_criterion_effect"):
        r = res[key]
        print(f"  {key:32s} yes {r['yes_counts']}  differing {r['differing_rows']:3d}  "
              f"McNemar p={r['mcnemar_p']}")
    s, x = res["disagreement_same_wrapper"], res["disagreement_cross_wrapper_reference"]
    print(f"  disagreement, same wrapper   {s['k']}/{s['n']} = {s['rate']:.2%}")
    print(f"  disagreement, cross wrapper  {x['k']}/{x['n']} = {x['rate']:.2%}  (reference)")
    if res["about_instruction_off_schema_rows"]:
        print(f"  off-schema answers in new run: {res['about_instruction_off_schema_rows']}")
    print(f"\n  => {res['verdict']}")


def main() -> None:
    ap = argparse.ArgumentParser(description="F14 wrapper test")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--aggregate-only", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--yes", action="store_true")
    ap.add_argument("--batch-delay", type=float, default=None)
    ap.add_argument("--patient", action="store_true")
    args = ap.parse_args()

    df = load_rows()
    print(f"New cell instruction:\n  {NEW_INSTRUCTION}\nRows: {len(df)}")

    if args.dry_run:
        for p in (ABOUT_CLAIM, PRIMARY_CLAIM, PRIMARY_INSTR):
            print(f"  {'OK     ' if p.exists() else 'MISSING'} {p.relative_to(ROOT)}")
        print(f"Threshold: >{THRESHOLD} differing rows. Estimated tokens: ~0.5M.")
        print("--dry-run: no API calls made.")
        return

    if not args.aggregate_only:
        if OUT_MAP.exists() and not args.force:
            print(f"{OUT_MAP.name} already on disk -- analysing it (use --force to redo)")
        else:
            if not args.yes and input("\nProceed (~0.5M tokens)? [y/N] ").strip().lower() != "y":
                print("Aborted.")
                return
            from harness.runner import PipelineRunner
            from harness.intercept import LLMInterceptor
            from harness.provenance import run_provenance

            interceptor = LLMInterceptor(log_dir=ROOT / "logs" / "llm_calls", tag="wrapper_test")
            interceptor.install()
            runner = PipelineRunner(
                model=BACKENDS["hosted"]["model"], backend="hosted",
                log_dir=ROOT / "logs", cache_path=ROOT / "cache.db", cache_enabled=False,
                batch_delay_seconds=args.batch_delay, patient=args.patient,
            )
            mapped = runner.run_map_only(
                df=df, domain=DOMAIN, map_instruction=NEW_INSTRUCTION,
                map_output_col=OUT_COL, valid_labels=VALID, run_id=RUN_ID,
            )
            out = mapped[[OUT_COL, f"_raw_{OUT_COL}"]].copy()
            out.index.name = "row_id"
            out.to_csv(OUT_MAP)
            META.write_text(json.dumps({
                "run_id": RUN_ID,
                "purpose": "F14 wrapper test: filter's question under the sem_map wrapper",
                "instruction": NEW_INSTRUCTION,
                "compared_with": {"about_claim": "rq3_abstracts_rep1",
                                  "primary_claim": "f7_ablation_abstracts_rep1",
                                  "primary_instruction": "rq1 hosted_abstracts_rep1_map"},
                "provenance": run_provenance(backend="hosted", seed=None, cache_enabled=False),
            }, indent=2, default=str))

    res = analyse()
    report(res)
    if "error" not in res:
        SUMMARY.write_text(json.dumps(res, indent=2, default=str))
        print(f"\nWrote {SUMMARY.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
