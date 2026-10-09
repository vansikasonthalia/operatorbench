"""
OperatorBench -- F7: scoped filter-criterion intervention

WHAT THIS TESTS, AND WHAT IT DOES NOT
-------------------------------------
This compares one exact pair of filter criteria on the frozen abstracts corpus:
inclusive aboutness ("is this paper ABOUT machine learning?") versus exclusive
primary-topic ("is machine learning the PRIMARY topic?"). The manuscript calls
the measured outcome criterion disagreement; the code and frozen result fields
retain the historical name `contradiction` for the operational predicate.

Across two replicates, the observed counts changed from 36 to 4 and from 38 to
4. This is evidence about this predicate intervention, model and corpus. It is
not an estimate of a general causal share, a general property of semantic
operators, or a validated measure of correctness.

  Baseline filter:  "Is this paper about machine learning? {abstract_text}"
  Ablated filter:   "Is machine learning the primary topic of this paper? {abstract_text}"

**The map instruction, operational predicate, and dataset are unchanged.** The
map instruction is read out of the frozen
`configs/pipelines.py`, never copied into this file, so it cannot drift. The
frozen contradiction predicate is likewise imported, not restated.

LIMITATION -- STATE THIS IN THE PAPER, DO NOT LET IT BE OVERSTATED
-----------------------------------------------------------------
F7 has two components. This run changes only the filter criterion.

  (a) LOTUS's own wrappers: sem_filter is framed to the model as CLAIM
      VERIFICATION ("determine whether the claim is true"), sem_map as
      INSTRUCTION FOLLOWING ("answer the user's instruction"). See
      `results/lotus_prompt_analysis.md`. This run does NOT change that --
      doing so would mean patching LOTUS.
  (b) The user's predicate criterion: inclusive "about" vs exclusive
      "primary topic". This run changes exactly that, and nothing else.

The observed reduction is scoped to this criterion pair and setup. It does not
show that this criterion mismatch generally explains the baseline rate or that
(a) is irrelevant. The wrapper asymmetry remains untested.

(b) is a user-controllable design choice; this experiment does not establish
that one criterion is more correct in general.

PRE-REGISTERED PREDICTIONS (written before the run -- do not edit afterwards)
----------------------------------------------------------------------------
The hypotheses below are preserved as the historical preregistration, including
their original causal wording. They are not the current interpretation; see the
scope statement above and `docs/RESULTS.md`.

  H1 (F7 wording is the cause): the ablated filter admits materially FEWER rows,
     and the contradiction rate among those it does admit falls well below the
     23.87% baseline -- call it under ~10%.
  H2 (F7 wording is NOT the cause): the contradiction rate among admitted rows
     stays near baseline even if the admitted count drops. The gap then is not
     about the user's wording.
  H3 (mixed): admits drop and precision against `topic_label_v2` rises, but the
     residual contradiction rate stays high. That would point at the LOTUS
     wrapper asymmetry (a) rather than the wording (b).

Whichever lands, it gets reported. A null here is a result, exactly as RQ4's
null was.

SECONDARY MEASUREMENT
---------------------
Filter precision and recall against `topic_label_v2` (the authors' own declared
arXiv category, F9-corrected), for the baseline and the ablated filter. If the
reworded filter is more precise, that is direct support for F11's
over-admission story.

Usage:
    python scripts/run_f7_ablation.py --dry-run
    python scripts/run_f7_ablation.py --baseline-rep 1 --batch-delay 12 --patient --yes
    python scripts/run_f7_ablation.py --aggregate-only
"""

import sys
import json
import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from configs.pipelines import PIPELINES, BACKENDS
from harness.metrics import wilson_ci
from harness.artifacts import load_completed_pipeline

DOMAIN = "abstracts"
LABELS_CSV = ROOT / "data" / "abstracts" / "abstracts_v1_labels_v2.csv"

# The ONLY thing this ablation changes. Minimal edit: "about" -> "the primary
# topic of". Logged into the run meta for auditability.
ABLATED_FILTER = (
    "Is machine learning the primary topic of this paper? {abstract_text}"
)


def load_rows():
    """All 500 frozen rows, with the F9-corrected labels attached if present."""
    df = pd.read_csv(ROOT / PIPELINES[DOMAIN]["data_file"],
                     escapechar="\\", index_col="row_id")
    assert len(df) == 500 and list(df.index) == list(range(500)), \
        "frozen abstracts set changed -- STOP, do not proceed (D-09)"

    if LABELS_CSV.exists():
        lab = pd.read_csv(LABELS_CSV, index_col="row_id")
        df = df.join(lab[["topic_label_v2"]])
    return df


def baseline_paths(rep):
    return (ROOT / "results" / f"rq3_{DOMAIN}_rep{rep}_filter.csv",
            ROOT / "results" / f"rq3_{DOMAIN}_rep{rep}_map.csv")


def run_replicate(runner, df, rep, force=False):
    run_id = f"f7_ablation_{DOMAIN}_rep{rep}"
    map_path = ROOT / "results" / f"{run_id}_map.csv"
    if map_path.exists() and not force:
        print(f"  [rep {rep}] already on disk -- loading (use --force to redo)")
        return load_completed_pipeline(ROOT / "results", run_id, df.index,
                                       PIPELINES[DOMAIN]["map"]["output_col"])

    cfg = PIPELINES[DOMAIN]
    runner.run_filter_map(
        df=df,
        domain=DOMAIN,
        filter_predicate=ABLATED_FILTER,          # <-- the only change
        map_instruction=cfg["map"]["instruction"],  # frozen, imported
        map_output_col=cfg["map"]["output_col"],
        valid_labels=cfg["map"]["valid_labels"],
        run_id=run_id,
    )
    # Merge experiment-specific fields into runner metadata instead of
    # replacing it, so provenance (package versions, hashes) is preserved.
    meta_path = ROOT / "results" / f"{run_id}_meta.json"
    existing_meta = {}
    if meta_path.exists():
        try:
            existing_meta = json.loads(meta_path.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    existing_meta.update({
        "run_id": run_id,
        "purpose": "F7 scoped predicate-criterion intervention -- aboutness to primary topic",
        "baseline_filter_langex": cfg["filter"]["predicate"],
        "ablated_filter_langex": ABLATED_FILTER,
        "map_langex": cfg["map"]["instruction"],
        "map_changed": False,
        "contradiction_predicate_changed": False,
        "baseline_run": f"rq3_{DOMAIN}_rep{rep}",
        "n_rows": len(df),
        "limitation": ("changes the filter criterion only; does NOT alter LOTUS's "
                       "claim-verification vs instruction-following wrappers, and "
                       "does not estimate a general causal share"),
    })
    meta_path.write_text(json.dumps(existing_meta, indent=2, default=str))
    return pd.read_csv(map_path, index_col="row_id")


def prf(admitted, declared_ml, all_rows):
    """Precision/recall of a filter against author-declared ML, excluding unknowns."""
    known = set(all_rows)
    adm = admitted & known
    tp = len(adm & declared_ml)
    fp = len(adm - declared_ml)
    fn = len((declared_ml - adm))
    prec = tp / (tp + fp) if (tp + fp) else None
    rec = tp / (tp + fn) if (tp + fn) else None
    return {
        "tp": tp, "fp": fp, "fn": fn,
        "precision": prec,
        "precision_ci": list(wilson_ci(tp, tp + fp)) if (tp + fp) else None,
        "recall": rec,
        "recall_ci": list(wilson_ci(tp, tp + fn)) if (tp + fn) else None,
        "f1": (2 * prec * rec / (prec + rec)) if prec and rec else None,
    }


def analyse(rep, df):
    b_filt, b_map = baseline_paths(rep)
    a_map_path = ROOT / "results" / f"f7_ablation_{DOMAIN}_rep{rep}_map.csv"
    if not (b_filt.exists() and b_map.exists()):
        return {"error": f"baseline rq3_{DOMAIN}_rep{rep} not on disk"}
    if not a_map_path.exists():
        return {"error": f"ablation rep{rep} not on disk"}

    cfg = PIPELINES[DOMAIN]
    col = cfg["map"]["output_col"]
    # `contradiction` is the frozen code/config name for operational disagreement.
    is_contra = cfg["contradiction"]["is_contradiction"]

    bm = pd.read_csv(b_map, index_col="row_id")
    am = pd.read_csv(a_map_path, index_col="row_id")
    b_adm, a_adm = set(bm.index), set(am.index)

    b_contra = [r for r in b_adm if is_contra(True, str(bm.loc[r, col]))]
    a_contra = [r for r in a_adm if is_contra(True, str(am.loc[r, col]))]
    b_rate = len(b_contra) / len(b_adm) if b_adm else 0.0
    a_rate = len(a_contra) / len(a_adm) if a_adm else 0.0

    out = {
        "replicate": rep,
        "baseline_admitted": len(b_adm),
        "ablated_admitted": len(a_adm),
        "admitted_delta": len(a_adm) - len(b_adm),
        "contradiction_baseline": {
            "k": len(b_contra), "n": len(b_adm), "rate": b_rate,
            "ci": list(wilson_ci(len(b_contra), len(b_adm))) if b_adm else None},
        "contradiction_ablated": {
            "k": len(a_contra), "n": len(a_adm), "rate": a_rate,
            "ci": list(wilson_ci(len(a_contra), len(a_adm))) if a_adm else None},
        "contradiction_delta_pp": (a_rate - b_rate) * 100,
        # Legacy JSON keys retained for compatibility; this changes a criterion,
        # not cosmetic wording.
        "rows_dropped_by_rewording": sorted(b_adm - a_adm),
        "rows_added_by_rewording": sorted(a_adm - b_adm),
    }

    # --- secondary: filter precision against the authors' own category -------
    if "topic_label_v2" in df.columns:
        labelled = df[df.topic_label_v2 != "unknown"]
        declared_ml = set(labelled[labelled.topic_label_v2 == "machine_learning"].index)
        out["n_labelled"] = len(labelled)
        out["n_unknown_excluded"] = int((df.topic_label_v2 == "unknown").sum())
        out["precision_baseline"] = prf(b_adm, declared_ml, labelled.index)
        out["precision_ablated"] = prf(a_adm, declared_ml, labelled.index)

    # --- which prediction did we land on? -----------------------------------
    if a_rate < 0.10 and len(a_adm) < len(b_adm):
        out["verdict"] = (
            "H1 pattern observed: disagreement fell below 10% with fewer admissions "
            "for this predicate pair and setup; no general causal share is estimated"
        )
    elif abs(a_rate - b_rate) < 0.05:
        out["verdict"] = (
            "H2 pattern: the measured disagreement rate changed little in this "
            "replicate; this result alone does not identify a mechanism"
        )
    else:
        out["verdict"] = (
            "H3 pattern: mixed outcome; interpret with paired proxy analyses and "
            "the scope limits stated in the findings"
        )
    return out


def report(results):
    print("\n" + "=" * 70)
    print("F7 -- filter-criterion intervention, aboutness -> primary topic (abstracts)")
    print("=" * 70)
    for r in results:
        if "error" in r:
            print(f"  rep{r.get('replicate','?')}: {r['error']}")
            continue
        cb, ca = r["contradiction_baseline"], r["contradiction_ablated"]
        print(f"\nbaseline = rq3 replicate {r['replicate']}")
        print(f"  filter admitted     {cb['n']} -> {ca['n']} "
              f"({r['admitted_delta']:+d})")
        print(f"  OPERATIONAL DISAGREEMENT {cb['rate']:.2%} [{cb['ci'][0]:.2%}, {cb['ci'][1]:.2%}]"
              f"  ->  {ca['rate']:.2%} [{ca['ci'][0]:.2%}, {ca['ci'][1]:.2%}]")
        print(f"                      delta {r['contradiction_delta_pp']:+.2f} pp"
              f"   <- primary metric")
        if "precision_baseline" in r:
            pb, pa = r["precision_baseline"], r["precision_ablated"]
            print(f"  filter precision    {pb['precision']:.2%} -> {pa['precision']:.2%}"
                  f"   (recall {pb['recall']:.2%} -> {pa['recall']:.2%})")
            print(f"  (against topic_label_v2; {r['n_unknown_excluded']} unknown rows excluded)")
        print(f"  rows excluded by changed criterion: {len(r['rows_dropped_by_rewording'])}, "
              f"newly admitted: {len(r['rows_added_by_rewording'])}")
        print(f"\n  => {r['verdict']}")


def main():
    ap = argparse.ArgumentParser(description="F7 scoped filter-criterion intervention")
    ap.add_argument("--baseline-rep", type=int, default=1,
                    help="which rq3 abstracts replicate to compare against")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--aggregate-only", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--yes", action="store_true")
    ap.add_argument("--batch-delay", type=float, default=None)
    ap.add_argument("--patient", action="store_true")
    args = ap.parse_args()

    df = load_rows()
    cfg = PIPELINES[DOMAIN]
    rep = args.baseline_rep

    print("=" * 70)
    print("OperatorBench -- F7 scoped predicate-criterion intervention")
    print("=" * 70)
    print(f"\nBaseline filter (frozen):\n  {cfg['filter']['predicate']}")
    print(f"\nAblated filter (this run):\n  {ABLATED_FILTER}")
    print(f"\nMap (UNCHANGED, imported from configs/pipelines.py):\n  "
          f"{cfg['map']['instruction']}")
    print(f"\nRows: {len(df)}   Baseline: rq3_{DOMAIN}_rep{rep}")

    if args.dry_run:
        bf, bm = baseline_paths(rep)
        print(f"\nBaseline on disk: filter {'OK' if bf.exists() else 'MISSING'}, "
              f"map {'OK' if bm.exists() else 'MISSING'}")
        print(f"Estimated tokens: ~{len(df) * 1014 / 1e6:.2f}M filter "
              f"+ map on admitted rows (~0.66M total)")
        print("--dry-run: no API calls made.")
        return

    if not args.aggregate_only:
        from harness.runner import PipelineRunner
        from harness.intercept import LLMInterceptor

        print(f"\nEstimated tokens: ~0.66M")
        if not args.yes and input("\nProceed? [y/N] ").strip().lower() != "y":
            print("Aborted.")
            return

        interceptor = LLMInterceptor(log_dir=ROOT / "logs" / "llm_calls",
                                     tag="f7_ablation")
        interceptor.install()
        runner = PipelineRunner(
            model=BACKENDS["hosted"]["model"],
            backend="hosted",
            log_dir=ROOT / "logs",
            cache_path=ROOT / "cache.db",
            cache_enabled=False,
            batch_delay_seconds=args.batch_delay,
            patient=args.patient,
        )
        print(f"\n--- f7 ablation {DOMAIN}, baseline rep {rep} ---")
        run_replicate(runner, df.copy(), rep, force=args.force)

    # Analyse EVERY ablation replicate on disk, not just the one just run --
    # otherwise running rep2 would overwrite rep1's entry in the summary.
    results = [analyse(r, df) for r in (1, 2, 3)
               if (ROOT / "results" / f"f7_ablation_{DOMAIN}_rep{r}_map.csv").exists()]
    report(results)

    rates = [r["contradiction_ablated"]["rate"] for r in results if "error" not in r]
    spread_pp = (max(rates) - min(rates)) * 100 if len(rates) > 1 else None
    if spread_pp is not None:
        print(f"\nintervention disagreement rate across {len(rates)} replicates: "
              f"spread {spread_pp:.2f} pp")
    out = ROOT / "results" / "f7_ablation_summary.json"
    out.write_text(json.dumps({
        "purpose": "F7 scoped criterion intervention: inclusive aboutness to exclusive primary topic",
        "result_field_names_note": ("contradiction_* keys are retained for artifact "
                                    "compatibility; they store the operational "
                                    "criterion-disagreement metric"),
        "baseline_filter_langex": cfg["filter"]["predicate"],
        "ablated_filter_langex": ABLATED_FILTER,
        "map_langex": cfg["map"]["instruction"],
        "map_changed": False,
        "contradiction_predicate_changed": False,
        "limitation": ("changes only this filter criterion; does NOT alter LOTUS's "
                       "claim-verification vs instruction-following wrappers, and "
                       "does not estimate a general causal share"),
        "interpretation_scope": ("two hosted replicates on abstracts, one model, "
                                 "one predicate pair; criterion-disagreement "
                                 "counts and proxy tradeoff are setup-specific"),
        "pre_registered": ["H1 collapse below ~10%", "H2 no change",
                           "H3 mixed -- precision rises, residual rate stays high"],
        "n_replicates_on_disk": len(results),
        "ablated_rate_spread_pp": spread_pp,
        "replicates": results,
    }, indent=2, default=str))
    print(f"\nWrote {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
