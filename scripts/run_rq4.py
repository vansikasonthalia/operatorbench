"""
OperatorBench -- RQ4: schema-position perturbation

WHAT THIS MEASURES
------------------
The `submission_month` date is already present in each abstract. This run also
places it in a typed column and supplies it in the operator context. It tests
whether that schema-position change alters operator outputs on abstracts.

The canonical inference is in `scripts/rq4_paired.py`: it compares operational
criterion-disagreement status on rows admitted under both conditions using
McNemar's exact test. The two replicates have only three discordant pairs each,
opposite-signed deltas, and p = 1.0 in both. Report this as no detectable change
under this manipulation, not as evidence that context effects are absent.

The unpaired rates calculated below use different admitted row sets. They are
retained as descriptive diagnostics only and are not the primary RQ4 result.
The code and frozen JSON field names retain `contradiction` for the historical
operational predicate; the paper calls the outcome criterion disagreement.

WHAT IS INJECTED
----------------
`submission_month` (YYYY-MM), the authors' real arXiv submission date, added as
a named column. Whether a paper is about machine learning cannot depend on when
it was submitted, so the predicate's semantics are unchanged by construction.

The date is already in the text the operators read. The manipulation adds no
new date information, but changes how the date is presented. The paired
analysis found no detectable change in criterion-disagreement status in these
two runs. Do not describe this as information leakage or as a general result
about metadata columns.

ABSTRACTS ONLY
--------------
The IMDB test split carries no production metadata at all. Synthesising
timestamps would abandon the real-but-irrelevant design that makes the finding
actionable rather than adversarial, so reviews is a stated limitation, not an
omission. See `scripts/derive_injection_cols.py`.

DENOMINATOR
-----------
Restricted to the 449 abstracts rows carrying a real submission date. Rows
without one are excluded, never given a placeholder value -- injecting a
fabricated "unknown" would be measuring our own placeholder. The baseline is
the matching RQ3 replicate restricted to the same rows, so every comparison is
paired on identical inputs.

Usage:
    python scripts/run_rq4.py --dry-run          # print langex + counts, no API
    python scripts/run_rq4.py --n-replicates 2 --batch-delay 12 --patient --yes
    python scripts/run_rq4.py --aggregate-only
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
INJECTION_CSV = ROOT / "data" / "abstracts" / "abstracts_v1_injection.csv"
LABELS_CSV = ROOT / "data" / "abstracts" / "abstracts_v1_labels_v2.csv"

# The injected langex. Identical to the RQ3 predicate/instruction except for
# the bracketed metadata clause, so any measured change is attributable to the
# clause and nothing else. Logged into the run meta for auditability.
INJECTED_FILTER = (
    "Is this paper about machine learning? "
    "[submitted: {submission_month}] {abstract_text}"
)
INJECTED_MAP = (
    "Respond with exactly one word. Classify the primary topic of this paper "
    "as: ML, systems, theory, or other. "
    "[submitted: {submission_month}] Abstract: {abstract_text}"
)


def load_eligible():
    """The 449 rows with a real submission month, with labels attached."""
    df = pd.read_csv(ROOT / PIPELINES[DOMAIN]["data_file"],
                     escapechar="\\", index_col="row_id")
    assert len(df) == 500 and list(df.index) == list(range(500))

    inj = pd.read_csv(INJECTION_CSV, index_col="row_id")
    df = df.join(inj[["submission_month", "has_submission_month"]])
    eligible = df[df["has_submission_month"]].drop(columns=["has_submission_month"])

    if LABELS_CSV.exists():
        lab = pd.read_csv(LABELS_CSV, index_col="row_id")
        eligible = eligible.join(lab[["topic_label_v2"]])
    return eligible


def baseline_paths(rep):
    return (ROOT / "results" / f"rq3_{DOMAIN}_rep{rep}_filter.csv",
            ROOT / "results" / f"rq3_{DOMAIN}_rep{rep}_map.csv")


def run_replicate(runner, df, rep, force=False):
    run_id = f"rq4_{DOMAIN}_rep{rep}"
    map_path = ROOT / "results" / f"{run_id}_map.csv"
    if map_path.exists() and not force:
        print(f"  [rep {rep}] already on disk -- loading (use --force to redo)")
        return load_completed_pipeline(ROOT / "results", run_id, df.index,
                                       PIPELINES[DOMAIN]["map"]["output_col"])

    cfg = PIPELINES[DOMAIN]
    results = runner.run_filter_map(
        df=df,
        domain=DOMAIN,
        filter_predicate=INJECTED_FILTER,
        map_instruction=INJECTED_MAP,
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
        "injected_filter_langex": INJECTED_FILTER,
        "injected_map_langex": INJECTED_MAP,
        "baseline": f"rq3_{DOMAIN}_rep{rep}",
        "n_eligible_rows": len(df),
        "injection_column": "submission_month",
        "note": "paired against the RQ3 replicate restricted to the same rows",
    })
    meta_path.write_text(json.dumps(existing_meta, indent=2, default=str))
    return results["map_results"]


def analyse(rep, eligible):
    """Paired baseline-vs-injected analysis for one replicate."""
    b_filt, b_map = baseline_paths(rep)
    i_map = ROOT / "results" / f"rq4_{DOMAIN}_rep{rep}_map.csv"
    if not (b_filt.exists() and b_map.exists()):
        return {"error": f"baseline rq3_{DOMAIN}_rep{rep} not on disk"}
    if not i_map.exists():
        return {"error": f"injected rq4 rep{rep} not on disk"}

    cfg = PIPELINES[DOMAIN]
    col = cfg["map"]["output_col"]
    is_contra = cfg["contradiction"]["is_contradiction"]

    rows = set(eligible.index)
    bf = pd.read_csv(b_filt, index_col="row_id")
    bm = pd.read_csv(b_map, index_col="row_id")
    im = pd.read_csv(i_map, index_col="row_id")

    b_adm = set(bf.index) & rows
    i_adm = set(im.index) & rows

    # --- filter-level drift -------------------------------------------------
    flipped_in = sorted(i_adm - b_adm)     # injection made the filter admit
    flipped_out = sorted(b_adm - i_adm)    # injection made the filter reject
    filter_flips = len(flipped_in) + len(flipped_out)

    # --- map-level drift, on rows admitted in BOTH --------------------------
    both = sorted(b_adm & i_adm)
    map_flips = [r for r in both
                 if str(bm.loc[r, col]).strip().lower()
                 != str(im.loc[r, col]).strip().lower()]

    # Legacy descriptive rates on each condition's admitted set. These use
    # different row sets; scripts/rq4_paired.py is the canonical analysis.
    b_contra = [r for r in b_adm if is_contra(True, str(bm.loc[r, col]))]
    i_contra = [r for r in i_adm if is_contra(True, str(im.loc[r, col]))]
    b_rate = len(b_contra) / len(b_adm) if b_adm else 0.0
    i_rate = len(i_contra) / len(i_adm) if i_adm else 0.0

    out = {
        "replicate": rep,
        "n_eligible": len(rows),
        "baseline_admitted": len(b_adm),
        "injected_admitted": len(i_adm),
        "filter_flip_rate": filter_flips / len(rows),
        "filter_flip_ci": list(wilson_ci(filter_flips, len(rows))),
        "filter_flipped_in": flipped_in,
        "filter_flipped_out": flipped_out,
        "map_flip_rate": len(map_flips) / len(both) if both else None,
        "map_flip_ci": list(wilson_ci(len(map_flips), len(both))) if both else None,
        "map_flipped_rows": map_flips,
        "contradiction_baseline": {
            "k": len(b_contra), "n": len(b_adm), "rate": b_rate,
            "ci": list(wilson_ci(len(b_contra), len(b_adm))) if b_adm else None},
        "contradiction_injected": {
            "k": len(i_contra), "n": len(i_adm), "rate": i_rate,
            "ci": list(wilson_ci(len(i_contra), len(i_adm))) if i_adm else None},
        "contradiction_delta_pp": (i_rate - b_rate) * 100,
    }

    # --- stratified: does injection hurt boundary rows more? ---------------
    if "topic_label_v2" in eligible.columns:
        strata = {}
        for name in ("machine_learning", "other"):
            sel = set(eligible[eligible.topic_label_v2 == name].index)
            bb, ii = b_adm & sel, i_adm & sel
            kb = sum(1 for r in bb if is_contra(True, str(bm.loc[r, col])))
            ki = sum(1 for r in ii if is_contra(True, str(im.loc[r, col])))
            strata[name] = {
                "baseline": {"k": kb, "n": len(bb),
                             "rate": kb / len(bb) if bb else None},
                "injected": {"k": ki, "n": len(ii),
                             "rate": ki / len(ii) if ii else None},
            }
        out["by_declared_label"] = strata
    return out


def report(results):
    print("\n" + "=" * 68)
    print("RQ4 -- submission-date schema-position perturbation (abstracts)")
    print("=" * 68)
    for r in results:
        if "error" in r:
            print(f"  rep{r.get('replicate','?')}: {r['error']}")
            continue
        cb, ci_ = r["contradiction_baseline"], r["contradiction_injected"]
        print(f"\nreplicate {r['replicate']}  (eligible rows: {r['n_eligible']})")
        print(f"  filter admitted   {cb['n']} -> {ci_['n']}")
        print(f"  filter flips      {r['filter_flip_rate']:.2%} "
              f"(in {len(r['filter_flipped_in'])}, out {len(r['filter_flipped_out'])})")
        if r["map_flip_rate"] is not None:
            n_both = len(r['map_flipped_rows']) and round(
                len(r['map_flipped_rows']) / r['map_flip_rate'])
            print(f"  map flips         {r['map_flip_rate']:.2%} "
                  f"({len(r['map_flipped_rows'])} of {n_both} admitted in both)")
        print(f"  DESCRIPTIVE DISAGREEMENT RATE (unpaired) {cb['rate']:.2%} -> {ci_['rate']:.2%}  "
              f"(delta {r['contradiction_delta_pp']:+.2f} pp)")
        if "by_declared_label" in r:
            for name, s in r["by_declared_label"].items():
                b, i = s["baseline"], s["injected"]
                if b["rate"] is None or i["rate"] is None:
                    continue
                print(f"    {name:<17} {b['rate']:.1%} (n={b['n']}) -> "
                      f"{i['rate']:.1%} (n={i['n']})")


def main():
    ap = argparse.ArgumentParser(description="RQ4 schema-position perturbation")
    ap.add_argument("--n-replicates", type=int, default=2)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--aggregate-only", action="store_true")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the langex and eligible counts; no API calls")
    ap.add_argument("--yes", action="store_true")
    ap.add_argument("--batch-delay", type=float, default=None)
    ap.add_argument("--patient", action="store_true")
    args = ap.parse_args()

    eligible = load_eligible()

    print("=" * 68)
    print("OperatorBench -- RQ4: submission-date schema-position perturbation")
    print(f"Domain: {DOMAIN} (reviews excluded -- no real metadata, by design)")
    print(f"Eligible rows: {len(eligible)} of 500 (have a real submission date)")
    print("=" * 68)
    print("\nInjected filter langex:\n  " + INJECTED_FILTER)
    print("\nInjected map langex:\n  " + INJECTED_MAP)
    print(f"\nExample value: submission_month = "
          f"{eligible['submission_month'].iloc[0]!r}")

    if args.dry_run:
        print(f"\nBaselines expected on disk:")
        for rep in range(1, args.n_replicates + 1):
            bf, bm = baseline_paths(rep)
            print(f"  rep{rep}: filter {'OK' if bf.exists() else 'MISSING'}, "
                  f"map {'OK' if bm.exists() else 'MISSING'}")
        est = len(eligible) * args.n_replicates * 1014 / 1e6
        print(f"\nEstimated tokens: ~{est:.2f}M filter + map on admitted rows")
        print("--dry-run: no API calls made.")
        return

    if not args.aggregate_only:
        from harness.runner import PipelineRunner
        from harness.intercept import LLMInterceptor

        est = len(eligible) * args.n_replicates * 1014 / 1e6
        print(f"\nEstimated tokens: ~{est:.2f}M (plus map on admitted rows)")
        if not args.yes and input("\nProceed? [y/N] ").strip().lower() != "y":
            print("Aborted.")
            return

        interceptor = LLMInterceptor(log_dir=ROOT / "logs" / "llm_calls", tag="rq4")
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
        for rep in range(1, args.n_replicates + 1):
            print(f"\n--- rq4 {DOMAIN} replicate {rep}/{args.n_replicates} ---")
            run_replicate(runner, eligible.copy(), rep, force=args.force)

    results = [analyse(rep, eligible) for rep in range(1, args.n_replicates + 1)]
    report(results)
    out = ROOT / "results" / "rq4_summary.json"
    out.write_text(json.dumps({
        "domain": DOMAIN,
        "injected_filter_langex": INJECTED_FILTER,
        "injected_map_langex": INJECTED_MAP,
        "n_eligible": len(eligible),
        "primary_metric": ("paired criterion-disagreement status; see "
                           "results/rq4_paired.json and scripts/rq4_paired.py"),
        "legacy_unpaired_rate_note": ("per-condition rates below use different "
                                      "admitted row sets and are descriptive only"),
        "result_field_names_note": ("contradiction_* keys are retained for "
                                    "artifact compatibility; they store the "
                                    "operational criterion-disagreement metric"),
        "reviews": "excluded -- no production metadata in the frozen source",
        "replicates": results,
    }, indent=2, default=str))
    print(f"\nWrote {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
