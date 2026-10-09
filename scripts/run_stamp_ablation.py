"""
OperatorBench -- F13 control: does the model read the arXiv category stamp?

WHY THIS EXISTS
---------------
`topic_label_v2` (the author-declared category proxy behind F10/F11/F12) is
parsed from the arXiv stamp -- e.g. `arXiv:1512.00608v1 [math.AC] 2 Dec 2015`
-- which sits INSIDE the 4,000-char window both operators receive (median
position: char 80). The model can therefore see the label the proxy analyses
compare it against. See `paper/FINDINGS.md` F13.

This run removes only that cue and re-runs the frozen abstracts pipeline once.

WHAT CHANGES, AND WHAT DOES NOT
-------------------------------
Changed:   every stamp match in the first 4,000 chars (stamp + trailing date) is
           overwritten with the SAME NUMBER OF SPACES. Character positions are
           preserved, so the truncated window covers exactly the same text --
           the stamp is the only difference.
Unchanged: filter predicate, map instruction, contradiction predicate, model,
           row set (all 500), batching. All imported from configs/pipelines.py.

The 51 rows without a stamp (`unknown`) are sent unchanged. They are kept so the
admitted set is comparable with the baseline, and they double as a within-run
check: any decision change on them is pure run-to-run noise.

Baseline: frozen `rq3_abstracts_rep1` (stamp present).

PRE-REGISTERED DECISION RULES (committed to git before the run -- do not edit)
------------------------------------------------------------------------------
Noise ceiling, measured 1 Oct 2026 before the run: across the 8 existing
abstracts runs (RQ1 reps 1-5, RQ3 reps 1-3), the most filter decisions on
labelled rows that ever differ between ANY two runs is 2.

  P1 (filter relies on the stamp): more than 4 labelled-row filter decisions
     (2x the noise ceiling) differ from rq3 rep1, AND the paired McNemar test
     on per-row agreement with topic_label_v2 gives p < 0.05 with agreement
     LOWER without the stamp.
  P2 (no detectable reliance): at most 4 labelled-row flips, and F10 precision
     and recall without the stamp both fall inside the stamp-present Wilson
     95% intervals.
  P3 (small or mixed effect): anything else. Report the numbers, and rescope
     F10-F12 to "agreement with a proxy the model could see".

Secondary, reported whatever the outcome: the F11 split (disagreement rate by
declared category), the overall operational disagreement rate (F2) with a
paired McNemar on rows admitted in both runs, and map-label changes on those
rows.

Scope: one replicate, one model, abstracts only. The P2 outcome means "no
detectable reliance at this sample size", not "the model ignores the stamp".

Usage:
    python scripts/run_stamp_ablation.py --dry-run          # no API calls
    python scripts/run_stamp_ablation.py --batch-delay 12 --patient --yes
    python scripts/run_stamp_ablation.py --aggregate-only   # re-analyse on disk
"""

import sys
import json
import math
import re
import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from configs.pipelines import PIPELINES, BACKENDS
from harness.metrics import wilson_ci

DOMAIN = "abstracts"
RUN_ID = f"stamp_ablation_{DOMAIN}_rep1"
LABELS_CSV = ROOT / "data" / "abstracts" / "abstracts_v1_labels_v2.csv"
BASELINE_FILTER = ROOT / "results" / f"rq3_{DOMAIN}_rep1_filter.csv"
BASELINE_MAP = ROOT / "results" / f"rq3_{DOMAIN}_rep1_map.csv"
OUT_FILTER = ROOT / "results" / f"{RUN_ID}_filter.csv"
OUT_MAP = ROOT / "results" / f"{RUN_ID}_map.csv"
SUMMARY = ROOT / "results" / "stamp_ablation_summary.json"

WINDOW = 4000          # the runner's truncation (D-10); stamps are blanked inside it
NOISE_CEILING = 2      # measured before the run; see docstring
FLIP_THRESHOLD = 2 * NOISE_CEILING

# Same anchor as scripts/derive_topic_label_v2.py::STAMP_RE, extended to swallow
# the trailing submission date so no part of the stamp survives.
STAMP_BLANK_RE = re.compile(
    r"arXiv:\s*[a-zA-Z0-9./-]+\s*\[[^\]]{2,40}\]"
    r"(?:\s+\d{1,2}\s+[A-Z][a-z]{2}\s+\d{4})?"
)


def blank_stamps(text: str, window: int = WINDOW) -> tuple[str, int]:
    """Overwrite every stamp inside the window with spaces of equal length.

    Returns (new_text, n_blanked). Length and every non-stamp character are
    preserved, so truncation to `window` sees identical surrounding text.
    """
    head, tail = text[:window], text[window:]
    n = 0

    def _spaces(m: re.Match) -> str:
        nonlocal n
        n += 1
        return " " * len(m.group(0))

    return STAMP_BLANK_RE.sub(_spaces, head) + tail, n


def load_rows() -> pd.DataFrame:
    df = pd.read_csv(ROOT / PIPELINES[DOMAIN]["data_file"],
                     escapechar="\\", index_col="row_id")
    assert len(df) == 500 and list(df.index) == list(range(500)), \
        "frozen abstracts set changed -- STOP, do not proceed (D-09)"
    lab = pd.read_csv(LABELS_CSV, index_col="row_id")
    return df.join(lab[["arxiv_category", "topic_label_v2"]])


def blanked_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Stamp-blanked copy of the frozen frame, plus a leak audit."""
    out = df.copy()
    text_col = PIPELINES[DOMAIN]["text_col"]
    counts, residual = {}, []
    for rid, text in out[text_col].items():
        new, n = blank_stamps(str(text))
        assert len(new) == len(str(text)), f"row {rid}: length changed"
        out.at[rid, text_col] = new
        counts[rid] = n
        cat = df.at[rid, "arxiv_category"]
        # Does the category code still appear anywhere the model will see?
        if isinstance(cat, str) and cat in new[:WINDOW]:
            residual.append(int(rid))

    labelled = df.index[df.topic_label_v2 != "unknown"]
    audit = {
        "labelled_rows": int(len(labelled)),
        "labelled_rows_blanked": int(sum(counts[r] > 0 for r in labelled)),
        "unknown_rows_changed": int(sum(counts[r] > 0 for r in df.index
                                        if df.at[r, "topic_label_v2"] == "unknown")),
        "rows_with_category_code_still_in_window": residual,
    }
    return out.drop(columns=["arxiv_category", "topic_label_v2"]), audit


def mcnemar_exact(b: int, c: int) -> float:
    """Exact two-sided McNemar p (same method as scripts/f7_precision_paired.py)."""
    n = b + c
    if n == 0:
        return 1.0
    obs = min(b, c)
    return min(1.0, 2 * sum(math.comb(n, i) for i in range(obs + 1)) / 2 ** n)


def admitted_set(path: Path) -> set:
    """RQ3-style filter CSVs list admitted rows only."""
    return set(pd.read_csv(path, usecols=["row_id"]).row_id)


def prf(adm: set, declared_ml: set, labelled: set) -> dict:
    a = adm & labelled
    tp, fp, fn = len(a & declared_ml), len(a - declared_ml), len(declared_ml - a)
    return {
        "tp": tp, "fp": fp, "fn": fn,
        "precision": tp / (tp + fp) if tp + fp else None,
        "precision_ci": list(wilson_ci(tp, tp + fp)) if tp + fp else None,
        "recall": tp / (tp + fn) if tp + fn else None,
        "recall_ci": list(wilson_ci(tp, tp + fn)) if tp + fn else None,
    }


def disagreement_by_label(map_df: pd.DataFrame, labels: pd.Series) -> dict:
    cfg = PIPELINES[DOMAIN]
    col, is_contra = cfg["map"]["output_col"], cfg["contradiction"]["is_contradiction"]
    out = {}
    for name, rows in [("all_admitted", list(map_df.index)),
                       ("ml_declared", [r for r in map_df.index if labels[r] == "machine_learning"]),
                       ("non_ml_declared", [r for r in map_df.index if labels[r] == "other"])]:
        k = sum(is_contra(True, str(map_df.at[r, col])) for r in rows)
        n = len(rows)
        out[name] = {"k": k, "n": n, "rate": k / n if n else None,
                     "ci": list(wilson_ci(k, n)) if n else None}
    return out


def analyse(df: pd.DataFrame, audit: dict | None) -> dict:
    if not OUT_MAP.exists():
        return {"error": f"{RUN_ID} not on disk -- run it first"}
    cfg = PIPELINES[DOMAIN]
    col, is_contra = cfg["map"]["output_col"], cfg["contradiction"]["is_contradiction"]
    labels = df.topic_label_v2
    labelled = set(df.index[labels != "unknown"])
    unknown = set(df.index[labels == "unknown"])
    declared_ml = set(df.index[labels == "machine_learning"])

    base_adm, abl_adm = admitted_set(BASELINE_FILTER), admitted_set(OUT_FILTER)
    bm = pd.read_csv(BASELINE_MAP, usecols=["row_id", col], index_col="row_id")
    am = pd.read_csv(OUT_MAP, usecols=["row_id", col], index_col="row_id")

    flips_lab = sorted((base_adm ^ abl_adm) & labelled)
    flips_unknown = sorted((base_adm ^ abl_adm) & unknown)

    # Paired agreement with the proxy on labelled rows (P1's test).
    def agrees(adm, r):
        return (r in adm) == (r in declared_ml)
    b = sum(agrees(base_adm, r) and not agrees(abl_adm, r) for r in labelled)
    c = sum(agrees(abl_adm, r) and not agrees(base_adm, r) for r in labelled)
    p_agree = mcnemar_exact(b, c)
    agree_base = sum(agrees(base_adm, r) for r in labelled) / len(labelled)
    agree_abl = sum(agrees(abl_adm, r) for r in labelled) / len(labelled)

    p_base, p_abl = prf(base_adm, declared_ml, labelled), prf(abl_adm, declared_ml, labelled)

    # Paired disagreement status on rows admitted in both runs (F2 secondary).
    both = sorted(base_adm & abl_adm)
    d_b = sum(is_contra(True, str(bm.at[r, col])) and not is_contra(True, str(am.at[r, col])) for r in both)
    d_c = sum(is_contra(True, str(am.at[r, col])) and not is_contra(True, str(bm.at[r, col])) for r in both)
    map_changed = [r for r in both if str(bm.at[r, col]) != str(am.at[r, col])]

    def inside(x, ci):
        return x is not None and ci is not None and ci[0] <= x <= ci[1]

    many_flips = len(flips_lab) > FLIP_THRESHOLD
    if many_flips and p_agree < 0.05 and agree_abl < agree_base:
        verdict = "P1 -- filter relies on the stamp: rescope F10-F12"
    elif (not many_flips and inside(p_abl["precision"], p_base["precision_ci"])
          and inside(p_abl["recall"], p_base["recall_ci"])):
        verdict = "P2 -- no detectable reliance on the stamp at this sample size"
    else:
        verdict = "P3 -- small or mixed effect: report numbers; describe F10-F12 as agreement with a proxy the model could see"

    return {
        "verdict": verdict,
        "leak_audit": audit,
        "noise_ceiling_labelled_flips": NOISE_CEILING,
        "flip_threshold": FLIP_THRESHOLD,
        "filter": {
            "admitted_baseline": len(base_adm), "admitted_no_stamp": len(abl_adm),
            "labelled_flips": len(flips_lab), "labelled_flip_rows": flips_lab,
            "labelled_flips_detail": [
                {"row_id": r, "declared": labels[r], "category": df.at[r, "arxiv_category"],
                 "with_stamp": r in base_adm, "without_stamp": r in abl_adm}
                for r in flips_lab],
            "unknown_row_flips_noise_check": flips_unknown,
        },
        "proxy_agreement_paired": {
            "test": "McNemar exact, per-row filter decision vs topic_label_v2",
            "agreement_with_stamp": round(agree_base, 4),
            "agreement_without_stamp": round(agree_abl, 4),
            "with_stamp_only": b, "without_stamp_only": c,
            "p_two_sided": round(p_agree, 5),
        },
        "F10_with_stamp": p_base,
        "F10_without_stamp": p_abl,
        "F11_with_stamp": disagreement_by_label(bm, labels),
        "F11_without_stamp": disagreement_by_label(am, labels),
        "F2_paired_on_rows_admitted_in_both": {
            "n": len(both), "disagree_with_stamp_only": d_b,
            "disagree_without_stamp_only": d_c, "mcnemar_p": round(mcnemar_exact(d_b, d_c), 5),
        },
        "map_label_changes_on_rows_admitted_in_both": map_changed,
    }


def report(res: dict) -> None:
    print("\n" + "=" * 70)
    print("F13 control -- arXiv stamp removed (abstracts, vs rq3 rep1)")
    print("=" * 70)
    if "error" in res:
        print("  " + res["error"])
        return
    f, pa = res["filter"], res["proxy_agreement_paired"]
    print(f"  admitted            {f['admitted_baseline']} -> {f['admitted_no_stamp']}")
    print(f"  labelled flips      {f['labelled_flips']}  (noise ceiling {res['noise_ceiling_labelled_flips']}, "
          f"threshold >{res['flip_threshold']})")
    print(f"  unknown-row flips   {len(f['unknown_row_flips_noise_check'])}  (pure noise check)")
    print(f"  proxy agreement     {pa['agreement_with_stamp']:.2%} -> {pa['agreement_without_stamp']:.2%}"
          f"   McNemar p={pa['p_two_sided']}")
    for name in ("F10_with_stamp", "F10_without_stamp"):
        p = res[name]
        print(f"  {name:18s}  precision {p['precision']:.2%}  recall {p['recall']:.2%}")
    for name in ("F11_with_stamp", "F11_without_stamp"):
        s = res[name]
        print(f"  {name:18s}  ML-declared {s['ml_declared']['k']}/{s['ml_declared']['n']}  "
              f"non-ML {s['non_ml_declared']['k']}/{s['non_ml_declared']['n']}  "
              f"all {s['all_admitted']['k']}/{s['all_admitted']['n']}")
    print(f"\n  => {res['verdict']}")


def main() -> None:
    ap = argparse.ArgumentParser(description="F13 stamp-removal control")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--aggregate-only", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--yes", action="store_true")
    ap.add_argument("--batch-delay", type=float, default=None)
    ap.add_argument("--patient", action="store_true")
    args = ap.parse_args()

    df = load_rows()
    blanked, audit = blanked_frame(df)
    print(f"Stamps blanked on {audit['labelled_rows_blanked']}/{audit['labelled_rows']} labelled rows; "
          f"unknown rows changed: {audit['unknown_rows_changed']}; "
          f"category code still in window on {len(audit['rows_with_category_code_still_in_window'])} rows")

    if args.dry_run:
        print(f"Baseline on disk: filter {'OK' if BASELINE_FILTER.exists() else 'MISSING'}, "
              f"map {'OK' if BASELINE_MAP.exists() else 'MISSING'}")
        print("Estimated tokens: ~0.66M (500 filter + ~155 map at ~1014 tok/row)")
        print("--dry-run: no API calls made.")
        return

    if not args.aggregate_only:
        if OUT_MAP.exists() and not args.force:
            print(f"{OUT_MAP.name} already on disk -- analysing it (use --force to redo)")
        else:
            if not args.yes and input("\nProceed (~0.66M tokens)? [y/N] ").strip().lower() != "y":
                print("Aborted.")
                return
            from harness.runner import PipelineRunner
            from harness.intercept import LLMInterceptor

            interceptor = LLMInterceptor(log_dir=ROOT / "logs" / "llm_calls", tag="stamp_ablation")
            interceptor.install()
            runner = PipelineRunner(
                model=BACKENDS["hosted"]["model"], backend="hosted",
                log_dir=ROOT / "logs", cache_path=ROOT / "cache.db", cache_enabled=False,
                batch_delay_seconds=args.batch_delay, patient=args.patient,
            )
            cfg = PIPELINES[DOMAIN]
            runner.run_filter_map(
                df=blanked, domain=DOMAIN,
                filter_predicate=cfg["filter"]["predicate"],      # frozen, unchanged
                map_instruction=cfg["map"]["instruction"],        # frozen, unchanged
                map_output_col=cfg["map"]["output_col"],
                valid_labels=cfg["map"]["valid_labels"],
                run_id=RUN_ID,
            )
            meta_path = ROOT / "results" / f"{RUN_ID}_meta.json"
            meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
            meta.update({
                "purpose": "F13 control: arXiv category stamp blanked (equal-length spaces)",
                "baseline_run": f"rq3_{DOMAIN}_rep1",
                "changed": "stamp text inside the 4,000-char window only",
                "unchanged": "filter predicate, map instruction, contradiction predicate, model, rows",
                "leak_audit": audit,
            })
            meta_path.write_text(json.dumps(meta, indent=2, default=str))

    res = analyse(df, audit)
    report(res)
    if "error" not in res:
        SUMMARY.write_text(json.dumps(res, indent=2, default=str))
        print(f"\nWrote {SUMMARY.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
