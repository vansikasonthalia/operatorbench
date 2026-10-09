"""
OperatorBench -- F15: is the headline an artifact of how the predicates were written?

WHY THIS EXISTS
---------------
The frozen predicates are QUESTIONS with the placeholder trailing:
    sem_filter("Is this paper about machine learning? {abstract_text}")
LOTUS renders that into its claim-verification wrapper as
    Claim: Is this paper about machine learning? Abstract_text
i.e. it asks the model whether a QUESTION is TRUE, with the column name left
dangling. LOTUS's own documented examples use STATEMENTS with the placeholder
inline -- `sem_filter("{text} is about sports")`, and for maps
`sem_map("Label the sentiment of Harry in the {document} as
positive/negative/neutral. Answer in one word.")` (lotus/ast/lazyframe.py,
lotus/sem_ops/sem_map.py, lotus 1.2.4). A reviewer can argue the headline is
an artifact of misusing the API. This run rewrites BOTH operators in the
documented form, keeping every criterion and label set word for word, and
re-runs both domains once.

                 frozen (as run)                             documented form (this run)
  abstracts F    Is this paper about machine learning? {x}   {abstract_text} is about machine learning
  abstracts M    Respond with exactly one word. Classify     Classify the primary topic of {abstract_text}
                 the primary topic of this paper as: ML,     as: ML, systems, theory, or other. Answer in
                 systems, theory, or other. Abstract: {x}    one word.
  reviews F      Is this review negative? {x}                {review_text} is a negative review
  reviews M      Respond with exactly one word. Classify     Classify the sentiment of {review_text} as:
                 the sentiment of this review as: positive,  positive, negative, or neutral. Answer in
                 negative, or neutral. Review: {x}           one word.

Unchanged: label sets, the frozen disagreement predicate, model, rows, batching,
truncation. Baseline: rq3_{domain}_rep1.

PRE-REGISTERED DECISION RULES (committed to git before the run -- do not edit)
------------------------------------------------------------------------------
Primary metric: the operational disagreement rate under the frozen predicate,
per domain, compared with the baseline replicate-1 Wilson 95% interval:
  abstracts 36/154 = 23.38% [17.39, 30.66];  reviews 7/271 = 2.58% [1.26, 5.23].

  S1 (robust to prompt form): the documented-form rate falls inside the
     baseline interval in BOTH domains.
  S2 (form-sensitive): it falls outside in either domain. The headline must
     then be reported as conditional on prompt form, with both forms shown.

Reported whatever the outcome, per domain:
  - filter admissions vs baseline: rows admitted only under each form, exact
    McNemar on all 500 rows (noise ceiling across existing abstracts runs: 2);
  - map labels on rows admitted under both forms: count changed;
  - abstracts: F11 split (disagreement by declared category), F10 proxy
    precision/recall, and how many of the 36 persistent baseline
    disagreements still disagree;
  - reviews: filter agreement with the IMDB sentiment labels under each form.

Scope: one run per domain, one model. S1 means "no detectable dependence on
prompt form at this sample size", not "prompt form is irrelevant".

Usage:
    python scripts/run_docform_rerun.py --dry-run
    python scripts/run_docform_rerun.py --batch-delay 12 --patient --yes
    python scripts/run_docform_rerun.py --aggregate-only
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

DOCFORM = {
    "abstracts": {
        "filter": "{abstract_text} is about machine learning",
        "map": ("Classify the primary topic of {abstract_text} as: ML, systems, "
                "theory, or other. Answer in one word."),
    },
    "reviews": {
        "filter": "{review_text} is a negative review",
        "map": ("Classify the sentiment of {review_text} as: positive, negative, "
                "or neutral. Answer in one word."),
    },
}
BASELINE_CI = {"abstracts": (0.1739, 0.3066), "reviews": (0.0126, 0.0523)}
PERSISTENT_36 = [18, 21, 23, 37, 76, 91, 106, 113, 117, 127, 164, 178, 200, 202, 204,
                 220, 221, 227, 228, 240, 277, 284, 301, 305, 330, 347, 357, 367, 372,
                 389, 395, 414, 430, 438, 487, 491]
SUMMARY = ROOT / "results" / "docform_rerun_summary.json"


def run_id(domain: str) -> str:
    return f"docform_{domain}_rep1"


def out_paths(domain: str):
    rid = run_id(domain)
    return ROOT / "results" / f"{rid}_filter.csv", ROOT / "results" / f"{rid}_map.csv"


def base_paths(domain: str):
    return (ROOT / "results" / f"rq3_{domain}_rep1_filter.csv",
            ROOT / "results" / f"rq3_{domain}_rep1_map.csv")


def load_rows(domain: str) -> pd.DataFrame:
    kw = {"escapechar": "\\"} if domain == "abstracts" else {}
    df = pd.read_csv(ROOT / PIPELINES[domain]["data_file"], index_col="row_id", **kw)
    assert len(df) == 500 and list(df.index) == list(range(500)), \
        f"frozen {domain} set changed -- STOP, do not proceed (D-09)"
    return df


def mcnemar_exact(b: int, c: int) -> float:
    """Exact two-sided McNemar p (same method as scripts/f7_precision_paired.py)."""
    n = b + c
    if n == 0:
        return 1.0
    return min(1.0, 2 * sum(math.comb(n, i) for i in range(min(b, c) + 1)) / 2 ** n)


def read_map(path: Path, col: str) -> pd.Series:
    return pd.read_csv(path, escapechar="\\", usecols=["row_id", col], index_col="row_id")[col].astype(str)


def rate(k: int, n: int) -> dict:
    return {"k": k, "n": n, "rate": k / n if n else None,
            "ci": list(wilson_ci(k, n)) if n else None}


def analyse_domain(domain: str) -> dict:
    f_new, m_new = out_paths(domain)
    if not m_new.exists():
        return {"error": f"{run_id(domain)} not on disk"}
    cfg = PIPELINES[domain]
    col, is_contra = cfg["map"]["output_col"], cfg["contradiction"]["is_contradiction"]
    f_base, m_base = base_paths(domain)
    bm, nm = read_map(m_base, col), read_map(m_new, col)
    base_adm, new_adm = set(bm.index), set(nm.index)

    k_new = sum(is_contra(True, v) for v in nm)
    k_base = sum(is_contra(True, v) for v in bm)
    new_rate = k_new / len(nm) if len(nm) else 0.0
    lo, hi = BASELINE_CI[domain]
    both = sorted(base_adm & new_adm)
    out = {
        "frozen_form": {"filter": cfg["filter"]["predicate"], "map": cfg["map"]["instruction"]},
        "documented_form": DOCFORM[domain],
        "disagreement_baseline": rate(k_base, len(bm)),
        "disagreement_documented_form": rate(k_new, len(nm)),
        "inside_baseline_ci": lo <= new_rate <= hi,
        "filter_admissions": {
            "baseline": len(base_adm), "documented_form": len(new_adm),
            "baseline_only": sorted(base_adm - new_adm),
            "documented_only": sorted(new_adm - base_adm),
            "mcnemar_p": round(mcnemar_exact(len(base_adm - new_adm), len(new_adm - base_adm)), 6),
        },
        "map_labels_changed_on_rows_admitted_in_both": [
            {"row_id": r, "baseline": bm[r], "documented_form": nm[r]} for r in both if bm[r] != nm[r]],
        "off_schema_labels_documented_form": int((~nm.isin(cfg["map"]["valid_labels"])).sum()),
    }

    if domain == "abstracts":
        lab = pd.read_csv(ROOT / "data" / "abstracts" / "abstracts_v1_labels_v2.csv",
                          index_col="row_id").topic_label_v2
        labelled = set(lab.index[lab != "unknown"])
        declared_ml = set(lab.index[lab == "machine_learning"])

        def prf(adm):
            a = adm & labelled
            tp, fp, fn = len(a & declared_ml), len(a - declared_ml), len(declared_ml - a)
            return {"tp": tp, "fp": fp, "fn": fn,
                    "precision": tp / (tp + fp) if tp + fp else None,
                    "recall": tp / (tp + fn) if tp + fn else None}

        def split(m):
            res = {}
            for name, val in (("ml_declared", "machine_learning"), ("non_ml_declared", "other")):
                rows = [r for r in m.index if lab[r] == val]
                res[name] = rate(sum(is_contra(True, m[r]) for r in rows), len(rows))
            return res

        out["F10_baseline"], out["F10_documented_form"] = prf(base_adm), prf(new_adm)
        out["F11_baseline"], out["F11_documented_form"] = split(bm), split(nm)
        still = [r for r in PERSISTENT_36 if r in new_adm and is_contra(True, nm[r])]
        out["persistent_36"] = {"still_admitted": sum(r in new_adm for r in PERSISTENT_36),
                                "still_disagree": len(still)}
    else:
        gold = load_rows(domain)["sentiment_label"]
        neg = set(gold.index[gold == "negative"])

        def agree(adm):
            return sum((r in adm) == (r in neg) for r in gold.index) / len(gold)

        out["filter_agreement_with_imdb_label"] = {"baseline": round(agree(base_adm), 4),
                                                   "documented_form": round(agree(new_adm), 4)}
    return out


def verdict(res: dict) -> str:
    oks = [res[d]["inside_baseline_ci"] for d in ("abstracts", "reviews") if "error" not in res[d]]
    if len(oks) < 2:
        return "incomplete -- both domains must be on disk"
    if all(oks):
        return "S1 -- robust to prompt form: documented-form rates inside the baseline intervals in both domains"
    return "S2 -- form-sensitive: report the headline as conditional on prompt form, with both forms"


def report(res: dict) -> None:
    print("\n" + "=" * 70)
    print("F15 -- documented-form rerun vs frozen form (rep1)")
    print("=" * 70)
    for d in ("abstracts", "reviews"):
        r = res[d]
        if "error" in r:
            print(f"  {d}: {r['error']}")
            continue
        b, n, fa = r["disagreement_baseline"], r["disagreement_documented_form"], r["filter_admissions"]
        print(f"  {d:9s} disagreement {b['k']}/{b['n']} = {b['rate']:.2%}  ->  "
              f"{n['k']}/{n['n']} = {n['rate']:.2%}  inside CI: {r['inside_baseline_ci']}")
        print(f"            admitted {fa['baseline']} -> {fa['documented_form']}  "
              f"(-{len(fa['baseline_only'])} / +{len(fa['documented_only'])}, McNemar p={fa['mcnemar_p']})  "
              f"map changes on shared rows: {len(r['map_labels_changed_on_rows_admitted_in_both'])}")
    print(f"\n  => {res['verdict']}")


def main() -> None:
    ap = argparse.ArgumentParser(description="F15 documented-form rerun")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--aggregate-only", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--yes", action="store_true")
    ap.add_argument("--batch-delay", type=float, default=None)
    ap.add_argument("--patient", action="store_true")
    args = ap.parse_args()

    for d in ("abstracts", "reviews"):
        print(f"{d}:\n  filter: {DOCFORM[d]['filter']}\n  map:    {DOCFORM[d]['map']}")

    if args.dry_run:
        for d in ("abstracts", "reviews"):
            for p in base_paths(d):
                print(f"  {'OK     ' if p.exists() else 'MISSING'} {p.relative_to(ROOT)}")
            load_rows(d)
        print("Estimated tokens: ~0.66M abstracts + ~0.25M reviews.")
        print("--dry-run: no API calls made.")
        return

    if not args.aggregate_only:
        todo = [d for d in ("abstracts", "reviews") if args.force or not out_paths(d)[1].exists()]
        if todo:
            if not args.yes and input(f"\nRun {todo} (~0.9M tokens)? [y/N] ").strip().lower() != "y":
                print("Aborted.")
                return
            from harness.runner import PipelineRunner
            from harness.intercept import LLMInterceptor

            interceptor = LLMInterceptor(log_dir=ROOT / "logs" / "llm_calls", tag="docform_rerun")
            interceptor.install()
            runner = PipelineRunner(
                model=BACKENDS["hosted"]["model"], backend="hosted",
                log_dir=ROOT / "logs", cache_path=ROOT / "cache.db", cache_enabled=False,
                batch_delay_seconds=args.batch_delay, patient=args.patient,
            )
            for d in todo:
                cfg = PIPELINES[d]
                runner.run_filter_map(
                    df=load_rows(d), domain=d,
                    filter_predicate=DOCFORM[d]["filter"],
                    map_instruction=DOCFORM[d]["map"],
                    map_output_col=cfg["map"]["output_col"],
                    valid_labels=cfg["map"]["valid_labels"],
                    run_id=run_id(d),
                )
                meta_path = ROOT / "results" / f"{run_id(d)}_meta.json"
                meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
                meta.update({"purpose": "F15: frozen criteria rewritten in LOTUS's documented langex form",
                             "frozen_form": {"filter": cfg["filter"]["predicate"],
                                             "map": cfg["map"]["instruction"]},
                             "documented_form": DOCFORM[d], "baseline_run": f"rq3_{d}_rep1"})
                meta_path.write_text(json.dumps(meta, indent=2, default=str))

    res = {d: analyse_domain(d) for d in ("abstracts", "reviews")}
    res["verdict"] = verdict(res)
    report(res)
    if all("error" not in res[d] for d in ("abstracts", "reviews")):
        SUMMARY.write_text(json.dumps(res, indent=2, default=str))
        print(f"\nWrote {SUMMARY.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
