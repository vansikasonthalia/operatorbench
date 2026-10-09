"""
OperatorBench -- sem_filter agreement with an author-declared category proxy

WHY THIS MATTERS
----------------
This compares the filter's aboutness decision with each paper's author-declared
arXiv primary category. The category is an external proxy, not ground truth for
the filter's question. The frozen sample was selected using an erroneous legacy
category grouping, so estimates describe this sample and are not population
estimates. This script defaults to frozen replicate 1; use
`scripts/f10_f11_frozen.py` for the canonical three-replicate summary.

WHAT IT REPORTS
---------------
sem_filter was asked "Is this paper about machine learning?" over 500 rows.
Against the rows' author-declared primary categories, among the 449 tagged rows:

  precision -- of the rows it admitted, how many are ML-declared
  recall    -- of the ML-declared rows, how many it admitted

Both restricted to rows carrying an arXiv stamp, with coverage stated.

THE CAVEAT THAT MUST SHIP WITH IT
---------------------------------
The filter was asked an ABOUTNESS question, not a primary-category question.
A cs.SY paper that applies neural networks is arguably "about machine
learning" while declaring cs.SY. So this measures agreement with the authors'
primary-category declaration, NOT filter error, and the write-up must say so
in those words. It is the same asymmetry F7 is about, and it is why this is
reported as precision against a stated proxy rather than as an accuracy claim.

Usage:
    python scripts/filter_precision.py
    python scripts/filter_precision.py --filter-csv results/rq3_abstracts_rep1_filter.csv
"""

import sys
import json
import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from harness.metrics import wilson_ci

LABELS = ROOT / "data" / "abstracts" / "abstracts_v1_labels_v2.csv"
SRC = ROOT / "data" / "abstracts" / "abstracts_v1.csv"


def pct(k, n):
    if n == 0:
        return "n/a"
    lo, hi = wilson_ci(k, n)
    return f"{k}/{n} = {k/n:.2%} [{lo:.2%}, {hi:.2%}]"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--filter-csv",
                    default="results/rq3_abstracts_rep1_filter.csv",
                    help="FROZEN replicate 1 by default (changed 28 Sept 2026). "
                         "The previous default was the PRELIMINARY run, which is "
                         "why earlier F10/F11 figures -- including the 4.7x "
                         "concentration ratio -- do not match the frozen data.")
    ap.add_argument("--out-prefix", default=None,
                    help="defaults to results/filter_precision_<run-name>, derived "
                         "from --filter-csv so a preliminary run can never silently "
                         "overwrite a frozen one's output")
    args = ap.parse_args()

    # Derive the output prefix from the input so the two runs cannot collide.
    if args.out_prefix is None:
        stem = Path(args.filter_csv).stem.replace("_filter", "")
        args.out_prefix = f"results/filter_precision_{stem}"

    preliminary = "preliminary" in args.filter_csv
    print("=" * 74)
    print(f"FILTER PRECISION -- input: {args.filter_csv}")
    if preliminary:
        print("!! THIS IS THE PRELIMINARY RUN, NOT THE FROZEN DATA.")
        print("!! Figures from it are superseded and must not appear in the paper.")
        print("!! For the reportable numbers run: python scripts/f10_f11_frozen.py")
    else:
        print("   Frozen replicate. Note that the per-replicate F10/F11 table the")
        print("   paper quotes is produced by scripts/f10_f11_frozen.py, which")
        print("   reports all three replicates and is the source of truth.")
    print("=" * 74)

    if not LABELS.exists():
        sys.exit("Run scripts/derive_topic_label_v2.py first.")

    lab = pd.read_csv(LABELS, index_col="row_id")
    filt = pd.read_csv(ROOT / args.filter_csv, index_col="row_id")
    admitted = set(filt.index)

    all_rows = pd.read_csv(SRC, escapechar="\\", index_col="row_id")
    assert len(all_rows) == 500 and list(all_rows.index) == list(range(500))

    lab = lab.assign(admitted=[i in admitted for i in lab.index])
    known = lab[lab.topic_label_v2 != "unknown"]

    is_ml = known.topic_label_v2 == "machine_learning"
    adm = known.admitted

    tp = int((adm & is_ml).sum())
    fp = int((adm & ~is_ml).sum())
    fn = int((~adm & is_ml).sum())
    tn = int((~adm & ~is_ml).sum())

    prec_n, rec_n = tp + fp, tp + fn
    precision = tp / prec_n if prec_n else 0.0
    recall = tp / rec_n if rec_n else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0

    untagged_admitted = int((lab.topic_label_v2 == "unknown").loc[
        [i for i in lab.index if i in admitted]].sum())

    # Where do operational disagreements concentrate? Needs the matching map CSV, which
    # carries the sem_map label for each admitted row.
    map_csv_path = ROOT / args.filter_csv.replace("_filter.csv", "_map.csv")
    c_ml = c_nml = n_ml = n_nml = 0
    if map_csv_path.exists():
        from configs.pipelines import PIPELINES
        is_contra = PIPELINES["abstracts"]["contradiction"]["is_contradiction"]
        mp = pd.read_csv(map_csv_path, index_col="row_id")
        contra = {rid for rid, r in mp.iterrows()
                  if is_contra(True, str(r[PIPELINES["abstracts"]["map"]["output_col"]]))}
        adm_known = known[known.admitted]
        adm_ml = adm_known.topic_label_v2 == "machine_learning"
        hit = pd.Series({i: (i in contra) for i in adm_known.index})
        n_ml, n_nml = int(adm_ml.sum()), int((~adm_ml).sum())
        c_ml, c_nml = int((hit & adm_ml).sum()), int((hit & ~adm_ml).sum())

    lines = []
    w = lines.append
    w("# sem_filter agreement with author-declared arXiv category proxy")
    w("")
    w(f"Filter run: `{args.filter_csv}`")
    w(f"Labels: `{LABELS.relative_to(ROOT)}` (topic_label_v2, F9-corrected)")
    w("")
    w("Filter predicate: *Is this paper about machine learning?*")
    w("")
    w("## Coverage")
    w("")
    w(f"- Rows in frame: **500**")
    w(f"- Admitted by sem_filter: **{len(admitted)}**")
    w(f"- Carrying an arXiv stamp (usable): **{len(known)}** "
      f"({len(known)/len(lab):.1%})")
    w(f"- Admitted but untagged, excluded from rates: **{untagged_admitted}**")
    w("")
    w("## Confusion matrix (tagged rows only)")
    w("")
    w("| | admitted | rejected | total |")
    w("|---|---|---|---|")
    w(f"| ML-declared | {tp} | {fn} | {tp+fn} |")
    w(f"| not ML-declared | {fp} | {tn} | {fp+tn} |")
    w(f"| **total** | {tp+fp} | {fn+tn} | {len(known)} |")
    w("")
    w("## Rates, with Wilson 95% CIs")
    w("")
    w(f"- **Proxy-relative precision** — of rows admitted, share ML-declared: {pct(tp, prec_n)}")
    w(f"- **Non-ML-declared admission rate**: {pct(fp, prec_n)}")
    w(f"- **Proxy-relative recall** — of ML-declared rows, share admitted: {pct(tp, rec_n)}")
    w(f"- **F1**: {f1:.3f}")
    w("")
    w("## What the filter admitted that the authors did not call ML")
    w("")
    w("| arXiv category | admitted |")
    w("|---|---|")
    for cat, n in (known[adm & ~is_ml]["arxiv_category"]
                   .value_counts().head(15).items()):
        w(f"| {cat} | {n} |")
    w("")
    w("## ML-declared rows the filter rejected")
    w("")
    w("| arXiv category | rejected |")
    w("|---|---|")
    for cat, n in (known[~adm & is_ml]["arxiv_category"]
                   .value_counts().head(15).items()):
        w(f"| {cat} | {n} |")
    w("")
    w("## Where the operational disagreements sit (F11 proxy analysis)")
    w("")
    if map_csv_path.exists():
        w("Splitting the admitted rows by whether the authors declared ML, and")
        w("asking how often `sem_map` then met the frozen disagreement rule:")
        w("")
        w("| admitted rows | n | disagreements | rate | 95% CI |")
        w("|---|---|---|---|---|")
        for name, k, n in (("ML-declared", c_ml, n_ml),
                           ("not ML-declared", c_nml, n_nml)):
            lo, hi = wilson_ci(k, n) if n else (0, 0)
            w(f"| {name} | {n} | {k} | {k/n:.1%} | [{lo:.1%}, {hi:.1%}] |")
        w("")
        w(f"The rate is **{(c_nml/n_nml)/(c_ml/n_ml):.1f}x higher** on rows the")
        w("filter admitted against the authors' own declaration, and the")
        w("confidence intervals do not overlap.")
        w("")
        w("This pattern is consistent with the F7 finding that disagreements")
        w("concentrate on filter-admitted non-ML rows, but it is proxy-based")
        w("evidence, not human adjudication or proof of the prompt mechanism")
        w("(the prompt mechanism itself is untested). Disagreements concentrate on")
        w("rows without an author-declared ML category; the categories and the")
        w("filter's aboutness criterion are not identical constructs.")
        w("")
        w("In this frozen sample, the filter admitted")
        w(f"**{recall:.1%}** of ML-declared rows and also admitted {fp}")
        w("rows without an ML primary-category declaration. These are proxy")
        w("associations in the selected sample; they do not establish that")
        w("the filter is generally inclusive or explain a population-level")
        w("share of disagreements. F12 separately reports the scoped")
        w("predicate intervention.")
    else:
        w(f"_Map CSV not found at {map_csv_path}; skipped._")
    w("")
    w("## How to state this in the paper")
    w("")
    w("The filter was asked an **aboutness** question; the arXiv category is a")
    w("**primary-category** declaration. A control-theory paper that applies")
    w("neural networks can honestly be 'about machine learning' and still")
    w("declare cs.SY. So this is agreement with a stated external proxy, not")
    w("filter error, and it must be written in those words. It is related to")
    w("the criteria tested by F12, but this proxy analysis alone does not")
    w("establish that the criterion difference causes disagreements.")
    w("")
    w("Author-declared categories are external to the filter output and do")
    w("not use the retired `topic_label` field. They remain a proxy for a")
    w("different construct, and the sample's original selection used an")
    w("erroneous category grouping. Repeatability and proxy agreement are")
    w("separate observations: the RQ1 flip rate of 0.20% says outputs rarely")
    w("changed across five runs; it does not establish answer correctness.")

    md = ROOT / f"{args.out_prefix}.md"
    md.write_text("\n".join(lines), encoding="utf-8")

    payload = {
        "filter_csv": args.filter_csv,
        "estimand": "agreement between sem_filter admission and author-declared arXiv primary-category proxy",
        "selection_caveat": ("The frozen 500-row sample was selected using an erroneous "
                             "legacy label grouping; these figures describe this selected "
                             "sample, not the arXiv population."),
        "interpretation": ("precision, recall, false-admission rate, and F1 are relative "
                           "to the author-category proxy, not adjudicated semantic accuracy."),
        "n_frame": 500,
        "n_admitted": len(admitted),
        "n_tagged": int(len(known)),
        "coverage": len(known) / len(lab),
        "n_admitted_untagged_excluded": untagged_admitted,
        "confusion": {"tp": tp, "fp": fp, "fn": fn, "tn": tn},
        "precision": precision,
        "precision_ci": list(wilson_ci(tp, prec_n)) if prec_n else None,
        "false_admission_rate": fp / prec_n if prec_n else None,
        "recall": recall,
        "recall_ci": list(wilson_ci(tp, rec_n)) if rec_n else None,
        "f1": f1,
        "caveat": "aboutness question vs primary-category declaration; "
                  "agreement with a stated proxy, not filter error",
    }
    js = ROOT / f"{args.out_prefix}.json"
    js.write_text(json.dumps(payload, indent=2))

    print("\n".join(lines[:40]))
    print(f"\nWrote {md.relative_to(ROOT)} and {js.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
