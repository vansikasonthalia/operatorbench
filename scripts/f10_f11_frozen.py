"""
OperatorBench -- F10 and F11 recomputed on the FROZEN replicates, per replicate.

WHY THIS EXISTS
---------------
F10 (filter precision against the author-declared category proxy) and F11
(operational disagreements concentrate on over-admitted rows) were first computed on
`rq3_preliminary_abstracts_filter.csv` -- the preliminary run, which is not the
frozen data. `filter_precision.py` now defaults to frozen replicate 1 for a
single-replicate diagnostic. This script aggregates all three frozen replicates
and is the canonical source. An external audit recomputed both from the frozen
replicates and the concentration ratio came out
at **4.20-4.38x**, not the 4.7x the preliminary run gave. That is the number that
belongs in the paper, and hand-typing it into prose is how it goes stale again.

So this script is the source of truth for both findings. Run it; quote its output.

WHAT IT REPORTS, PER FROZEN REPLICATE
-------------------------------------
- Precision and recall of the filter against `topic_label_v2`.
- Operational disagreement rate among ML-declared admits vs non-ML-declared admits, with
  Wilson intervals on each, and their ratio (F11's concentration).
- The spread across replicates, so nothing is pooled.

THE PROXY IS A PROXY -- say this wherever these numbers appear
--------------------------------------------------------------
`topic_label_v2` is the authors' declared arXiv **primary category**. The filter
was asked an *aboutness* question. Those are different questions, so what is
measured is AGREEMENT WITH A STATED DECLARATION, not filter accuracy. 51 of 500
rows carry no declared category and are excluded from every figure here; that
exclusion is reported rather than hidden.

No API calls. Usage:
    python scripts/f10_f11_frozen.py
"""

import glob
import json
import math
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from configs.pipelines import PIPELINES
from harness.labels import (labelled_row_ids, load_abstract_labels,
                            ml_row_ids, label_provenance)

RESULTS = ROOT / "results"
DOMAIN = "abstracts"


def wilson(k, n, z=1.96):
    if n == 0:
        return (0.0, 0.0)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n) / d
    return (max(0.0, c - h) * 100, min(1.0, c + h) * 100)


def analyse():
    cfg = PIPELINES[DOMAIN]
    col = cfg["map"]["output_col"]
    is_contra = cfg["contradiction"]["is_contradiction"]
    labels, prov = load_abstract_labels()
    known, ml = labelled_row_ids(labels), ml_row_ids(labels)

    rows = []
    for path in sorted(glob.glob(str(RESULTS / f"rq3_{DOMAIN}_rep*_map.csv"))):
        rep = int(re.search(r"_rep(\d+)_", path).group(1))
        m = pd.read_csv(path, index_col="row_id")
        admitted = set(m.index)
        admitted_known = admitted & known

        tp = len(admitted_known & ml)          # ML-declared and admitted
        fp = len(admitted_known - ml)          # not ML-declared but admitted
        fn = len(ml - admitted)                # ML-declared and rejected

        contradicted = {i for i in admitted if is_contra(True, str(m.loc[i, col]))}
        c_ml = len(contradicted & admitted_known & ml)
        c_non = len((contradicted & admitted_known) - ml)

        rate_ml = c_ml / tp if tp else 0.0
        rate_non = c_non / fp if fp else 0.0

        rows.append({
            "replicate": rep,
            "admitted": len(admitted),
            "admitted_with_declared_category": len(admitted_known),
            "F10_precision": round(tp / (tp + fp) * 100, 2),
            "F10_precision_wilson": [round(x, 2) for x in wilson(tp, tp + fp)],
            "F10_recall": round(tp / (tp + fn) * 100, 2),
            "F10_recall_wilson": [round(x, 2) for x in wilson(tp, tp + fn)],
            "ml_declared_admits": tp,
            "contradictions_among_ml_declared": c_ml,
            "rate_ml_declared": round(rate_ml * 100, 2),
            "wilson_ml_declared": [round(x, 2) for x in wilson(c_ml, tp)],
            "non_ml_declared_admits": fp,
            "contradictions_among_non_ml_declared": c_non,
            "rate_non_ml_declared": round(rate_non * 100, 2),
            "wilson_non_ml_declared": [round(x, 2) for x in wilson(c_non, fp)],
            "F11_concentration_ratio": round(rate_non / rate_ml, 2) if rate_ml else None,
            "intervals_disjoint": wilson(c_non, fp)[0] > wilson(c_ml, tp)[1],
        })

    ratios = [r["F11_concentration_ratio"] for r in rows if r["F11_concentration_ratio"]]
    precs = [r["F10_precision"] for r in rows]
    return {
        "purpose": "F10/F11 on the frozen replicates, per replicate, never pooled",
        "metric_name": "operational criterion disagreement under the frozen rule",
        "legacy_field_names_note": ("contradictions_among_* keys retain the "
                                     "historical artifact name for the operational "
                                     "criterion-disagreement counts"),
        "supersedes": ("figures computed on rq3_preliminary_abstracts_filter.csv, "
                       "including the 4.7x concentration ratio"),
        "proxy_caveat": prov["caveat"],
        "rows_without_declared_category_excluded": len(labels) - len(known),
        "label_provenance": prov,
        "per_replicate": rows,
        "F10_precision_spread_pp": round(max(precs) - min(precs), 2),
        "F11_ratio_range": [min(ratios), max(ratios)] if ratios else None,
        "F11_holds_in_every_replicate": all(r["intervals_disjoint"] for r in rows),
    }


def main():
    d = analyse()
    print("=" * 78)
    print("F10 / F11 ON THE FROZEN REPLICATES  (supersedes the preliminary run)")
    print("=" * 78)
    print(f"  proxy: author-declared arXiv primary category "
          f"({d['rows_without_declared_category_excluded']} rows without one, excluded)")
    print()
    print(f"  {'rep':>3} {'precision':>10} {'recall':>9} | "
          f"{'ML-decl':>18} | {'non-ML-decl':>18} | {'ratio':>6}")
    for r in d["per_replicate"]:
        print(f"  {r['replicate']:>3} {r['F10_precision']:9.2f}% {r['F10_recall']:8.2f}% | "
              f"{r['contradictions_among_ml_declared']:>3}/{r['ml_declared_admits']:<3}"
              f" = {r['rate_ml_declared']:5.2f}%   | "
              f"{r['contradictions_among_non_ml_declared']:>3}/{r['non_ml_declared_admits']:<3}"
              f" = {r['rate_non_ml_declared']:5.2f}%   | "
              f"{r['F11_concentration_ratio']:5.2f}x")
        print(f"      {'':>10} {'':>9} | "
              f"[{r['wilson_ml_declared'][0]:.1f}, {r['wilson_ml_declared'][1]:.1f}]"
              f"{'':>6} | "
              f"[{r['wilson_non_ml_declared'][0]:.1f}, {r['wilson_non_ml_declared'][1]:.1f}]"
              f"{'':>4} | disjoint={r['intervals_disjoint']}")
    print(f"\n  F10 precision spread : {d['F10_precision_spread_pp']} pp")
    print(f"  F11 ratio range      : {d['F11_ratio_range'][0]}x - {d['F11_ratio_range'][1]}x"
          f"   (the preliminary run gave 4.7x -- do not quote that)")
    print(f"  F11 intervals disjoint in every replicate: {d['F11_holds_in_every_replicate']}")
    dest = RESULTS / "f10_f11_frozen.json"
    dest.write_text(json.dumps(d, indent=2))
    print(f"\n  wrote {dest.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
