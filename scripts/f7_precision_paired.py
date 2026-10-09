"""
OperatorBench -- proxy-agreement and precision/recall tradeoff tests for F12.

WHY THIS EXISTS
---------------
F12 reported filter precision rising 73.19% -> 84.52% against author-declared
arXiv categories, and I used that to argue the criterion alignment made the
filter "more correct" rather than merely more agreeable. **Two audit objections
landed, and both are right:**

  (1) The precision confidence intervals OVERLAP -- [65.24, 79.88] vs
      [75.30, 90.73] -- so the improvement is not established by
      non-overlapping intervals and needs a paired test.
  (2) Recall fell 87.83% -> 61.74%. "More correct" was unqualified and wrong;
      it is a precision/recall TRADEOFF.

This script settles it with tests that respect the paired structure. Every row
has a fixed proxy label and two filter decisions, so the comparison is within-row.

THREE QUESTIONS, THREE TESTS
----------------------------
A. Did the intervention change overall agreement with the stated proxy?
   McNemar exact on per-row agreement with proxy labels across all labelled rows.
   Discordant cells only; concordant rows carry no information.

B. Is removal selective relative to the category proxy?
   Fisher exact on the 2x2 [removed, kept] x [proxy-negative, proxy-positive].
   These are disjoint row groups, so Fisher rather than McNemar.

C. What did it cost in absolute terms?
   Proxy-positive rows removed vs proxy-negative rows removed. A ledger, not a test.

THE PROXY IS A PROXY
--------------------
`topic_label_v2` is the authors' own arXiv primary category. It is external and
author-supplied, which is its virtue, but it is a *primary-category declaration*
and the baseline filter was asked an *aboutness* question. It is not a definitive
judgement of what each paper is about. Do not call agreement with it "accuracy"
without that qualifier.

No API calls. Usage:
    python scripts/f7_precision_paired.py
"""

import sys
import json
import math
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "results"
LABELS = ROOT / "data" / "abstracts" / "abstracts_v1_labels_v2.csv"


def wilson(k, n, z=1.96):
    if n == 0:
        return (0.0, 0.0)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n) / d
    return (max(0.0, c - h), min(1.0, c + h))


def mcnemar_exact(b, c):
    n = b + c
    if n == 0:
        return None
    obs = min(b, c)
    return min(1.0, 2 * sum(math.comb(n, i) for i in range(obs + 1)) / 2 ** n)


def fisher_exact(a, b, c, d):
    """Two-sided Fisher on [[a,b],[c,d]] by summing tables no more likely than observed."""
    def lchoose(n, k):
        return math.lgamma(n + 1) - math.lgamma(k + 1) - math.lgamma(n - k + 1)
    n = a + b + c + d
    r1, c1 = a + b, a + c
    def prob(x):
        return math.exp(lchoose(r1, x) + lchoose(n - r1, c1 - x) - lchoose(n, c1))
    obs = prob(a)
    lo, hi = max(0, c1 - (n - r1)), min(r1, c1)
    return min(1.0, sum(prob(x) for x in range(lo, hi + 1) if prob(x) <= obs + 1e-12))


def analyse(rep):
    lab = pd.read_csv(LABELS, index_col="row_id")
    labelled = lab[lab.topic_label_v2 != "unknown"]
    known = set(labelled.index)
    ml = set(labelled[labelled.topic_label_v2 == "machine_learning"].index)

    b_f = RESULTS / f"rq3_abstracts_rep{rep}_map.csv"
    a_f = RESULTS / f"f7_ablation_abstracts_rep{rep}_map.csv"
    if not (b_f.exists() and a_f.exists()):
        return {"replicate": rep, "error": "baseline or ablation map missing"}
    base = set(pd.read_csv(b_f, index_col="row_id").index) & known
    abl = set(pd.read_csv(a_f, index_col="row_id").index) & known
    if not (base & ml) or not (base - ml) or not abl:
        return {"replicate": rep, "error": "insufficient proxy classes or admissions for precision/removal analysis"}

    def prf(adm):
        tp, fp, fn = len(adm & ml), len(adm - ml), len(ml - adm)
        return tp, fp, fn, tp / (tp + fp), tp / (tp + fn)

    btp, bfp, bfn, bp, br = prf(base)
    atp, afp, afn, ap, ar = prf(abl)

    # A. overall agreement with the author-category proxy, paired
    b_ok = {r for r in known if (r in base) == (r in ml)}
    a_ok = {r for r in known if (r in abl) == (r in ml)}
    b_only, c_only = len(b_ok - a_ok), len(a_ok - b_ok)
    p_mc = mcnemar_exact(b_only, c_only)

    # B. selectivity of removal
    dropped = base - abl
    tp_lost, fp_lost = len(dropped & ml), len(dropped - ml)
    p_fisher = fisher_exact(fp_lost, bfp - fp_lost, tp_lost, btp - tp_lost)
    agreement_verdict = (
        "no discordant pairs; no evidence of a change" if p_mc is None else
        "no significant change in overall agreement with the proxy" if p_mc >= 0.05 else
        "overall agreement with the proxy increased" if c_only > b_only else
        "overall agreement with the proxy decreased"
    )
    negative_rate = fp_lost / bfp
    positive_rate = tp_lost / btp
    removal_verdict = (
        "no significant difference in removal rates" if p_fisher >= 0.05 else
        "proxy-negative admits are removed at a higher rate" if negative_rate > positive_rate else
        "proxy-positive admits are removed at a higher rate"
    )

    return {
        "replicate": rep,
        "n_labelled_rows": len(known),
        "baseline": {"tp": btp, "fp": bfp, "fn": bfn,
                     "precision": round(bp, 4), "precision_ci": [round(x, 4) for x in wilson(btp, btp + bfp)],
                     "recall": round(br, 4), "recall_ci": [round(x, 4) for x in wilson(btp, btp + bfn)]},
        "ablated": {"tp": atp, "fp": afp, "fn": afn,
                    "precision": round(ap, 4), "precision_ci": [round(x, 4) for x in wilson(atp, atp + afp)],
                    "recall": round(ar, 4), "recall_ci": [round(x, 4) for x in wilson(atp, atp + afn)]},
        "precision_cis_overlap": not (wilson(atp, atp + afp)[0] > wilson(btp, btp + bfp)[1]
                                      or wilson(btp, btp + bfp)[0] > wilson(atp, atp + afp)[1]),
        "A_proxy_agreement_paired": {
            "test": "McNemar exact, per-row decision vs proxy label",
            "proxy_agreement_baseline": round(len(b_ok) / len(known), 4),
            "proxy_agreement_ablated": round(len(a_ok) / len(known), 4),
            "baseline_only_agreement": b_only,
            "ablated_only_agreement": c_only,
            "p_two_sided": None if p_mc is None else round(p_mc, 5),
            "verdict": agreement_verdict,
        },
        "B_selectivity_of_removal": {
            "test": "Fisher exact, [removed,kept] x [was FP, was TP]",
            "proxy_negative_removal_rate": round(negative_rate, 4),
            "proxy_positive_removal_rate": round(positive_rate, 4),
            "rate_ratio": round(negative_rate / positive_rate, 2)
            if positive_rate else None,
            "p_two_sided": round(p_fisher, 6),
            "verdict": removal_verdict,
        },
        "C_proxy_relative_ledger": {
            "rows_dropped_labelled": len(dropped),
            "proxy_positive_rows_removed": tp_lost,
            "proxy_negative_rows_removed": fp_lost,
            "note": ("Removal counts are relative to the category proxy; "
                     "removed rows have not been adjudicated as errors."),
        },
    }


def main():
    out = [analyse(r) for r in (1, 2)]
    print("=" * 74)
    print("F12 PAIRED ANALYSIS -- proxy agreement and removal selectivity")
    print("=" * 74)
    for d in out:
        if "error" in d:
            print(f"  rep{d['replicate']}: {d['error']}")
            continue
        b, a = d["baseline"], d["ablated"]
        A, B, C = (d["A_proxy_agreement_paired"], d["B_selectivity_of_removal"],
                   d["C_proxy_relative_ledger"])
        print(f"\nreplicate {d['replicate']}  ({d['n_labelled_rows']} labelled rows)")
        print(f"  precision {b['precision']:.2%} [{b['precision_ci'][0]:.2%},{b['precision_ci'][1]:.2%}]"
              f"  ->  {a['precision']:.2%} [{a['precision_ci'][0]:.2%},{a['precision_ci'][1]:.2%}]"
              f"   CIs overlap: {d['precision_cis_overlap']}")
        print(f"  recall    {b['recall']:.2%} [{b['recall_ci'][0]:.2%},{b['recall_ci'][1]:.2%}]"
              f"  ->  {a['recall']:.2%} [{a['recall_ci'][0]:.2%},{a['recall_ci'][1]:.2%}]")
        print(f"  A. overall agreement with proxy {A['proxy_agreement_baseline']:.2%} -> "
              f"{A['proxy_agreement_ablated']:.2%}"
              f"   (b={A['baseline_only_agreement']}, c={A['ablated_only_agreement']},"
              f" McNemar p={A['p_two_sided']})")
        print(f"     => {A['verdict']}")
        print(f"  B. proxy-negative rows removed {B['proxy_negative_removal_rate']:.1%} vs "
              f"proxy-positive rows removed {B['proxy_positive_removal_rate']:.1%}"
              f"  (rate ratio: {B['rate_ratio']})  Fisher p={B['p_two_sided']}")
        print(f"     => {B['verdict']}")
        print(f"  C. removed {C['proxy_positive_rows_removed']} proxy-positive rows to remove"
              f" {C['proxy_negative_rows_removed']} proxy-negative rows")

    dest = RESULTS / "f7_precision_paired.json"
    dest.write_text(json.dumps({
        "purpose": "paired tests behind F12's proxy-agreement and selectivity claims",
        "classification_target": "author-declared arXiv primary-category proxy",
        "headline": "; ".join(
            f"replicate {d['replicate']}: {d['A_proxy_agreement_paired']['verdict']}; "
            f"{d['B_selectivity_of_removal']['verdict']}"
            for d in out if "error" not in d),
        "proxy_caveat": ("topic_label_v2 is the authors' arXiv primary-category "
                         "declaration, not a definitive judgement of aboutness"),
        "replicates": out,
    }, indent=2))
    print(f"\nWrote {dest.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
