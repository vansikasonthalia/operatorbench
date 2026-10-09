"""
OperatorBench -- RQ4 re-analysed as a PAIRED experiment.

WHY THIS SCRIPT EXISTS
----------------------
`run_rq4.py::analyse()` reports the operational disagreement rate on the baseline's
admitted set against the rate on the injected run's admitted set. Those are
DIFFERENT ROW SETS. A difference between them mixes two things that should be
separated:

  (a) injection changed WHICH ROWS the filter admits  -- a denominator change
  (b) injection changed OPERATOR BEHAVIOUR on rows admitted either way

The headline "change in disagreement rate" is meant to be (b). As computed it
is (a) and (b) together, so a reviewer can reasonably say the number does not
measure what the docstring claims.

WHAT THIS DOES INSTEAD
----------------------
Injection is a within-row manipulation: the same abstract, two prompt
conditions. So the estimand is paired, and the analysis should be too.

1. **Paired set** -- rows admitted in BOTH conditions. On these, the
   denominator is fixed by construction, so any change is behaviour.
2. **McNemar** -- the correct test for a binary outcome on paired units.
   Only DISCORDANT pairs carry information:
       b = meets the disagreement rule under baseline, not under injection
       c = meets the disagreement rule under injection, not under baseline
   Concordant pairs are uninformative and must not inflate n. Exact two-sided
   binomial p on b+c trials at p=0.5; no normal approximation, since b+c is small.
3. **Admission change reported SEPARATELY** -- rows the injection added or
   dropped from the filter, as its own quantity, never folded into (b).

A null here is consistent with robustness to this perturbation, but with only
three discordant pairs per replicate it has little power to detect a change.
The paired McNemar test was a post-hoc reanalysis (the original specification
described measuring output flips, not a paired test). The point of this
re-analysis is to give the null a precise estimand: "no detectable behaviour
change on the rows we can compare" rather than "two rates looked similar",
but it is not evidence of equivalence (that would require a prespecified margin
and enough informative pairs).

No API calls. Reads the existing RQ4 and RQ3 replicate CSVs.

Usage:
    python scripts/rq4_paired.py
"""

import sys
import json
import math
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from configs.pipelines import PIPELINES

RESULTS = ROOT / "results"
DOMAIN = "abstracts"
INJECTION_CSV = ROOT / "data" / "abstracts" / "abstracts_v1_injection.csv"


def wilson_ci(k, n, z=1.96):
    if n == 0:
        return (0.0, 0.0)
    p = k / n
    d = 1 + z**2 / n
    c = (p + z**2 / (2 * n)) / d
    h = z * math.sqrt((p * (1 - p) + z**2 / (4 * n)) / n) / d
    return (max(0.0, c - h), min(1.0, c + h))


def binom_exact_two_sided(b, c):
    """Exact McNemar. P(|X - n/2| >= |b - n/2|) for X ~ Bin(n, 0.5), n = b+c."""
    n = b + c
    if n == 0:
        return None
    obs = min(b, c)
    tail = sum(math.comb(n, i) for i in range(0, obs + 1)) / (2 ** n)
    return min(1.0, 2 * tail)


def eligible_rows():
    inj = pd.read_csv(INJECTION_CSV, index_col="row_id")
    return set(inj[inj["has_submission_month"]].index)


def analyse(rep, elig, col, is_contra):
    b_map = RESULTS / f"rq3_{DOMAIN}_rep{rep}_map.csv"
    i_map = RESULTS / f"rq4_{DOMAIN}_rep{rep}_map.csv"
    if not (b_map.exists() and i_map.exists()):
        return {"replicate": rep, "error": "baseline or injected map missing"}

    bm = pd.read_csv(b_map, index_col="row_id")
    im = pd.read_csv(i_map, index_col="row_id")
    b_adm = set(bm.index) & elig
    i_adm = set(im.index) & elig

    paired = sorted(b_adm & i_adm)
    added = sorted(i_adm - b_adm)
    dropped = sorted(b_adm - i_adm)

    bc = {r for r in paired if is_contra(True, str(bm.loc[r, col]))}
    ic = {r for r in paired if is_contra(True, str(im.loc[r, col]))}

    b_only = sorted(bc - ic)          # McNemar b
    c_only = sorted(ic - bc)          # McNemar c
    both = len(bc & ic)
    neither = len(paired) - len(bc | ic)

    n = len(paired)
    rb, ri = len(bc) / n, len(ic) / n
    p = binom_exact_two_sided(len(b_only), len(c_only))

    # label flips on the paired set, regardless of contradiction status
    flips = [r for r in paired
             if str(bm.loc[r, col]).strip().lower() != str(im.loc[r, col]).strip().lower()]

    return {
        "replicate": rep,
        "eligible_rows": len(elig),
        "admitted_baseline": len(b_adm),
        "admitted_injected": len(i_adm),
        "PAIRED_set_size": n,
        "admission_change": {
            "added_by_injection": len(added),
            "dropped_by_injection": len(dropped),
            "net": len(i_adm) - len(b_adm),
            "added_rows": added, "dropped_rows": dropped,
        },
        "paired_contradiction": {
            "baseline_k": len(bc), "baseline_rate": round(rb, 4),
            "injected_k": len(ic), "injected_rate": round(ri, 4),
            "delta_pp": round((ri - rb) * 100, 2),
        },
        "mcnemar": {
            "b_baseline_only": len(b_only), "b_rows": b_only,
            "c_injected_only": len(c_only), "c_rows": c_only,
            "concordant_both": both, "concordant_neither": neither,
            "discordant_total": len(b_only) + len(c_only),
            "exact_two_sided_p": None if p is None else round(p, 4),
        },
        "paired_label_flips": {
            "k": len(flips), "n": n, "rate": round(len(flips) / n, 4),
            "wilson": [round(x, 4) for x in wilson_ci(len(flips), n)],
            "rows": flips,
        },
    }


def main():
    cfg = PIPELINES[DOMAIN]
    col = cfg["map"]["output_col"]
    is_contra = cfg["contradiction"]["is_contradiction"]
    elig = eligible_rows()

    print("=" * 72)
    print("RQ4 RE-ANALYSED AS PAIRED -- abstracts")
    print("=" * 72)
    print("The unpaired rate comparison in rq4_summary.json mixes a denominator")
    print("change with a behaviour change. Below they are separated.\n")

    out = [analyse(r, elig, col, is_contra) for r in (1, 2)]
    for d in out:
        if "error" in d:
            print(f"  rep{d['replicate']}: {d['error']}")
            continue
        ac, pc, mc, pf = (d["admission_change"], d["paired_contradiction"],
                          d["mcnemar"], d["paired_label_flips"])
        print(f"replicate {d['replicate']}")
        print(f"  admitted {d['admitted_baseline']} -> {d['admitted_injected']}"
              f"   (+{ac['added_by_injection']} / -{ac['dropped_by_injection']},"
              f" net {ac['net']:+d})   <- (a) ADMISSION change, reported separately")
        print(f"  PAIRED set: {d['PAIRED_set_size']} rows admitted in both conditions")
        print(f"  contradiction on the paired set: {pc['baseline_k']}/{d['PAIRED_set_size']}"
              f" = {pc['baseline_rate']:.2%}  ->  {pc['injected_k']}/{d['PAIRED_set_size']}"
              f" = {pc['injected_rate']:.2%}   (delta {pc['delta_pp']:+.2f} pp)")
        print(f"  McNemar: b={mc['b_baseline_only']} c={mc['c_injected_only']}"
              f"  discordant={mc['discordant_total']}"
              f"  concordant both={mc['concordant_both']} neither={mc['concordant_neither']}")
        print(f"           exact two-sided p = {mc['exact_two_sided_p']}")
        if mc["b_rows"]: print(f"           baseline-only rows: {mc['b_rows']}")
        if mc["c_rows"]: print(f"           injected-only rows: {mc['c_rows']}")
        print(f"  paired label flips (any change): {pf['k']}/{pf['n']} = {pf['rate']:.2%}"
              f"  [{pf['wilson'][0]:.2%}, {pf['wilson'][1]:.2%}]\n")

    dest = RESULTS / "rq4_paired.json"
    dest.write_text(json.dumps({
        "purpose": "RQ4 re-analysed as a paired within-row experiment",
        "supersedes": ("the unpaired baseline-vs-injected rate comparison in "
                       "rq4_summary.json, which mixes denominator change with "
                       "behaviour change"),
        "estimand": ("contradiction status change on rows admitted under BOTH "
                     "conditions; McNemar exact on discordant pairs"),
        "admission_change_reported_separately": True,
        "replicates": out,
    }, indent=2))
    print(f"Wrote {dest.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
