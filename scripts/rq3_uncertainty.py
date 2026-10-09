"""
OperatorBench -- RQ3 uncertainty, recomputed correctly.

WHY THIS SCRIPT EXISTS
----------------------
The earlier findings narrative applied a Wilson interval to pooled counts
across replicates. (`rq3_summary.json` correctly marks pooled totals as
descriptive; the error was in the prose, not the script output.) **That
interval is wrong and must not be reported.** Wilson assumes n independent
Bernoulli trials. The replicates re-run the SAME frozen rows, so pooled n
counts each distinct row ~once per replicate. On abstracts, n=464 is 156 distinct rows seen ~2.97 times
each, and F3 shows the repeats are repeated and dependent (36 of 36 contradicting rows contradict in every replicate).

Effect measured: the pooled interval came out 5.55 pp NARROWER than a single
replicate's, purely from counting the same rows three times. Replication cannot
shrink row-sampling uncertainty on a frozen dataset. Only a larger dataset can.

This script computes the three quantities that ARE defensible, separately,
and never merges them. No API calls; reads only the replicate CSVs.

THE THREE QUANTITIES
--------------------
1. **Run-to-run uncertainty** -- per-replicate rates and their spread. This is
   what replication actually buys you. Denominator: replicates.
2. **Row-sampling uncertainty** -- Wilson over DISTINCT rows, not row
   observations. Denominator: rows admitted in every replicate. This is the
   uncertainty that would shrink if the frozen dataset were larger.
3. **Cluster bootstrap** -- resample distinct rows with replacement, carrying
   all of a row's replicate observations together, and take percentiles. This
   is the general method that respects the repeated-measures structure, and is
   what to quote if a reviewer challenges (2).

The pooled rate itself stays as a DESCRIPTIVE summary. It is a fine point
estimate. It just has no independent-sample interval.

Usage:
    python scripts/rq3_uncertainty.py
    python scripts/rq3_uncertainty.py --bootstrap 20000 --seed 20260928
"""

import sys
import json
import math
import random
import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from configs.pipelines import PIPELINES

RESULTS = ROOT / "results"


def wilson_ci(k, n, z=1.96):
    if n == 0:
        return (0.0, 0.0)
    p = k / n
    denom = 1 + z**2 / n
    centre = (p + z**2 / (2 * n)) / denom
    half = z * math.sqrt((p * (1 - p) + z**2 / (4 * n)) / n) / denom
    return (max(0.0, centre - half), min(1.0, centre + half))


def load_replicates(domain, max_reps=8):
    """Return {rep: (admitted_set, contradicted_set)} for every replicate on disk."""
    cfg = PIPELINES[domain]
    col = cfg["map"]["output_col"]
    is_contra = cfg["contradiction"]["is_contradiction"]   # frozen, imported
    valid = {v.lower() for v in cfg["map"]["valid_labels"]}
    out = {}
    for r in range(1, max_reps + 1):
        f = RESULTS / f"rq3_{domain}_rep{r}_map.csv"
        if not f.exists():
            continue
        m = pd.read_csv(f, index_col="row_id")
        adm = set(m.index)
        con = {i for i in adm if is_contra(True, str(m.loc[i, col]))}
        vio = {i for i in adm if str(m.loc[i, col]).strip().lower() not in valid}
        out[r] = (adm, con, vio)
    return out


def cluster_bootstrap(reps, B=20000, seed=20260928):
    """
    Resample DISTINCT ROWS with replacement; each sampled row brings all of its
    replicate observations. Returns percentile CI on the pooled rate.

    This is the estimand `run_rq3.py` thinks it is computing. The difference
    from Wilson-on-pooled-counts is entirely the clustering.
    """
    rng = random.Random(seed)
    rows = sorted(set().union(*[a for a, _, _ in reps.values()]))
    # per row: (times admitted, times contradicted) across replicates
    per_row = {
        i: (sum(1 for a, _, _ in reps.values() if i in a),
            sum(1 for _, c, _ in reps.values() if i in c))
        for i in rows
    }
    n_rows = len(rows)
    rates = []
    for _ in range(B):
        k = d = 0
        for _ in range(n_rows):
            a, c = per_row[rows[rng.randrange(n_rows)]]
            d += a
            k += c
        if d:
            rates.append(k / d)
    rates.sort()
    lo = rates[int(0.025 * len(rates))]
    hi = rates[int(0.975 * len(rates)) - 1]
    return lo, hi, n_rows


def analyse(domain, B, seed):
    reps = load_replicates(domain)
    if len(reps) < 2:
        return {"domain": domain, "error": "fewer than 2 replicates on disk"}

    adm_sets = [a for a, _, _ in reps.values()]
    con_sets = [c for _, c, _ in reps.values()]
    vio_sets = [v for _, _, v in reps.values()]
    union_adm = set().union(*adm_sets)
    inter_adm = set.intersection(*adm_sets)
    union_con = set().union(*con_sets)
    inter_con = set.intersection(*con_sets)

    pooled_k = sum(len(c) for c in con_sets)
    pooled_n = sum(len(a) for a in adm_sets)
    per_rep = {r: len(c) / len(a) for r, (a, c, _) in reps.items()}
    rates = list(per_rep.values())

    # (2) row-sampling: over rows admitted in EVERY replicate
    always_k = len(inter_adm & inter_con)
    always_n = len(inter_adm)
    ever_k = len(inter_adm & union_con)
    w_always = wilson_ci(always_k, always_n)
    w_ever = wilson_ci(ever_k, always_n)

    # (3) cluster bootstrap
    b_lo, b_hi, n_rows = cluster_bootstrap(reps, B=B, seed=seed)

    # what the WRONG interval was, for the record
    w_wrong = wilson_ci(pooled_k, pooled_n)

    return {
        "domain": domain,
        "n_replicates": len(reps),
        "pooled_rate_DESCRIPTIVE_ONLY": round(pooled_k / pooled_n, 4),
        "pooled_k": pooled_k,
        "pooled_n_row_OBSERVATIONS": pooled_n,
        "distinct_rows_union": len(union_adm),
        "distinct_rows_admitted_every_replicate": always_n,
        "observations_per_distinct_row": round(pooled_n / len(union_adm), 2),
        "INVALID_pooled_wilson": [round(x, 4) for x in w_wrong],
        "run_to_run": {
            "per_replicate_rates": {r: round(v, 4) for r, v in per_rep.items()},
            "mean": round(sum(rates) / len(rates), 4),
            "spread_pp": round((max(rates) - min(rates)) * 100, 2),
        },
        "row_sampling": {
            "estimand": "persistent/ever disagreement among rows admitted in every replicate; distinct from the pooled admission-weighted rate",
            "scope": "conditional on this selected frozen sample; not population representativeness",
            "always_contradicts": {
                "k": always_k, "n": always_n,
                "rate": round(always_k / always_n, 4) if always_n else None,
                "wilson": [round(x, 4) for x in w_always],
                "width_pp": round((w_always[1] - w_always[0]) * 100, 2)},
            "ever_contradicts": {
                "k": ever_k, "n": always_n,
                "rate": round(ever_k / always_n, 4) if always_n else None,
                "wilson": [round(x, 4) for x in w_ever]},
        },
        "schema_violations": {
            "note": ("SAME independence defect as the contradiction rate: pooled "
                     "counts repeat rows. Row-level figures below are the "
                     "reportable ones."),
            "pooled_k_DESCRIPTIVE": sum(len(v) for v in vio_sets),
            "pooled_n_row_OBSERVATIONS": pooled_n,
            "per_replicate_counts": {r: len(v) for r, (_, _, v) in reps.items()},
            "row_level": {
                "n_rows_admitted_every_replicate": always_n,
                "ever_violates": {
                    "k": len(inter_adm & set().union(*vio_sets)),
                    "wilson": [round(x, 4) for x in wilson_ci(
                        len(inter_adm & set().union(*vio_sets)), always_n)]},
                "always_violates": {
                    "k": len(inter_adm & set.intersection(*vio_sets)),
                    "wilson": [round(x, 4) for x in wilson_ci(
                        len(inter_adm & set.intersection(*vio_sets)), always_n)]},
            },
        },
        "cluster_bootstrap": {
            "estimand": "pooled admission-weighted disagreement rate, resampling rows admitted at least once",
            "scope": "row-resampling sensitivity conditional on this selected sample and observed replicates",
            "B": B, "seed": seed, "resampled_rows": n_rows,
            "ci": [round(b_lo, 4), round(b_hi, 4)],
            "width_pp": round((b_hi - b_lo) * 100, 2)},
    }


def report(res):
    if "error" in res:
        print(f"  {res['domain']}: {res['error']}")
        return
    d = res
    print(f"\n{'='*72}\n{d['domain'].upper()}  ({d['n_replicates']} replicates)\n{'='*72}")
    print(f"  pooled point estimate      {d['pooled_rate_DESCRIPTIVE_ONLY']:.2%}"
          f"  ({d['pooled_k']}/{d['pooled_n_row_OBSERVATIONS']} row-OBSERVATIONS)")
    print(f"  distinct rows behind that  {d['distinct_rows_union']}"
          f"  -> {d['observations_per_distinct_row']} observations per row. NOT independent.")
    w = d["INVALID_pooled_wilson"]
    print(f"  X pooled Wilson (INVALID)  [{w[0]:.2%}, {w[1]:.2%}]"
          f"   width {(w[1]-w[0])*100:.2f} pp   <- do not report")

    rr = d["run_to_run"]
    print(f"\n  1. RUN-TO-RUN (what replication buys)")
    print(f"     rates {[f'{v:.2%}' for v in rr['per_replicate_rates'].values()]}"
          f"  mean {rr['mean']:.2%}  spread {rr['spread_pp']:.2f} pp")

    rs = d["row_sampling"]["always_contradicts"]
    ev = d["row_sampling"]["ever_contradicts"]
    print(f"\n  2. ROW-SAMPLING (Wilson over DISTINCT rows admitted every replicate)")
    print(f"     always contradicts {rs['k']}/{rs['n']} = {rs['rate']:.2%}"
          f"  [{rs['wilson'][0]:.2%}, {rs['wilson'][1]:.2%}]  width {rs['width_pp']:.2f} pp")
    print(f"     ever   contradicts {ev['k']}/{ev['n']} = {ev['rate']:.2%}"
          f"  [{ev['wilson'][0]:.2%}, {ev['wilson'][1]:.2%}]")

    sv = d["schema_violations"]; rl = sv["row_level"]
    ev2, al2 = rl["ever_violates"], rl["always_violates"]
    print(f"\n  SCHEMA VIOLATIONS (same defect; row-level is the reportable one)")
    print(f"     pooled {sv['pooled_k_DESCRIPTIVE']}/{sv['pooled_n_row_OBSERVATIONS']} obs"
          f"  per-replicate {list(sv['per_replicate_counts'].values())}  <- descriptive")
    print(f"     ever  violates {ev2['k']}/{rl['n_rows_admitted_every_replicate']}"
          f"  [{ev2['wilson'][0]:.2%}, {ev2['wilson'][1]:.2%}]")
    print(f"     always violates {al2['k']}/{rl['n_rows_admitted_every_replicate']}"
          f"  [{al2['wilson'][0]:.2%}, {al2['wilson'][1]:.2%}]")

    cb = d["cluster_bootstrap"]
    print(f"\n  3. CLUSTER BOOTSTRAP ({cb['B']} resamples of {cb['resampled_rows']} rows)")
    print(f"     [{cb['ci'][0]:.2%}, {cb['ci'][1]:.2%}]  width {cb['width_pp']:.2f} pp")
    print(f"\n     bootstrap is {cb['width_pp'] - (w[1]-w[0])*100:+.2f} pp vs the invalid pooled interval")


def main():
    ap = argparse.ArgumentParser(description="RQ3 uncertainty, done correctly")
    ap.add_argument("--bootstrap", type=int, default=20000,
                    help="bootstrap resamples (default matches the frozen analysis)")
    ap.add_argument("--seed", type=int, default=20260928)
    args = ap.parse_args()

    print("=" * 72)
    print("RQ3 UNCERTAINTY -- recomputed without the independence error")
    print("=" * 72)
    print("Why this script exists: an earlier analysis put a Wilson interval on")
    print("POOLED replicate counts, treating repeated measurements of the same")
    print("frozen rows as independent samples. That interval was retracted.")
    print()
    print("Where it actually lived, corrected 28 Sept 2026: the retracted interval")
    print("appeared in the F2 narrative in paper/FINDINGS.md (now struck through and")
    print("labelled), NOT in rq3_summary.json. run_rq3.py's saved summary carries")
    print("pooled counts as DESCRIPTIVE totals only, with an 'interpretation' field")
    print("stating that a pooled Wilson interval is invalid. Do not tell readers to")
    print("distrust that artifact -- it is correct.")

    out = [analyse(dom, args.bootstrap, args.seed) for dom in ("abstracts", "reviews")]
    for r in out:
        report(r)

    dest = RESULTS / "rq3_uncertainty.json"
    dest.write_text(json.dumps({
        "purpose": "correct uncertainty for RQ3; supersedes the pooled Wilson interval",
        "invalid_method": "Wilson on counts pooled across replicates of a frozen row set",
        "why_invalid": ("replicates re-measure the same rows; pooled n counts each "
                        "distinct row once per replicate, and F3 shows the repeats "
                        "are repeated and dependent"),
        "report_these": ["run_to_run.spread_pp",
                         "row_sampling.always_contradicts.wilson",
                         "cluster_bootstrap.ci"],
        "domains": out,
    }, indent=2))
    print(f"\nWrote {dest.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
