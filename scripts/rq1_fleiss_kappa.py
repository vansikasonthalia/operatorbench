"""
OperatorBench -- RQ1 Fleiss' kappa (closes spec gap G2).

WHY THIS SCRIPT EXISTS
----------------------
Spec §6 specified "per-row exact agreement across 5 runs + Fleiss' kappa" for
RQ1. Only the flip rate was ever computed. Raw agreement is chance-inflated when
labels are skewed (reviews filter output is ~54% True, map output ~49%
negative), so kappa corrects agreement for what the marginal label frequencies
would produce by chance.

Each replicate is treated as a "rater" and each row as a "subject". Map labels
are the normalised labels as stored; an out-of-schema label such as `mixed` is
its own category, because that is what the operator emitted.

Read kappa alongside the flip rate, never instead of it: with near-total
agreement, kappa is close to 1 and mostly restates F1. Its job is to show the
high agreement is not an artifact of skewed labels.

No API calls; reads only results/rq1/hosted_{domain}_rep{1..5}_{filter,map}.csv.

Usage:
    python scripts/rq1_fleiss_kappa.py
"""

import json
from collections import Counter
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RQ1 = ROOT / "results" / "rq1"
OUT = ROOT / "results" / "rq1_fleiss_kappa.json"
N_REPS = 5
COLUMNS = {
    ("reviews", "filter"): "filter_passed",
    ("reviews", "map"): "mapped_sentiment",
    ("abstracts", "filter"): "filter_passed",
    ("abstracts", "map"): "mapped_topic",
}


def load_ratings(domain: str, operator: str) -> pd.DataFrame:
    """One column per replicate, one row per row_id, values as strings."""
    col = COLUMNS[(domain, operator)]
    frames = []
    for rep in range(1, N_REPS + 1):
        path = RQ1 / f"hosted_{domain}_rep{rep}_{operator}.csv"
        df = pd.read_csv(path, usecols=["row_id", col]).set_index("row_id")
        frames.append(df[col].astype(str).rename(f"rep{rep}"))
    ratings = pd.concat(frames, axis=1, join="inner")
    if len(ratings) != 500:
        raise SystemExit(f"{domain}/{operator}: expected 500 shared rows, got {len(ratings)}")
    return ratings


def fleiss_kappa(ratings: pd.DataFrame) -> dict:
    """Fleiss' kappa for N subjects x n raters with nominal categories."""
    n_subjects, n_raters = ratings.shape
    categories = sorted(set(ratings.values.ravel()))
    counts = [Counter(row) for row in ratings.itertuples(index=False)]

    # Per-subject agreement P_i, then its mean P-bar.
    p_i = [
        (sum(c[k] ** 2 for k in categories) - n_raters) / (n_raters * (n_raters - 1))
        for c in counts
    ]
    p_bar = sum(p_i) / n_subjects

    # Chance agreement from pooled category proportions.
    total = n_subjects * n_raters
    p_j = {k: sum(c[k] for c in counts) / total for k in categories}
    p_e = sum(v ** 2 for v in p_j.values())

    kappa = (p_bar - p_e) / (1 - p_e) if p_e < 1 else float("nan")
    return {
        "n_rows": n_subjects,
        "n_replicates": n_raters,
        "categories": categories,
        "category_proportions": {k: round(v, 4) for k, v in p_j.items()},
        "observed_agreement_P_bar": round(p_bar, 4),
        "chance_agreement_P_e": round(p_e, 4),
        "fleiss_kappa": round(kappa, 4),
    }


def main() -> None:
    out = {
        "purpose": "Fleiss' kappa across the five hosted RQ1 replicates (spec §6; closes G2)",
        "raters": "replicates; subjects: the 500 frozen rows per domain",
        "caveat": ("Replicates are unseeded hosted executions, not independent human raters. "
                   "Kappa here measures run-to-run agreement corrected for label skew; "
                   "report it beside the F1 flip rate, not instead of it."),
        "results": {},
    }
    for (domain, operator) in COLUMNS:
        res = fleiss_kappa(load_ratings(domain, operator))
        out["results"][f"{domain}_{operator}"] = res
        print(f"{domain:9s} {operator:6s}  kappa={res['fleiss_kappa']:.4f}  "
              f"P_bar={res['observed_agreement_P_bar']:.4f}  P_e={res['chance_agreement_P_e']:.4f}")
    OUT.write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")
    print(f"\nwrote {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
