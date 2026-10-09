"""
OperatorBench -- corrected abstracts labels (topic_label_v2)

WHY THIS EXISTS
---------------
`prep_datasets.py` built `topic_label` from `ccdv/arxiv-classification` with:

    ml_categories = {0, 1, 2, 3}   # cs.AI, cs.CL, cs.CV, cs.LG typically

The comment says "typically". It guessed, and the guess is wrong -- index 0 is
math.AC (Commutative Algebra) and index 3 is cs.SY (Systems and Control).
See `paper/FINDINGS.md` F9.

This script derives a corrected label from the **authors' own declared arXiv
primary category**, which is present in `abstract_text` itself (the arXiv stamp
line, e.g. "arXiv:1802.06481v1 [cs.IT] 19 Feb 2018").

FREEZE DISCIPLINE (D-09)
------------------------
`prep_datasets.py` is NOT re-run and nothing is resampled. This script is
read-only on `abstracts_v1.csv`. Row membership and row order are asserted
identical, and the output is a SEPARATE file keyed by the same row_id. The
freeze holds.

HONESTY
-------
The label is three-valued. Rows with no arXiv stamp are `unknown`, never
guessed. Every downstream rate reports its denominator and the coverage rate
alongside it. Inventing a label for untagged rows would reproduce exactly the
defect this script exists to fix.

Usage:
    python scripts/derive_topic_label_v2.py
    python scripts/derive_topic_label_v2.py --check    # verify, write nothing
"""

import re
import sys
import json
import hashlib
import argparse
import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "data" / "abstracts" / "abstracts_v1.csv"
OUT = ROOT / "data" / "abstracts" / "abstracts_v1_labels_v2.csv"
MANIFEST = ROOT / "data" / "abstracts" / "manifest_labels_v2.json"

# The ML set is identical to `scripts/make_handcheck_sheet.py`'s ML_CATS, so
# Signal A in the coding sheet and topic_label_v2 can never disagree. If one
# changes, change both.
ML_CATS = {"cs.LG", "cs.AI", "cs.CV", "cs.CL", "cs.NE", "stat.ML"}

# Anchored on the arXiv stamp rather than on "any bracket", so a bracketed
# citation or a matrix index cannot be mistaken for a category. Captures the
# bracket contents verbatim, which also admits dotless categories (quant-ph,
# hep-th, gr-qc) that a `\w+\.\w{2}` pattern silently drops.
STAMP_RE = re.compile(r"arXiv:\s*[a-zA-Z0-9./-]+\s*\[([^\]]{2,40})\]")

# The runner truncates to 4000 chars before the model sees anything (D-10), so
# the category is only a legitimate signal if it survives truncation. Extract
# from the same window the operators saw -- not from the full text.
TRUNCATE_CHARS = 4000


def extract_category(text: str):
    """Return the authors' declared arXiv primary category, or None."""
    m = STAMP_RE.search(str(text)[:TRUNCATE_CHARS])
    return m.group(1).strip() if m else None


def derive(df: pd.DataFrame) -> pd.DataFrame:
    # NB: build the list first. `Series.map` converts None to NaN, and `x is
    # None` is False for NaN -- which would silently label every untagged row
    # "other" instead of "unknown". That is the same class of defect as F9
    # (an unexamined assumption quietly producing a wrong proxy label), so the
    # missing-value test is explicit here rather than incidental.
    cats = [extract_category(t) for t in df["abstract_text"]]
    out = pd.DataFrame(index=df.index)
    out["arxiv_category"] = cats
    out["topic_label_v2"] = [
        "unknown" if c is None else ("machine_learning" if c in ML_CATS else "other")
        for c in cats
    ]
    out["label_source"] = [
        "no_arxiv_stamp" if c is None else "author_declared_arxiv_category"
        for c in cats
    ]
    out["topic_label_v1"] = df["topic_label"]
    out["v1_v2_agree"] = [
        None if v2 == "unknown" else bool(v1 == v2)
        for v1, v2 in zip(out["topic_label_v1"], out["topic_label_v2"])
    ]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true",
                    help="report only; write nothing")
    args = ap.parse_args()

    df = pd.read_csv(SRC, escapechar="\\", index_col="row_id")

    # Freeze assertions. If either fails, stop -- something has touched the
    # frozen data and no derived label is trustworthy.
    assert len(df) == 500, f"expected 500 rows, found {len(df)}"
    assert list(df.index) == list(range(500)), "row_id order has changed"

    out = derive(df)

    tagged = out["arxiv_category"].notna().sum()
    counts = out["topic_label_v2"].value_counts().to_dict()
    known = out[out["topic_label_v2"] != "unknown"]
    disagree = (~known["v1_v2_agree"].astype(bool)).sum()

    print("=" * 68)
    print("topic_label_v2 -- corrected abstracts labels")
    print("=" * 68)
    print(f"rows                       {len(df)}")
    print(f"arXiv category found       {tagged} ({tagged/len(df):.1%})")
    print(f"no stamp within {TRUNCATE_CHARS} chars   {len(df)-tagged}")
    print()
    print("topic_label_v2:")
    for k in ("machine_learning", "other", "unknown"):
        print(f"  {k:<20} {counts.get(k, 0)}")
    print()
    print(f"v1 vs v2, on the {len(known)} labelled rows:")
    print(f"  disagree                 {disagree} ({disagree/len(known):.1%})")
    print()
    print("v1 = machine_learning, top actual categories (the F9 defect):")
    bad = out[(out.topic_label_v1 == "machine_learning")
              & (out.topic_label_v2 == "other")]
    for cat, n in bad["arxiv_category"].value_counts().head(12).items():
        print(f"  {n:4d}  {cat}")
    print()
    print("v1 = other, but actually ML-declared:")
    missed = out[(out.topic_label_v1 == "other")
                 & (out.topic_label_v2 == "machine_learning")]
    for cat, n in missed["arxiv_category"].value_counts().head(12).items():
        print(f"  {n:4d}  {cat}")

    if args.check:
        print("\n--check: nothing written.")
        return

    out.to_csv(OUT)
    sha = hashlib.sha256(OUT.read_bytes()).hexdigest()[:16]
    MANIFEST.write_text(json.dumps({
        "derived_from": SRC.name,
        "derived_from_sha256_prefix": hashlib.sha256(
            SRC.read_bytes()).hexdigest()[:16],
        "generated": datetime.date.today().isoformat(),
        "sha256_prefix": sha,
        "rows": len(out),
        "method": "authors' declared arXiv primary category, parsed from the "
                  f"arXiv stamp within the first {TRUNCATE_CHARS} chars "
                  "(the same window the operators saw)",
        "ml_categories": sorted(ML_CATS),
        "coverage": {"tagged": int(tagged), "untagged": int(len(df) - tagged)},
        "label_counts": counts,
        "v1_v2_disagreements_on_labelled_rows": int(disagree),
        "note": "Supplements topic_label; does not replace it. Row membership "
                "and order are identical to abstracts_v1.csv (asserted). "
                "prep_datasets.py was not re-run (D-09).",
    }, indent=2))
    print(f"\nWrote {OUT.relative_to(ROOT)}  sha256={sha}")
    print(f"Wrote {MANIFEST.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
