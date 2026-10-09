"""
OperatorBench -- RQ4 injection columns (resolves G1)

Historical G1: the original RQ4 config named nonexistent metadata columns.
The current config records the resolved scope (two hosted replicates on
abstracts only). This script derives the real submission-month field used by
that experiment.

ABSTRACTS -- real metadata, available
-------------------------------------
The arXiv stamp inside `abstract_text` carries the authors' actual submission
date: "arXiv:1802.06481v1 [cs.IT] 19 Feb 2018". 449 of 500 rows have one.
`submission_month` is therefore REAL production metadata, not fabricated.

REVIEWS -- no real metadata, and none is invented
-------------------------------------------------
The IMDB test split carries `text` and `label` only. There is no timestamp, no
user id, no source field. RQ4 is therefore run on abstracts only, and reviews
is written up as a stated limitation: the frozen source carries no production
metadata to inject. Synthesising timestamps would abandon the "real-but-
irrelevant" design that spec §7 and `LOTUS-reading-notes.md` §13 argue for --
the entire point is that the metadata is something a practitioner would have no
reason to strip, which a value we made up is not.

WHAT THE COLUMN IS FOR
----------------------
The date is already present in the text the operators read, so injecting it as
a column does not give the model new information. That is deliberate and it is
the finding: it isolates the effect of a fact's SCHEMA POSITION, holding its
content constant. Practitioners pass metadata as columns constantly. If
promoting a fact from prose to a typed field changes the operator's output --
and changes whether two operators agree -- that is a property of the operator
interface, not of the model's knowledge.

Freeze discipline (D-09): read-only on the frozen CSV, output is a separate
file keyed by the same row_id, row membership and order asserted unchanged.

Usage:
    python scripts/derive_injection_cols.py
    python scripts/derive_injection_cols.py --check
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
OUT = ROOT / "data" / "abstracts" / "abstracts_v1_injection.csv"
MANIFEST = ROOT / "data" / "abstracts" / "manifest_injection.json"

TRUNCATE_CHARS = 4000  # the window the operators actually see (D-10)

STAMP_DATE_RE = re.compile(
    r"arXiv:\s*[a-zA-Z0-9./-]+\s*\[[^\]]{2,40}\]\s*"
    r"(\d{1,2})\s+([A-Z][a-z]{2})\s+(\d{4})"
)

MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1)}


def extract_submission_month(text: str):
    """Return 'YYYY-MM' from the arXiv stamp, or None."""
    m = STAMP_DATE_RE.search(str(text)[:TRUNCATE_CHARS])
    if not m:
        return None
    _, mon, year = m.groups()
    if mon not in MONTHS:
        return None
    return f"{year}-{MONTHS[mon]:02d}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="report only")
    args = ap.parse_args()

    df = pd.read_csv(SRC, escapechar="\\", index_col="row_id")
    assert len(df) == 500, f"expected 500 rows, found {len(df)}"
    assert list(df.index) == list(range(500)), "row_id order has changed"

    months = [extract_submission_month(t) for t in df["abstract_text"]]
    out = pd.DataFrame(index=df.index)
    out["submission_month"] = months
    out["has_submission_month"] = [m is not None for m in months]

    n = int(out["has_submission_month"].sum())
    print("=" * 68)
    print("RQ4 injection columns (G1)")
    print("=" * 68)
    print(f"abstracts rows                 {len(df)}")
    print(f"submission_month extracted     {n} ({n/len(df):.1%})")
    print(f"no date in stamp               {len(df)-n}")
    print()
    yr = pd.Series([m[:4] for m in months if m]).value_counts().sort_index()
    print("year spread:", dict(yr))
    print()
    print("reviews: NO real metadata in the frozen source -- RQ4 is")
    print("abstracts-only and reviews ships as a stated limitation.")
    print()
    print("RQ4 eligible rows = rows with a submission_month AND admitted by")
    print("the baseline filter. Rows without one are excluded from rates,")
    print("never defaulted -- the denominator is reported alongside.")

    if args.check:
        print("\n--check: nothing written.")
        return

    out.to_csv(OUT)
    sha = hashlib.sha256(OUT.read_bytes()).hexdigest()[:16]
    MANIFEST.write_text(json.dumps({
        "derived_from": SRC.name,
        "generated": datetime.date.today().isoformat(),
        "sha256_prefix": sha,
        "rows": len(out),
        "column": "submission_month",
        "format": "YYYY-MM",
        "method": "authors' arXiv submission date, parsed from the arXiv stamp "
                  f"within the first {TRUNCATE_CHARS} chars",
        "coverage": {"present": n, "absent": len(df) - n},
        "reviews": "not derived -- IMDB test split carries no production "
                   "metadata; RQ4 is abstracts-only by design, not omission",
        "note": "The date is already present in abstract_text. Injecting it as "
                "a column holds information constant and varies only schema "
                "position. See the RQ4 methodology note.",
    }, indent=2))
    print(f"\nWrote {OUT.relative_to(ROOT)}  sha256={sha}")
    print(f"Wrote {MANIFEST.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
