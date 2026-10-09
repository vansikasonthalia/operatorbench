"""
OperatorBench -- standing consistency guard for the contradiction hand-check.

WHY THIS EXISTS
---------------
The hand-check worksheet was generated from the PRELIMINARY run, then the frozen
replicates were produced, then the worksheet was coded, then findings were
written from it. Four artifacts, three of them hand-maintained, all quoting row
counts at each other. That is exactly the setup in which a denominator silently
drifts and a reviewer finds it before you do.

An audit found precisely that: a 36-row cross-tab quoted against a 35-row tally,
and a gate statement whose row set was never named. This script makes the
reconciliation mechanical so it cannot rot again.

WHAT IT ASSERTS
---------------
1. The CANONICAL row set is the persistence set: rows contradicting in ALL
   frozen replicates. It must equal rep1's set (rep1 == rep3 == intersection),
   because F3's persistence figure and F12's 36 -> 4 both use that denominator.
   One denominator for three findings, or the paper cannot be reconciled.
2. Every CODED worksheet row is in that set. A coded row outside it has leaked a
   preliminary-run artifact into a reported rate.
3. Row 86 is on the worksheet, is in NO frozen replicate, and is UNCODED.
4. The tally and cross-tab recomputed from the per-row fields match what the
   worksheet prints, and the cross-tab total equals the coded count.
5. Every coded verdict agrees with the frozen codebook matrix (D-W2-16). The A3
   branch is a RESOLUTION PROCEDURE, not a category -- a row with a
   `Signal A override` line is scored on its resolved A code.
6. The gate outcome is determinate: even if every uncoded in-set row came back
   GENUINE, the pre-registered threshold is still not met. If that ever stops
   being true, the gate is genuinely open again and this script says so loudly.

A placeholder is NOT a verdict. The unfilled field reads
`GENUINE / AMBIGUOUS / FILTER-WRONG`; a naive regex matches "GENUINE" and
invents a coded row. That bug produced a false GENUINE count and a false
codebook violation in an earlier pass. The parser here rejects placeholders
explicitly -- do not simplify it.

No API calls. Exits non-zero on any inconsistency.

Usage:
    python scripts/check_handcheck_consistency.py
"""

import re
import sys
import json
import collections
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from configs.pipelines import PIPELINES

RESULTS = ROOT / "results"
SHEET = RESULTS / "handcheck_abstracts.md"
DOMAIN = "abstracts"
GATE_THRESHOLD = 18          # pre-registered: GENUINE > 18 of 36
EXPECT_EXCLUDED = 86         # on the sheet, in no frozen replicate

# Codebook v1, frozen 12 Aug 2026 (D-W2-16). Do not edit to make a test pass.
MATRIX = {
    ("A1", "B1"): "GENUINE",
    ("A1", "B2"): "AMBIGUOUS",
    ("A1", "B3"): "AMBIGUOUS",
    ("A2", "B1"): "AMBIGUOUS",
    ("A2", "B2"): "FILTER-WRONG",
    ("A2", "B3"): "FILTER-WRONG",
}
VERDICTS = ("GENUINE", "AMBIGUOUS", "FILTER-WRONG")
PLACEHOLDER_V = re.compile(r"GENUINE\s*/\s*AMBIGUOUS\s*/\s*FILTER-WRONG")
PLACEHOLDER_B = re.compile(r"B1\s*/\s*B2\s*/\s*B3")


def frozen_replicates():
    """Contradiction sets per frozen replicate, keyed by replicate number."""
    cfg = PIPELINES[DOMAIN]
    col = cfg["map"]["output_col"]
    is_contra = cfg["contradiction"]["is_contradiction"]
    out = {}
    for path in sorted(RESULTS.glob(f"rq3_{DOMAIN}_rep*_map.csv")):
        rep = int(re.search(r"_rep(\d+)_", path.name).group(1))
        m = pd.read_csv(path, index_col="row_id")
        out[rep] = {
            "admitted": set(m.index),
            "contradicted": {i for i in m.index if is_contra(True, str(m.loc[i, col]))},
        }
    return out


def parse_sheet():
    """row_id -> (A, B, verdict). A is the RESOLVED code; verdict None if uncoded."""
    text = SHEET.read_text(encoding="utf-8")
    rows = {}
    for block in re.split(r"\n### ", text):
        m = re.match(r"\d+\.\s*row_id\s*(\d+)", block)
        if not m:
            continue
        lines = block.splitlines()

        def line(key):
            return next((l for l in lines if key in l), "")

        v_line = line("**Verdict (from the matrix)")
        b_line = line("**Signal B (yours)")
        a_line = line("**Signal A —")
        o_line = line("**Signal A override")

        verdict = None
        if v_line and not PLACEHOLDER_V.search(v_line):
            hit = re.search(r"(%s)" % "|".join(VERDICTS), v_line)
            verdict = hit.group(1) if hit else None

        b = None
        if b_line and not PLACEHOLDER_B.search(b_line):
            hit = re.search(r"\b(B[123])\b", b_line)
            b = hit.group(1) if hit else None

        hit = re.search(r"\*\*(A[123])\*\*", a_line) or re.search(r"\b(A[123])\b", a_line)
        a = hit.group(1) if hit else None
        if o_line:                      # codebook A3 resolution, applied by the coder
            hit = re.search(r"\b(A[12])\b", o_line)
            if hit:
                a = hit.group(1)

        rows[int(m.group(1))] = (a, b, verdict)
    return rows


def printed_tally_and_tab():
    """The numbers the worksheet itself prints, so we can compare against them."""
    text = SHEET.read_text(encoding="utf-8")
    tally = {}
    for v in VERDICTS:
        m = re.search(rf"^\|\s*{v}\s*\|\s*(\d+)\s*\|", text, re.M)
        if m:
            tally[v] = int(m.group(1))
    tab = {}
    for a in ("A1", "A2"):
        m = re.search(rf"^\|\s*{a}\b[^|]*\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|",
                      text, re.M)
        if m:
            tab[a] = tuple(int(g) for g in m.groups())
    return tally, tab


def main():
    fails, notes = [], []
    reps = frozen_replicates()
    if len(reps) < 2:
        print("Need at least two frozen replicates; found "
              f"{sorted(reps)}. Nothing to reconcile.")
        return 1

    cons = {r: d["contradicted"] for r, d in reps.items()}
    canonical = set.intersection(*cons.values())      # the persistence set
    union = set().union(*cons.values())
    always_admitted = set.intersection(*[d["admitted"] for d in reps.values()])

    # (1) canonical set must coincide with a single replicate's set
    coincide = [r for r, c in cons.items() if c == canonical]
    if not coincide:
        fails.append("Canonical (persistence) set equals NO single replicate's set. "
                     "F3 and F12 can no longer share the hand-check denominator -- "
                     "restate every one of them explicitly.")
    else:
        notes.append(f"persistence set == rep{'/rep'.join(map(str, coincide))} set")

    rows = parse_sheet()
    coded = {r for r, (_, _, v) in rows.items() if v}
    uncoded = set(rows) - coded

    # (2) no coded row outside the canonical set
    stray = sorted(coded - canonical)
    if stray:
        fails.append(f"Coded rows outside the canonical set: {stray}. "
                     "These are preliminary-run artifacts and must not enter a rate.")

    # (3) the known excluded row
    if EXPECT_EXCLUDED in rows:
        if EXPECT_EXCLUDED in canonical:
            fails.append(f"Row {EXPECT_EXCLUDED} is now IN the canonical set; the "
                         "exclusion note in the worksheet and F6 is stale.")
        elif EXPECT_EXCLUDED in coded:
            fails.append(f"Row {EXPECT_EXCLUDED} has been CODED but contradicts in no "
                         "frozen replicate. Its verdict must not enter any rate.")
        else:
            notes.append(f"row {EXPECT_EXCLUDED} present, in no replicate, uncoded -- correct")

    # (4) recomputed vs printed
    in_set = sorted(coded & canonical)
    tally = collections.Counter(rows[r][2] for r in in_set)
    tab = collections.Counter((rows[r][0], rows[r][1]) for r in in_set)
    n = len(in_set)

    p_tally, p_tab = printed_tally_and_tab()
    for v in VERDICTS:
        if v in p_tally and p_tally[v] != tally[v]:
            fails.append(f"Worksheet prints {v} = {p_tally[v]}; recomputed {tally[v]}.")
    for a in ("A1", "A2"):
        got = tuple(tab[(a, b)] for b in ("B1", "B2", "B3"))
        if a in p_tab and p_tab[a] != got:
            fails.append(f"Worksheet prints {a} row {p_tab[a]}; recomputed {got}. "
                         "A mismatch of exactly the A3-resolved count means a table "
                         "somewhere is treating A3 as a category instead of a "
                         "resolution procedure.")
    placed = sum(tab.values())
    if placed != n:
        fails.append(f"Cross-tab places {placed} rows but {n} are coded in-set: "
                     f"{n - placed} row(s) have no matrix cell.")

    # (5) codebook agreement
    bad = [(r, rows[r][0], rows[r][1], rows[r][2], MATRIX.get((rows[r][0], rows[r][1])))
           for r in in_set if MATRIX.get((rows[r][0], rows[r][1])) != rows[r][2]]
    if bad:
        fails.append(f"{len(bad)} coded verdict(s) disagree with the frozen matrix: {bad}. "
                     "The codebook forbids per-row overrides.")

    # (6) is the gate determinate?
    missing = sorted(canonical - coded)
    observed = tally["GENUINE"]
    ceiling = observed + len(missing)
    gate_open = ceiling > GATE_THRESHOLD
    if gate_open:
        fails.append(f"Gate is NOT determinate: {observed} GENUINE observed, ceiling "
                     f"{ceiling} > threshold {GATE_THRESHOLD} with {len(missing)} row(s) "
                     f"uncoded {missing}. The gate cannot be reported as closed until "
                     "those are coded.")

    print("=" * 74)
    print("HAND-CHECK CONSISTENCY -- abstracts")
    print("=" * 74)
    print(f"  frozen replicates            : {sorted(reps)}")
    print(f"  admitted in every replicate  : {len(always_admitted)}")
    print(f"  CANONICAL persistence set    : {len(canonical)}   <- the only denominator")
    print(f"  union, ever contradicts      : {len(union)}"
          f"   (rep-specific extras: {sorted(union - canonical)})")
    print(f"  worksheet rows               : {len(rows)}  ({len(coded)} coded,"
          f" {len(uncoded)} uncoded {sorted(uncoded)})")
    print(f"  coded and in the canonical set: {n}"
          f"   -> coverage {n}/{len(canonical)} = {n / len(canonical):.1%}")
    print(f"  in the set but not coded     : {missing}")
    print(f"\n  tally (n={n}):", {v: tally[v] for v in VERDICTS})
    print(f"  cross-tab (n={placed}):")
    print(f"    {'':5}{'B1':>5}{'B2':>5}{'B3':>5}{'tot':>6}")
    for a in ("A1", "A2"):
        rr = [tab[(a, b)] for b in ("B1", "B2", "B3")]
        print(f"    {a:5}{rr[0]:5}{rr[1]:5}{rr[2]:5}{sum(rr):6}")
    cols = [sum(tab[(a, b)] for a in ("A1", "A2")) for b in ("B1", "B2", "B3")]
    print(f"    {'tot':5}{cols[0]:5}{cols[1]:5}{cols[2]:5}{placed:6}")
    print(f"\n  GATE  rule: GENUINE > {GATE_THRESHOLD} of {len(canonical)}")
    print(f"        observed {observed}, ceiling {ceiling} -> "
          f"{'OPEN -- undetermined' if gate_open else 'FAILS, determinately'}")
    for nt in notes:
        print(f"  ok: {nt}")

    dest = RESULTS / "handcheck_reconciliation.json"
    dest.write_text(json.dumps({
        "canonical_row_set": "contradicts in every frozen replicate (persistence set)",
        "n_canonical": len(canonical),
        "coincides_with_replicates": coincide,
        "coded_in_set": n,
        "coverage": round(n / len(canonical), 4),
        "uncoded_in_set": missing,
        "excluded_on_sheet_not_in_any_replicate": sorted(set(rows) - union),
        "excluded_not_persistent": sorted(union - canonical),
        "union_ever_contradicts": len(union),
        "tally": {v: tally[v] for v in VERDICTS},
        "cross_tab": {f"{a}x{b}": tab[(a, b)] for a in ("A1", "A2")
                      for b in ("B1", "B2", "B3")},
        "cross_tab_n": placed,
        "codebook_disagreements": len(bad),
        "gate": {"threshold": GATE_THRESHOLD, "observed_genuine": observed,
                 "ceiling": ceiling, "determinate": not gate_open,
                 "outcome": "OPEN" if gate_open else "FAILS"},
        "genuine_rows": [r for r in in_set if rows[r][2] == "GENUINE"],
        "consistent": not fails,
    }, indent=2))
    print(f"\n  wrote {dest.relative_to(ROOT)}")

    if fails:
        print("\n" + "!" * 74)
        print(f"{len(fails)} INCONSISTENCY(S):")
        for f in fails:
            print(f"  - {f}")
        print("!" * 74)
        return 1
    print("\n  ALL CONSISTENT -- worksheet, frozen replicates and codebook agree.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
