"""
Archived generator for the original single-coder exploratory hand-check.

The worksheet's GENUINE / AMBIGUOUS / FILTER-WRONG codebook was not validated
for construct validity, used one coder who had seen the model outputs, and has
no inter-coder agreement evidence. The pre-registered go/no-go gate failed and
is not a validity criterion. Do not use this generator to produce a new
adjudication or clear-cut rate. The existing coded worksheet is preserved;
regenerating it would overwrite the human notes.

Usage is retained only for archival reconstruction; see README.md before
running. Output: results/handcheck_<domain>.md
"""

import re
import sys
import argparse
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from configs.pipelines import PIPELINES

EXCERPT_CHARS = 1200  # enough to judge, short enough to read 20 of them

# arXiv primary categories, as assigned by the authors. This is the single most
# useful signal for adjudication and it requires no ML expertise to read: the
# people who wrote the paper declared what field it belongs to.
ARXIV_CAT_RE = re.compile(r"\[([a-zA-Z-]+\.[A-Za-z]{2})\]")

ML_CATS = {"cs.LG", "cs.AI", "cs.CV", "cs.CL", "cs.NE", "stat.ML"}
CAT_MEANING = {
    "cs.LG": "Machine Learning", "cs.AI": "Artificial Intelligence",
    "cs.CV": "Computer Vision", "cs.CL": "Computation and Language (NLP)",
    "cs.NE": "Neural and Evolutionary Computing", "stat.ML": "Machine Learning (stats)",
    "math.ST": "Statistics Theory", "stat.TH": "Statistics Theory",
    "math.AT": "Algebraic Topology", "math.OC": "Optimization and Control",
    "math.PR": "Probability", "math.NA": "Numerical Analysis",
    "cs.PL": "Programming Languages", "cs.DS": "Data Structures and Algorithms",
    "cs.DB": "Databases", "cs.DC": "Distributed Computing",
    "cs.IT": "Information Theory", "cs.SY": "Systems and Control",
    "cs.CR": "Cryptography and Security", "cs.SE": "Software Engineering",
    "q-bio.QM": "Quantitative Methods (biology)", "q-bio.GN": "Genomics",
    "physics.data-an": "Data Analysis (physics)", "stat.AP": "Statistics Applications",
    "stat.ME": "Statistics Methodology", "stat.CO": "Computational Statistics",
    "econ.EM": "Econometrics", "eess.SP": "Signal Processing",
}


def arxiv_category(text):
    """Return (code, human-readable meaning, is_ml_category) or (None, None, None)."""
    m = ARXIV_CAT_RE.search(text[:3000])
    if not m:
        return None, None, None
    code = m.group(1)
    return code, CAT_MEANING.get(code, "—"), code in ML_CATS


def load_map(domain, map_csv):
    path = ROOT / (map_csv or f"results/rq3_preliminary_{domain}_map.csv")
    if not path.exists():
        sys.exit(f"No map CSV at {path}. Pass --map-csv explicitly.")
    return pd.read_csv(path, index_col="row_id"), path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--domain", choices=["reviews", "abstracts"], required=True)
    ap.add_argument("--map-csv", default=None)
    ap.add_argument("--n", type=int, default=None,
                    help="cap the number of rows (default: all contradictions)")
    ap.add_argument("--seed", type=int, default=42,
                    help="sampling seed when --n is smaller than the population")
    args = ap.parse_args()

    cfg = PIPELINES[args.domain]
    map_col = cfg["map"]["output_col"]
    text_col = cfg["text_col"]
    is_contra = cfg["contradiction"]["is_contradiction"]

    df, src = load_map(args.domain, args.map_csv)

    contradicted = [rid for rid, row in df.iterrows()
                    if is_contra(True, str(row[map_col]))]
    total = len(contradicted)
    if args.n and args.n < total:
        # Reproducible sample, so the worksheet can be regenerated identically.
        contradicted = (pd.Series(contradicted)
                        .sample(args.n, random_state=args.seed)
                        .sort_values().tolist())

    out = ROOT / "results" / f"handcheck_{args.domain}.md"
    lines = [
        f"# Hand-check worksheet — {args.domain}",
        "",
        f"Source: `{src.relative_to(ROOT)}`",
        f"Contradictions in this run: **{total}** "
        f"of {len(df)} filter-positive rows ({total/len(df):.1%})",
        f"Rows in this sheet: **{len(contradicted)}**",
        "",
        "> **Exploratory hand-check only.** One coder; not blind to the prior",
        "> model outputs; no inter-coder agreement; construct validity not",
        "> established. Categories below record one coder's rubric, not ground",
        "> truth, an objective contradiction label, or a population-level rate.",
        "",
        f"Filter asked: *{cfg['filter']['predicate'].split('{')[0].strip()}* → admitted these rows",
        f"Map asked: *{cfg['map']['instruction'].split('Review:')[0].split('Abstract:')[0].strip()}*",
        "",
        "---",
        "",
        "## Historical coder rubric — not a validated adjudication instrument",
        "",
        "You are not assessing research quality. You are answering one question:",
        "",
        "> **Did the two operators give answers that can both be right?**",
        "",
        "### Two signals do most of the work",
        "",
        "**1. The arXiv category, shown for each row below.** The paper's own authors",
        "declared which field it belongs to. `cs.LG` / `cs.AI` / `stat.ML` mean the",
        "authors called it machine learning. `math.ST`, `math.AT`, `q-bio.QM`, `cs.PL`",
        "mean they did not. You do not need to evaluate the mathematics to read a label",
        "the authors chose. Note this text was visible to the model too.",
        "",
        "**2. What the paper claims to contribute.** Skim the excerpt for the verb:",
        "",
        "| Language | Points to |",
        "|---|---|",
        "| \"we train\", \"accuracy\", \"dataset\", \"benchmark\", \"our model outperforms\" | ML |",
        "| \"we prove\", \"theorem\", \"asymptotic\", \"convergence\", \"we derive\" | theory |",
        "| \"implementation\", \"throughput\", \"scalable\", \"framework\", \"runtime\" | systems |",
        "| domain application (biology, physics, finance) using existing methods | other |",
        "",
        "### The decision",
        "",
        "- **GENUINE** — this single coder's rubric marked the row when the author",
        "  category and contribution signal both indicated ML but the map chose",
        "  another label. This code is exploratory and does not establish logical",
        "  contradiction or a validated construct.",
        "- **AMBIGUOUS** — ML category but theoretical contribution, or vice versa. Both",
        "  answers defensible under this coder's rubric. This does not establish",
        "  that the model is coherent or that the input is objectively ambiguous.",
        "- **FILTER-WRONG** — this coder judged the ML content incidental under the",
        "  rubric's aboutness rule. This is not a ground-truth filter error.",
        "",
        "### Stated coder rules — Vansika, 12 Aug 2026 (not independently validated)",
        "",
        "**Rule M (map question):** if ML is primarily the application context rather",
        "than the method/contribution, classify as NOT ML. A primary-contribution test.",
        "",
        "**Rule F (filter question):** a looser *aboutness* test. The filter was asked",
        "\"is this paper **about** ML?\", not \"is ML the primary topic?\". Substantive ML",
        "discussion makes \"yes\" defensible. FILTER-WRONG only when ML is incidental.",
        "",
        "The two rules differ because the operators were asked different questions.",
        "Holding the filter to a primary-contribution standard would judge it against a",
        "question it was never asked, and would erase the framing asymmetry the paper",
        "is about (`results/lotus_prompt_analysis.md`).",
        "",
        "**Historical convention:** coder hesitation was assigned AMBIGUOUS by this",
        "rubric. That convention is not an objective definition of ambiguity.",
        "",
        "The pre-registered **>50% GENUINE** gate was evaluated and failed. It was an",
        "internal decision rule, not a test of construct validity.",
        "",
        "### Write one line of reasoning per row",
        "",
        "Name the signal you used. Good: *\"cs.LG paper proposing a new architecture,",
        "map said 'systems' — no defensible reading.\"* Bad: *\"seems wrong.\"* These",
        "lines become the paper's failure taxonomy and will be quoted.",
        "",
        "---",
        "",
    ]

    for i, rid in enumerate(contradicted, 1):
        row = df.loc[rid]
        text = str(row[text_col]) if text_col in df.columns else "(text column not in this CSV)"
        excerpt = text[:EXCERPT_CHARS].replace("\n", " ").strip()
        truncated = " …" if len(text) > EXCERPT_CHARS else ""
        gt = row.get(cfg["ground_truth_col"], "n/a")
        raw = row.get(f"_raw_{map_col}", "")
        code, meaning, is_ml = arxiv_category(text)
        if code:
            verdict_hint = "author-declared ML" if is_ml else "author-declared NOT ML"
            cat_cell = f"`{code}` — {meaning} ({verdict_hint})"
        else:
            cat_cell = "not found in text — judge from content alone"

        lines += [
            f"### {i}. row_id {rid}",
            "",
            f"| filter | map label | raw output | dataset label |",
            f"|---|---|---|---|",
            f"| admitted | **{row[map_col]}** | `{raw}` | {gt} |",
            "",
            f"**arXiv category:** {cat_cell}",
            "",
            f"> {excerpt}{truncated}",
            "",
            "**Verdict:** ` GENUINE / AMBIGUOUS / FILTER-WRONG `",
            "",
            "**Why:** ",
            "",
            "---",
            "",
        ]

    lines += [
        "## Tally (fill in when done)",
        "",
        "| Verdict | Count | % |",
        "|---|---|---|",
        "| GENUINE | | |",
        "| AMBIGUOUS | | |",
        "| FILTER-WRONG | | |",
        "",
        "**Gate check:** GENUINE > 50% → headline viable. Below → report the",
        "contradiction rate on clear-cut rows separately, per spec 01 section 7.",
        "",
        "**Patterns noticed:** ",
        "",
    ]

    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {out}")
    print(f"  {len(contradicted)} rows to judge (of {total} contradictions)")


if __name__ == "__main__":
    main()
