"""
OperatorBench -- F16: does the proxy's primary-category-only definition bias F10/F11?

WHY THIS EXISTS
---------------
`topic_label_v2` uses only the PRIMARY arXiv category in each paper's stamp.
arXiv papers are routinely cross-listed: e.g. row 1 is cs.RO primary but also
listed in cs.AI and cs.LG. Under the primary-only proxy such a paper counts as
non-ML, so the filter "about ML?" admitting it is scored as a false positive,
and its disagreements land in F11's "non-ML-declared" group. A reviewer can
argue F10's precision and F11's 4.2x concentration are partly proxy artifacts.

WHAT THIS DOES
--------------
1. Fetches every categorised paper's full category list from the arXiv API
   (export.arxiv.org, batches of 100, 3 s apart as arXiv asks) and caches it
   to data/abstracts/abstracts_v1_crosslists.csv. The API's primary category
   is checked against the stamp's.
2. Recomputes F10 and F11 per frozen replicate, and the F12 precision/recall,
   under a second proxy definition.

DEFINITION, FIXED BEFORE ANY RESULT WAS COMPUTED
------------------------------------------------
  ml_any = the paper lists ANY category in ML_CATS (primary or cross-list),
  with ML_CATS identical to scripts/derive_topic_label_v2.py:
  {cs.LG, cs.AI, cs.CV, cs.CL, cs.NE, stat.ML}.
Rows without a stamp stay excluded (51). Both definitions are reported side
by side; neither replaces the other. A primary declaration and a cross-list
are both author choices; cross-listing is the more inclusive reading and is
closer to the filter's aboutness question.

Caveat: the API returns categories for the latest version of each paper,
which can differ from the version in the frozen text (rare).

No LLM calls. Network: arXiv API only, once; afterwards it reads the cache.

Usage:
    python scripts/crosslist_reanalysis.py           # fetch if not cached, then analyse
    python scripts/crosslist_reanalysis.py --refetch
"""

import re
import sys
import json
import time
import argparse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from configs.pipelines import PIPELINES
from harness.metrics import wilson_ci

ML_CATS = {"cs.LG", "cs.AI", "cs.CV", "cs.CL", "cs.NE", "stat.ML"}
ID_RE = re.compile(r"arXiv:\s*([a-zA-Z0-9./-]+?)(?:v\d+)?\s*\[([^\]]{2,40})\]")
CACHE = ROOT / "data" / "abstracts" / "abstracts_v1_crosslists.csv"
OUT = ROOT / "results" / "crosslist_reanalysis.json"
API = "https://export.arxiv.org/api/query?id_list={ids}&max_results={n}"
NS = {"a": "http://www.w3.org/2005/Atom", "arxiv": "http://arxiv.org/schemas/atom"}


def stamp_ids() -> pd.DataFrame:
    df = pd.read_csv(ROOT / "data" / "abstracts" / "abstracts_v1.csv",
                     escapechar="\\", index_col="row_id")
    rows = []
    for rid, text in df.abstract_text.items():
        m = ID_RE.search(str(text)[:4000])
        if m:
            rows.append({"row_id": rid, "arxiv_id": m.group(1), "stamp_primary": m.group(2)})
    return pd.DataFrame(rows).set_index("row_id")


def fetch(ids: pd.DataFrame) -> pd.DataFrame:
    by_id = {}
    id_list = ids.arxiv_id.tolist()
    for i in range(0, len(id_list), 100):
        chunk = id_list[i:i + 100]
        url = API.format(ids=",".join(chunk), n=len(chunk))
        with urllib.request.urlopen(url, timeout=60) as resp:
            root = ET.fromstring(resp.read())
        for entry in root.findall("a:entry", NS):
            eid = entry.find("a:id", NS).text.rsplit("/abs/", 1)[-1]
            eid = re.sub(r"v\d+$", "", eid)
            prim = entry.find("arxiv:primary_category", NS)
            cats = [c.get("term") for c in entry.findall("a:category", NS)]
            by_id[eid] = {"api_primary": prim.get("term") if prim is not None else None,
                          "all_categories": ";".join(cats)}
        print(f"  fetched {min(i + 100, len(id_list))}/{len(id_list)}")
        time.sleep(3)
    out = ids.copy()
    out["api_primary"] = [by_id.get(a, {}).get("api_primary") for a in out.arxiv_id]
    out["all_categories"] = [by_id.get(a, {}).get("all_categories") for a in out.arxiv_id]
    return out


def prf(adm: set, ml: set, labelled: set) -> dict:
    a = adm & labelled
    tp, fp, fn = len(a & ml), len(a - ml), len(ml - a)
    return {"tp": tp, "fp": fp, "fn": fn,
            "precision": round(tp / (tp + fp), 4) if tp + fp else None,
            "precision_ci": [round(x, 4) for x in wilson_ci(tp, tp + fp)] if tp + fp else None,
            "recall": round(tp / (tp + fn), 4) if tp + fn else None,
            "recall_ci": [round(x, 4) for x in wilson_ci(tp, tp + fn)] if tp + fn else None}


def split(map_s: pd.Series, ml: set, labelled: set) -> dict:
    is_contra = PIPELINES["abstracts"]["contradiction"]["is_contradiction"]
    res = {}
    for name, rows in (("ml", [r for r in map_s.index if r in ml]),
                       ("non_ml", [r for r in map_s.index if r in labelled and r not in ml])):
        k = sum(is_contra(True, str(map_s[r])) for r in rows)
        res[name] = {"k": k, "n": len(rows), "rate": round(k / len(rows), 4) if rows else None,
                     "ci": [round(x, 4) for x in wilson_ci(k, len(rows))] if rows else None}
    if res["ml"]["rate"]:
        res["ratio"] = round(res["non_ml"]["rate"] / res["ml"]["rate"], 2)
    return res


def main() -> None:
    ap = argparse.ArgumentParser(description="F16 cross-listing reanalysis")
    ap.add_argument("--refetch", action="store_true")
    args = ap.parse_args()

    if CACHE.exists() and not args.refetch:
        meta = pd.read_csv(CACHE, index_col="row_id")
    else:
        print("Fetching categories from the arXiv API ...")
        meta = fetch(stamp_ids())
        meta.to_csv(CACHE)
        print(f"wrote {CACHE.relative_to(ROOT)}")

    missing = meta.index[meta.all_categories.isna()].tolist()
    mismatch = meta.index[meta.api_primary.notna() & (meta.api_primary != meta.stamp_primary)].tolist()
    cats = meta.all_categories.fillna("").str.split(";")
    labelled = set(meta.index)
    ml_primary = set(meta.index[meta.stamp_primary.isin(ML_CATS)])
    ml_any = set(meta.index[[bool(set(c) & ML_CATS) for c in cats]])

    out = {
        "definition": "ml_any = any listed category in ML_CATS (primary or cross-list)",
        "ml_cats": sorted(ML_CATS),
        "coverage": {"labelled_rows": len(labelled), "api_missing": missing,
                     "api_primary_differs_from_stamp": mismatch},
        "label_counts": {"ml_primary": len(ml_primary), "ml_any": len(ml_any),
                         "reclassified_to_ml_by_crosslist": len(ml_any - ml_primary),
                         "primary_ml_without_ml_listing": len(ml_primary - ml_any)},
        "replicates": [],
    }
    for rep in (1, 2, 3):
        m = pd.read_csv(ROOT / "results" / f"rq3_abstracts_rep{rep}_map.csv", escapechar="\\",
                        usecols=["row_id", "mapped_topic"], index_col="row_id").mapped_topic
        adm = set(m.index)
        out["replicates"].append({
            "replicate": rep,
            "F10_primary": prf(adm, ml_primary, labelled), "F10_any": prf(adm, ml_any, labelled),
            "F11_primary": split(m, ml_primary, labelled), "F11_any": split(m, ml_any, labelled),
        })
    for rep in (1, 2):
        adm = set(pd.read_csv(ROOT / "results" / f"f7_ablation_abstracts_rep{rep}_map.csv",
                              escapechar="\\", usecols=["row_id"]).row_id)
        out["replicates"][rep - 1]["F12_revised_filter_primary"] = prf(adm, ml_primary, labelled)
        out["replicates"][rep - 1]["F12_revised_filter_any"] = prf(adm, ml_any, labelled)

    OUT.write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")
    lc = out["label_counts"]
    print(f"\nML (primary only): {lc['ml_primary']}   ML (any listing): {lc['ml_any']}   "
          f"reclassified by cross-list: {lc['reclassified_to_ml_by_crosslist']}")
    print(f"API missing: {len(missing)}   API primary != stamp: {len(mismatch)}")
    for r in out["replicates"]:
        p, a = r["F10_primary"], r["F10_any"]
        sp, sa = r["F11_primary"], r["F11_any"]
        print(f"rep{r['replicate']}: F10 precision {p['precision']:.2%} -> {a['precision']:.2%}, "
              f"recall {p['recall']:.2%} -> {a['recall']:.2%} | F11 ML {sp['ml']['k']}/{sp['ml']['n']} vs "
              f"non-ML {sp['non_ml']['k']}/{sp['non_ml']['n']} ({sp.get('ratio')}x)  ->  "
              f"ML {sa['ml']['k']}/{sa['ml']['n']} vs non-ML {sa['non_ml']['k']}/{sa['non_ml']['n']} ({sa.get('ratio')}x)")
    print(f"\nwrote {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
