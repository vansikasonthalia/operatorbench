"""
OperatorBench -- RQ3: Cross-operator criterion disagreement (headline experiment)

Extends run_rq3_preliminary.py from a single run to N unseeded replicates
per domain, so the headline numbers carry a variance estimate rather than a
single point measurement.

WHAT THIS ADDS OVER THE PRELIMINARY SCRIPT
------------------------------------------
1. N replicates per domain (default 3), each saved as it completes.
2. Per-replicate operational disagreement rates with Wilson CIs; pooled counts
   reported as descriptive totals (a pooled Wilson interval is invalid
   because the same rows recur across replicates).
3. Repeatability of the operational disagreement status: for rows admitted in
   every replicate, report how often the frozen rule marks disagreement. This
   describes repeated outputs under this setup; it does not establish an
   intrinsic property of the operator pair or a source of variation.
4. Schema-boundary sensitivity analysis. The frozen reviews rule excludes
   invalid labels, whereas the abstracts rule includes every non-ML output.
   A separate breakdown distinguishes invalid outputs from valid-label
   disagreement without changing either frozen rule.

REPLICATE vs SEED
-----------------
PipelineRunner records `seed` in the call log but does not thread it into the
LiteLLM request for this study; BACKENDS["hosted"]["seed"] is None. These are
therefore unseeded replicates, not seed-controlled repeats. The CLI flag is
named --n-replicates for that reason; describe them as replicates in the paper.

Usage:
    cd C:\\Users\\vans\\OneDrive\\OperatorBench
    .venv\\Scripts\\activate
    $env:OPENAI_API_KEY = "sk-..."

    python scripts/run_rq3.py                          # both domains, 3 replicates
    python scripts/run_rq3.py --domain abstracts
    python scripts/run_rq3.py --n-replicates 5
    python scripts/run_rq3.py --aggregate-only         # re-aggregate saved runs
"""

import os
import sys
import json
import math
import time
import argparse
import pandas as pd
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from harness import (
    PipelineRunner, LLMInterceptor,
    contradiction_rate, schema_violation_rate, wilson_ci, format_results_table,
)
from harness.runner import BATCH_SIZE, BATCH_DELAY_SECONDS
from harness.artifacts import load_completed_pipeline
from harness.metrics import raw_schema_compliance, disagreement_breakdown
from configs.pipelines import PIPELINES, BACKENDS, RQ_CONFIGS

APPROX_TOKENS_PER_ROW = {"reviews": 350, "abstracts": 1100}
USD_PER_1M_INPUT_TOKENS = 0.15


def fmt_duration(seconds):
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    return f"{h}h {m}m" if h else f"{m}m {s}s"


def estimate_run(domains, n_reps, n_rows, delay=None):
    """Filter runs on all rows; map runs only on filter-positives (~30-55%)."""
    delay = BATCH_DELAY_SECONDS if delay is None else delay
    secs, tokens = 0.0, 0
    for domain in domains:
        map_rows = int(n_rows * (0.54 if domain == "reviews" else 0.31))
        for count in (n_rows, map_rows):
            nb = math.ceil(count / BATCH_SIZE)
            secs += ((nb - 1) * delay + nb * 2.0) * n_reps
        tokens += APPROX_TOKENS_PER_ROW.get(domain, 500) * (n_rows + map_rows) * n_reps
    return secs, (tokens / 1_000_000) * USD_PER_1M_INPUT_TOKENS


def preflight():
    """Fail fast if the hosted API key is missing -- see run_rq1.preflight()."""
    if not os.environ.get("OPENAI_API_KEY"):
        print("\nERROR: OPENAI_API_KEY is not set in this shell.\n")
        print("  PowerShell:  $env:OPENAI_API_KEY = \"sk-...\"")
        print("\nIt is per-window, so it must be set again in every new terminal.")
        sys.exit(1)


def run_tag(limit):
    """
    Namespace result files by row count so a --limit pilot cannot be mistaken
    for the full run. Replicates are skipped when their files already exist, so
    without this a 50-row pilot would be silently reused as the headline result.
    """
    return f"n{limit}_" if limit else ""


def load_domain(domain, limit=None):
    cfg = PIPELINES[domain]
    read_kwargs = {"index_col": "row_id"}
    if domain == "abstracts":
        read_kwargs["escapechar"] = "\\"
    df = pd.read_csv(ROOT / cfg["data_file"], **read_kwargs)
    return df.iloc[:limit] if limit else df


# ---------------------------------------------------------------------------
# One replicate
# ---------------------------------------------------------------------------

def run_replicate(runner, df, domain, replicate, tag="", force=False):
    """Run the full filter->map pipeline once. Returns the map DataFrame."""
    cfg = PIPELINES[domain]
    run_id = f"rq3_{tag}{domain}_rep{replicate}"
    map_path = ROOT / "results" / f"{run_id}_map.csv"

    if map_path.exists() and not force:
        print(f"  [rep {replicate}] already on disk -- loading (use --force to redo)")
        return load_completed_pipeline(ROOT / "results", run_id, df.index,
                                       cfg["map"]["output_col"])

    results = runner.run_filter_map(
        df=df,
        domain=domain,
        filter_predicate=cfg["filter"]["predicate"],
        map_instruction=cfg["map"]["instruction"],
        map_output_col=cfg["map"]["output_col"],
        valid_labels=cfg["map"]["valid_labels"],
        run_id=run_id,
    )
    return results["map_results"]


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def aggregate_domain(domain, n_reps, total_rows, tag=""):
    cfg = PIPELINES[domain]
    map_col = cfg["map"]["output_col"]
    valid = cfg["map"]["valid_labels"]
    is_contra = cfg["contradiction"]["is_contradiction"]
    valid_lower = {v.lower() for v in valid}

    map_runs, rep_ids = [], []
    for r in range(1, n_reps + 1):
        p = ROOT / "results" / f"rq3_{tag}{domain}_rep{r}_map.csv"
        if not p.exists():
            print(f"  WARNING: replicate {r} missing on disk, skipping")
            continue
        map_runs.append(pd.read_csv(p, index_col="row_id"))
        rep_ids.append(r)

    if not map_runs:
        return {"error": "no replicates found on disk"}

    per_rep, pooled_k, pooled_n = [], 0, 0
    sv_per_rep, sv_k, sv_n = [], 0, 0
    adj_k = 0

    for r, m in zip(rep_ids, map_runs):
        contra = contradiction_rate(m, map_col, is_contra)
        schema = schema_violation_rate(m, map_col, valid)

        # Sensitivity upper count: also treat out-of-schema labels as disagreements.
        out_of_schema_ids = {
            int(rid) for rid, row in m.iterrows()
            if str(row[map_col]).lower() not in valid_lower
        }
        contra_ids = {rid for rid, _ in contra["contradicted_rows"]}
        adj = len(contra_ids | out_of_schema_ids)
        adj_lo, adj_hi = wilson_ci(adj, len(m))

        per_rep.append({
            "replicate": r,
            "disagreement_breakdown": disagreement_breakdown(m, map_col, valid, is_contra),
            "raw_schema_compliance": raw_schema_compliance(m, f"_raw_{map_col}", valid)
            if f"_raw_{map_col}" in m else {"error": "raw outputs unavailable"},
            "n_filter_positive": contra["n_filter_positive"],
            "filter_rate": round(contra["n_filter_positive"] / total_rows, 4),
            "n_contradictions": contra["n_contradictions"],
            "rate": contra["rate"],
            "ci_lower": contra["ci_lower"],
            "ci_upper": contra["ci_upper"],
            "n_schema_violations": schema["n_violations"],
            "schema_rate": schema["rate"],
            "adjusted_n_contradictions": adj,
            "adjusted_rate": round(adj / len(m), 4) if len(m) else 0.0,
            "adjusted_ci": [round(adj_lo, 4), round(adj_hi, 4)],
            "contradicted_rows": contra["contradicted_rows"],
            "violation_examples": schema["violation_examples"],
        })
        pooled_k += contra["n_contradictions"]
        pooled_n += contra["n_filter_positive"]
        sv_k += schema["n_violations"]
        sv_n += schema["n_total"]
        adj_k += adj
        sv_per_rep.append(schema)

    # Event-weighted summaries across replicates are descriptive only: rows
    # recur across runs, so these counts are not independent Bernoulli trials.
    pooled_rate = pooled_k / pooled_n if pooled_n else 0.0

    rates = [p["rate"] for p in per_rep]
    mean_rate = sum(rates) / len(rates)
    spread = max(rates) - min(rates)

    # Contradiction persistence over rows admitted by the filter in EVERY replicate
    always_admitted = set(map_runs[0].index)
    for m in map_runs[1:]:
        always_admitted &= set(m.index)

    persistence = Counter()
    persistent_rows = []
    for rid in sorted(always_admitted):
        hits = sum(
            1 for m in map_runs
            if is_contra(True, str(m.loc[rid, map_col]))
        )
        persistence[hits] += 1
        if hits == len(map_runs):
            persistent_rows.append(int(rid))

    n_ever = sum(c for h, c in persistence.items() if h > 0)
    n_always = persistence.get(len(map_runs), 0)
    common_n = len(always_admitted)
    common_ever_lo, common_ever_hi = wilson_ci(n_ever, common_n)
    common_always_lo, common_always_hi = wilson_ci(n_always, common_n)

    return {
        "domain": domain,
        "n_replicates": len(map_runs),
        "total_rows": total_rows,
        "per_replicate": per_rep,
        "pooled": {
            "n_contradictions": pooled_k,
            "n_filter_positive": pooled_n,
            "rate": round(pooled_rate, 4),
            "mean_of_replicate_rates": round(mean_rate, 4),
            "replicate_rate_spread": round(spread, 4),
            "interpretation": "descriptive event-weighted total; repeated rows make a Wilson CI invalid",
        },
        "pooled_schema_violations": {
            "n_violations": sv_k,
            "n_total": sv_n,
            "rate": round(sv_k / sv_n, 4) if sv_n else 0.0,
            "interpretation": "descriptive event-weighted total; repeated rows make a Wilson CI invalid",
        },
        "pooled_adjusted_contradictions": {
            "note": "counts out-of-schema map labels as disagreements -- sensitivity upper count",
            "n": adj_k,
            "rate": round(adj_k / pooled_n, 4) if pooled_n else 0.0,
            "interpretation": "descriptive event-weighted total; repeated rows make a Wilson CI invalid",
        },
        "shared_row_cohort": {
            "definition": "rows admitted in every replicate; each distinct row counted once",
            "n_rows": common_n,
            "ever_contradicted": {
                "n": n_ever,
                "rate": round(n_ever / common_n, 4) if common_n else 0.0,
                "ci_lower": round(common_ever_lo, 4),
                "ci_upper": round(common_ever_hi, 4),
            },
            "contradicted_in_every_replicate": {
                "n": n_always,
                "rate": round(n_always / common_n, 4) if common_n else 0.0,
                "ci_lower": round(common_always_lo, 4),
                "ci_upper": round(common_always_hi, 4),
            },
            "caveat": "Wilson intervals treat distinct rows as independent draws from a target population; for this fixed benchmark the observed proportions are exact.",
        },
        "persistence": {
            "n_rows_admitted_in_all_replicates": len(always_admitted),
            "histogram_replicates_contradicted": dict(sorted(persistence.items())),
            "n_ever_contradicted": n_ever,
            "n_always_contradicted": n_always,
            "systematic_fraction": round(n_always / n_ever, 4) if n_ever else 0.0,
            "always_contradicted_row_ids": persistent_rows[:100],
        },
    }


def print_domain_summary(res):
    if "error" in res:
        print(f"  ERROR: {res['error']}")
        return
    p, s, a, per = res["pooled"], res["pooled_schema_violations"], \
        res["pooled_adjusted_contradictions"], res["persistence"]
    print(f"\n  {res['domain'].upper()}  ({res['n_replicates']} replicates)")
    print(f"    Event-weighted disagreement (descriptive): "
          f"{p['n_contradictions']}/{p['n_filter_positive']} ({p['rate']:.2%}); "
          "no binomial CI because rows recur across replicates")
    cohort = res["shared_row_cohort"]
    always = cohort["contradicted_in_every_replicate"]
    print(f"    Shared-row cohort: {always['n']}/{cohort['n_rows']} meet the "
          f"disagreement rule in every replicate ({always['rate']:.2%}; row-level Wilson CI "
          f"[{always['ci_lower']:.2%}, {always['ci_upper']:.2%}])")
    rate_strs = ", ".join("{:.2%}".format(r["rate"]) for r in res["per_replicate"])
    print(f"    Per-replicate rates:  [{rate_strs}]  "
          f"(spread {p['replicate_rate_spread']:.2%})")
    print(f"    Schema violations:    {s['n_violations']}/{s['n_total']} ({s['rate']:.2%})")
    print(f"    Schema-boundary sensitivity upper count: "
          f"{a['n']}/{p['n_filter_positive']} ({a['rate']:.2%})")
    if res["n_replicates"] < 2:
        print("    Persistence: NOT MEANINGFUL with 1 replicate -- every observed "
              "disagreement trivially occurs in 'every' run. Needs >=2.")
    else:
        print(f"    Persistence: {per['n_always_contradicted']}/{per['n_ever_contradicted']} "
              f"ever-disagreeing rows meet the disagreement rule in EVERY replicate "
              f"({per['systematic_fraction']:.1%} repeat in this setup)")


def main():
    parser = argparse.ArgumentParser(description="RQ3: cross-operator criterion disagreement")
    parser.add_argument("--domain", choices=["reviews", "abstracts"])
    parser.add_argument("--n-replicates", type=int, default=RQ_CONFIGS["rq3"]["n_runs"])
    parser.add_argument("--limit", type=int, default=None, help="first N rows (pilot)")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--aggregate-only", action="store_true")
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--batch-delay", type=float, default=None,
                        help="override inter-batch sleep in seconds (runner default 12). "
                             "This changes run timing; output equivalence across delay "
                             "settings has not been established.")
    parser.add_argument("--patient", action="store_true",
                        help="exponential backoff up to 15 min per retry for ~8h total, "
                             "so an overnight run can sleep through a daily limit reset.")
    args = parser.parse_args()

    domains = [args.domain] if args.domain else ["reviews", "abstracts"]
    n_rows = args.limit or 500
    tag = run_tag(args.limit)

    print("=" * 68)
    print("OperatorBench -- RQ3: Cross-operator criterion disagreement")
    print(f"Domains: {', '.join(domains)}   Replicates: {args.n_replicates}")
    print(f"Rows: {args.limit or 'all (500)'}")
    if args.limit:
        print("PILOT MODE -- results are namespaced separately from the full run.")
    print("=" * 68)

    if not args.aggregate_only:
        preflight()
        secs, usd = estimate_run(domains, args.n_replicates, n_rows, args.batch_delay)
        print(f"\nEstimated wall time: ~{fmt_duration(secs)}")
        print(f"Estimated API cost:  ~${usd:.2f}")
        if not args.yes:
            if input("\nProceed? [y/N] ").strip().lower() != "y":
                print("Aborted.")
                return

        interceptor = LLMInterceptor(log_dir=ROOT / "logs" / "llm_calls", tag="rq3")
        interceptor.install()

        runner = PipelineRunner(
            model=BACKENDS["hosted"]["model"],
            backend="hosted",
            log_dir=ROOT / "logs",
            cache_path=ROOT / "cache.db",
            cache_enabled=False,
            batch_delay_seconds=args.batch_delay,
            patient=args.patient,
        )

        started = time.time()
        for domain in domains:
            df = load_domain(domain, limit=args.limit)
            print(f"\n{'=' * 68}\nDomain: {domain}  ({len(df)} rows)\n{'=' * 68}")
            for r in range(1, args.n_replicates + 1):
                print(f"\n--- {domain} replicate {r}/{args.n_replicates} "
                      f"(elapsed {fmt_duration(time.time() - started)}) ---")
                run_replicate(runner, df, domain, r, tag=tag, force=args.force)

        interceptor.uninstall()
        s = interceptor.summary()
        print(f"\nLLM calls: {s['total_calls']}  "
              f"prompt tokens: {s['total_prompt_tokens']}  "
              f"completion tokens: {s['total_completion_tokens']}")

    print(f"\n{'=' * 68}\nRQ3 RESULTS\n{'=' * 68}")
    all_results = {}
    for domain in domains:
        res = aggregate_domain(domain, args.n_replicates, n_rows, tag=tag)
        all_results[domain] = res
        print_domain_summary(res)

    out = ROOT / "results" / f"rq3_{tag}summary.json"
    out.write_text(json.dumps(all_results, indent=2, default=str))
    print(f"\nSaved -> {out}")

    if len(all_results) == 2 and all("error" not in r for r in all_results.values()):
        rv = all_results["reviews"]["pooled"]["rate"]
        ab = all_results["abstracts"]["pooled"]["rate"]
        if rv > 0:
            print(f"\nSetup comparison: the abstracts rate is {ab / rv:.1f}x the reviews rate.")
            print("The setups differ in corpus, task, prompts, and label set; this is not a domain effect.")


if __name__ == "__main__":
    main()
