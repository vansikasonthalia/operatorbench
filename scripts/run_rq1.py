"""
OperatorBench -- RQ1: Output consistency across repeated runs

Measures observed run-to-run output variation for individual semantic
operators. RQ3 separately measures whether the two operators agree under the
benchmark's stated criteria.

DESIGN NOTE (important for the paper's methodology section)
-----------------------------------------------------------
We deliberately do NOT reuse run_filter_map() here. That method runs sem_map
only on filter-positive rows, so the map denominator changes from run to run.
consistency_across_runs() would then silently intersect the runs, and any
measured "map flip rate" would be entangled with filter instability.

Instead each replicate runs the two operators in isolation over the SAME fixed
500-row frame:

  filter consistency : run_filter_only  on all N rows -> pass/fail per row
  map consistency    : run_map_only     on all N rows -> label per row

Both metrics therefore have a fixed denominator of N and are attributable to a
single operator. We additionally report a *pipeline-level* consistency figure
restricted to rows that passed the filter in every replicate, which is the
quantity RQ3 actually depends on.

SEED CAVEAT
-----------
PipelineRunner forwards its configured seed to LOTUS. The harness never configured a
seed for the hosted runs (BACKENDS["hosted"]["seed"] is None), so hosted
replicates are unseeded runs on the same frozen rows -- repeated executions, not
seed-controlled repeats. Report them as replicates in the paper. The local Ollama
backend was planned to pin a seed but that tier was cut before execution; RQ1
used hosted only.

Usage:
    cd C:\\Users\\vans\\OneDrive\\OperatorBench
    .venv\\Scripts\\activate
    $env:OPENAI_API_KEY = "sk-..."

    python scripts/run_rq1.py --backend hosted                 # full, both domains
    python scripts/run_rq1.py --backend hosted --limit 100     # cheap pilot first
    python scripts/run_rq1.py --backend hosted --domain reviews
    # python scripts/run_rq1.py --backend local              # local tier was cut
    python scripts/run_rq1.py --aggregate-only                 # re-aggregate saved runs

Crash safety: every replicate is written to results/rq1/ as soon as it finishes.
Re-running the same command skips replicates that already have files on disk,
so an interrupted overnight run resumes where it stopped.
"""

import os
import sys
import json
import math
import time
import argparse
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from harness import (
    PipelineRunner, LLMInterceptor,
    consistency_across_runs, schema_violation_rate, format_results_table,
)
from harness.runner import BATCH_SIZE, BATCH_DELAY_SECONDS
from harness.metrics import raw_schema_compliance
from harness.artifacts import atomic_write, validate_identity
import hashlib
from configs.pipelines import PIPELINES, BACKENDS, RQ_CONFIGS

RESULTS_DIR = ROOT / "results" / "rq1"


# ---------------------------------------------------------------------------
# Cost / time estimation -- printed before anything is spent
# ---------------------------------------------------------------------------

# Rough measured averages from the Week 1 preliminary run.
APPROX_TOKENS_PER_ROW = {"reviews": 350, "abstracts": 1100}
USD_PER_1M_INPUT_TOKENS = 0.15  # gpt-4o-mini input pricing


def backend_batch_settings(backend, delay_override=None):
    """
    Ollama has no TPM ceiling, so the 12s inter-batch sleep is pure waste
    locally -- it would add ~3.5h of sleeping to a full RQ1 run for nothing.
    Smaller batches keep an 8B model's context comfortable on long abstracts.
    Hosted settings are unchanged from Week 1.
    """
    if backend == "local":
        return {"batch_size": 5,
                "batch_delay_seconds": 0 if delay_override is None else delay_override}
    return {"batch_size": BATCH_SIZE,
            "batch_delay_seconds": (BATCH_DELAY_SECONDS if delay_override is None
                                    else delay_override)}


def estimate_run(domains, n_runs, n_rows, backend, delay_override=None):
    """Return (seconds, usd) estimate for the whole experiment."""
    cfg = backend_batch_settings(backend, delay_override)
    bs, delay = cfg["batch_size"], cfg["batch_delay_seconds"]
    # Seconds of actual inference per batch: hosted API is fast, a local 8B
    # model on CPU/consumer GPU is far slower. These are rough planning numbers.
    per_batch_compute = 2.0 if backend == "hosted" else 25.0

    total_seconds = 0.0
    total_tokens = 0
    for domain in domains:
        n_batches = math.ceil(n_rows / bs)
        # two operators per replicate, each batched with a delay between batches
        per_operator = (n_batches - 1) * delay + n_batches * per_batch_compute
        total_seconds += per_operator * 2 * n_runs
        total_tokens += APPROX_TOKENS_PER_ROW.get(domain, 500) * n_rows * 2 * n_runs
    usd = (total_tokens / 1_000_000) * USD_PER_1M_INPUT_TOKENS
    if backend == "local":
        usd = 0.0  # local inference is free but much slower per call
    return total_seconds, usd


def fmt_duration(seconds):
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    return f"{h}h {m}m" if h else f"{m}m {s}s"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def preflight(backend):
    """
    Fail in one second rather than after the confirmation prompt and a batch of
    doomed LLM calls. LiteLLM's missing-credentials error arrives buried under
    ~80 lines of nested traceback, which is a slow way to learn that a
    per-window PowerShell env var was never set.
    """
    if backend == "hosted" and not os.environ.get("OPENAI_API_KEY"):
        print("\nERROR: OPENAI_API_KEY is not set in this shell.\n")
        print("  PowerShell:  $env:OPENAI_API_KEY = \"sk-...\"")
        print("\nIt is per-window, so it must be set again in every new terminal.")
        sys.exit(1)

    if backend == "local":
        try:
            import urllib.request
            base = os.environ.get("OLLAMA_API_BASE", "http://localhost:11434")
            urllib.request.urlopen(f"{base}/api/tags", timeout=3)
        except Exception:
            print(f"\nERROR: no Ollama service reachable at {base}\n")
            print("  Start it with:  ollama serve")
            print("  See docs/local_backend_setup.md")
            sys.exit(1)


def run_tag(backend, limit):
    """
    Namespace result files by backend AND row count.

    Without the row count in the filename, a 50-row pilot and the full 500-row
    run write identical paths -- and since replicates are skipped when their
    files already exist, the full run would silently resume from pilot data and
    report a 50-row result as the headline number.
    """
    return f"{backend}_n{limit}" if limit else backend


def load_domain(domain, limit=None):
    cfg = PIPELINES[domain]
    read_kwargs = {"index_col": "row_id"}
    if domain == "abstracts":
        read_kwargs["escapechar"] = "\\"
    df = pd.read_csv(ROOT / cfg["data_file"], **read_kwargs)
    if limit:
        # The frozen, shuffled dataset makes this a stable, reproducible subset.
        # A head slice does not necessarily preserve the full sample's balance.
        df = df.iloc[:limit]
    return df


# ---------------------------------------------------------------------------
# One replicate
# ---------------------------------------------------------------------------

def write_rq1_metadata(domain, tag, replicate, filter_df, map_df, backend,
                       runner=None):
    """Write execution provenance, or an explicitly post-hoc artifact manifest.

    Existing frozen CSVs predate per-run metadata.  When they are loaded, record
    the facts recoverable from the artifacts/config and mark execution-time
    provenance unavailable rather than presenting current settings as observed
    historical settings.
    """
    cfg = PIPELINES[domain]
    run_id = f"rq1_{tag}_{domain}_rep{replicate}"
    meta_path = ROOT / "results" / f"{run_id}_meta.json"
    if runner is None and meta_path.exists():
        return meta_path

    passed = filter_df["filter_passed"].astype(str).str.lower().eq("true").sum()
    meta = {
        "run_id": run_id,
        "experiment": "RQ1 individual operator repeatability",
        "domain": domain,
        "replicate": replicate,
        "backend": backend,
        "model": runner.model if runner is not None else BACKENDS[backend]["model"],
        "filter_predicate": cfg["filter"]["predicate"],
        "map_instruction": cfg["map"]["instruction"],
        "valid_labels": sorted(cfg["map"]["valid_labels"]),
        "n_rows": len(filter_df),
        "filter_passed": int(passed),
        "map_rows": len(map_df),
        "artifacts": {
            "filter_csv": f"results/rq1/{tag}_{domain}_rep{replicate}_filter.csv",
            "map_csv": f"results/rq1/{tag}_{domain}_rep{replicate}_map.csv",
        },
    }
    if runner is None:
        meta["capture_status"] = "posthoc_artifact_manifest"
        meta["provenance_status"] = (
            "Execution-time package versions, call parameters, and provider-applied "
            "temperature were not recorded for these existing artifacts. Config "
            "fields above come from the current frozen pipeline config and are not "
            "proof of the historical request payload."
        )
    else:
        from harness.provenance import run_provenance
        meta["capture_status"] = "captured_at_execution"
        meta["provenance"] = run_provenance(
            temperature=runner._observed_temperature(),
            max_text_chars=runner.max_text_chars,
            backend=runner.backend,
            seed=runner.seed,
            cache_enabled=getattr(runner.cache, "enabled", None),
        )

    atomic_write(meta_path, json.dumps(meta, indent=2, default=str))
    return meta_path


def run_replicate(runner, df, domain, tag, replicate, force=False):
    """
    Run sem_filter and sem_map in isolation over the full frame.

    Returns (filter_df, map_df) where:
      filter_df: index row_id, column 'filter_passed' in {'True','False'}
      map_df   : index row_id, columns [<map_output_col>, '_raw_<col>']
    """
    cfg = PIPELINES[domain]
    map_col = cfg["map"]["output_col"]

    filter_path = RESULTS_DIR / f"{tag}_{domain}_rep{replicate}_filter.csv"
    map_path = RESULTS_DIR / f"{tag}_{domain}_rep{replicate}_map.csv"
    identity = runner.experiment_identity(df, domain, cfg["filter"]["predicate"],
                                          cfg["map"]["instruction"], map_col, cfg["map"]["valid_labels"])

    def load_operator(path, columns):
        manifest_path = path.with_suffix(".manifest.json")
        if not manifest_path.exists():
            raise ValueError(f"Missing execution manifest for {path.name}; historical files are for offline analysis")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        validate_identity(manifest, identity)
        if hashlib.sha256(path.read_bytes()).hexdigest() != manifest["sha256"]:
            raise ValueError(f"Artifact hash mismatch: {path.name}")
        frame = pd.read_csv(path, index_col="row_id")
        if not frame.index.is_unique or set(frame.index) != set(df.index) or not set(columns) <= set(frame.columns):
            raise ValueError(f"Incomplete rows or columns: {path.name}")
        return frame

    def save_operator(path, frame, columns):
        if not frame.index.is_unique or set(frame.index) != set(df.index) or not set(columns) <= set(frame.columns):
            raise ValueError(f"Incomplete rows: {path.name}")
        atomic_write(path, frame.to_csv(index_label="row_id"))
        atomic_write(path.with_suffix(".manifest.json"), json.dumps({
            "experiment_identity": identity, "completion_status": "complete",
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }, indent=2))

    if filter_path.exists() and map_path.exists() and not force:
        print(f"  [rep {replicate}] already on disk -- loading (use --force to redo)")
        f_df = load_operator(filter_path, ["filter_passed"])
        m_df = load_operator(map_path, [map_col, f"_raw_{map_col}"])
        write_rq1_metadata(domain, tag, replicate, f_df, m_df,
                           backend=runner.backend)
        return f_df, m_df

    run_id = f"rq1_{tag}_{domain}_rep{replicate}"

    # --- sem_filter over all rows -------------------------------------------
    # Resume is per-operator, not per-replicate. A replicate interrupted during
    # the map stage already has a complete, valid filter CSV on disk; re-running
    # it would burn 500 more calls against a 2M/day token cap for no new data.
    if filter_path.exists() and not force:
        print(f"  [rep {replicate}] filter already on disk -- skipping to map")
        f_df = load_operator(filter_path, ["filter_passed"])
    else:
        passed = runner.run_filter_only(
            df=df,
            domain=domain,
            filter_predicate=cfg["filter"]["predicate"],
            run_id=f"{run_id}_filter",
        )
        f_df = pd.DataFrame(
            {"filter_passed": [str(rid in passed.index) for rid in df.index]},
            index=df.index,
        )
        f_df.index.name = "row_id"
        save_operator(filter_path, f_df, ["filter_passed"])

    # --- sem_map over ALL rows (not just filter-positive) --------------------
    # Fixed denominator is what makes the flip rate attributable to sem_map.
    mapped = runner.run_map_only(
        df=df,
        domain=domain,
        map_instruction=cfg["map"]["instruction"],
        map_output_col=map_col,
        valid_labels=cfg["map"]["valid_labels"],
        run_id=f"{run_id}_map",
    )
    keep = [c for c in (map_col, f"_raw_{map_col}") if c in mapped.columns]
    m_df = mapped[keep].copy()
    m_df.index.name = "row_id"
    save_operator(map_path, m_df, [map_col, f"_raw_{map_col}"])

    # Capture full provenance for this execution, including isolated operators.
    write_rq1_metadata(domain, tag, replicate, f_df, m_df,
                       backend=runner.backend, runner=runner)

    print(f"  [rep {replicate}] saved -> {filter_path.name}, {map_path.name}")
    return f_df, m_df


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def aggregate_domain(domain, backend, tag, n_runs):
    """Load all replicates from disk and compute RQ1 metrics."""
    cfg = PIPELINES[domain]
    map_col = cfg["map"]["output_col"]

    filter_runs, map_runs = [], []
    for r in range(1, n_runs + 1):
        fp = RESULTS_DIR / f"{tag}_{domain}_rep{r}_filter.csv"
        mp = RESULTS_DIR / f"{tag}_{domain}_rep{r}_map.csv"
        if not (fp.exists() and mp.exists()):
            print(f"  WARNING: replicate {r} missing on disk, skipping")
            continue
        filter_df = pd.read_csv(fp, index_col="row_id")
        map_df = pd.read_csv(mp, index_col="row_id")
        write_rq1_metadata(domain, tag, r, filter_df, map_df, backend=backend)
        filter_runs.append(filter_df)
        map_runs.append(map_df)

    if len(filter_runs) < 2:
        return {"error": f"need >=2 replicates, found {len(filter_runs)}"}

    # 1. Filter operator consistency (fixed denominator = all rows)
    filter_consistency = consistency_across_runs(
        run_results=filter_runs, label_col="filter_passed", id_col="row_id"
    )

    # 2. Map operator consistency (fixed denominator = all rows)
    map_consistency = consistency_across_runs(
        run_results=map_runs, label_col=map_col, id_col="row_id"
    )

    # 3. Pipeline-level consistency: map labels restricted to rows the filter
    #    admitted in EVERY replicate. This is the population RQ3 measures over.
    always_passed = None
    for f_df in filter_runs:
        ids = set(f_df.index[f_df["filter_passed"].astype(str) == "True"])
        always_passed = ids if always_passed is None else (always_passed & ids)

    if always_passed:
        restricted = [m.loc[sorted(always_passed & set(m.index))] for m in map_runs]
        pipeline_consistency = consistency_across_runs(
            run_results=restricted, label_col=map_col, id_col="row_id"
        )
    else:
        pipeline_consistency = {"error": "no rows passed the filter in every replicate"}

    # 4. Per-replicate descriptive stability
    pass_counts = [int((f["filter_passed"].astype(str) == "True").sum()) for f in filter_runs]
    label_dists = [m[map_col].astype(str).value_counts().to_dict() for m in map_runs]
    schema_per_rep = [
        schema_violation_rate(m, map_col, cfg["map"]["valid_labels"])
        for m in map_runs
    ]
    raw_schema_per_rep = [
        raw_schema_compliance(m, f"_raw_{map_col}", cfg["map"]["valid_labels"])
        if f"_raw_{map_col}" in m else {"error": "raw outputs unavailable"}
        for m in map_runs
    ]

    return {
        "domain": domain,
        "backend": backend,
        "model": BACKENDS[backend]["model"],
        "n_replicates": len(filter_runs),
        "raw_schema_compliance_per_replicate": raw_schema_per_rep,
        "n_rows": int(len(filter_runs[0])),
        "filter_consistency": filter_consistency,
        "map_consistency": map_consistency,
        "pipeline_consistency": pipeline_consistency,
        "filter_pass_counts_per_replicate": pass_counts,
        "filter_pass_count_range": [min(pass_counts), max(pass_counts)],
        "n_always_passed": len(always_passed) if always_passed else 0,
        "map_label_distributions": label_dists,
        "schema_violations_per_replicate": [
            {"n_violations": s["n_violations"], "rate": s["rate"]} for s in schema_per_rep
        ],
    }


def print_domain_summary(res):
    if "error" in res:
        print(f"  ERROR: {res['error']}")
        return
    fc, mc, pc = res["filter_consistency"], res["map_consistency"], res["pipeline_consistency"]
    print(f"\n  {res['domain'].upper()}  ({res['n_replicates']} replicates, "
          f"{res['n_rows']} rows, {res['model']})")
    print(f"    sem_filter  flip rate: {fc['n_flipped']}/{fc['n_rows']} "
          f"({fc['flip_rate']:.2%})  95% CI [{fc['ci_lower']:.2%}, {fc['ci_upper']:.2%}]")
    print(f"    sem_map     flip rate: {mc['n_flipped']}/{mc['n_rows']} "
          f"({mc['flip_rate']:.2%})  95% CI [{mc['ci_lower']:.2%}, {mc['ci_upper']:.2%}]")
    if "error" not in pc:
        print(f"    pipeline    flip rate: {pc['n_flipped']}/{pc['n_rows']} "
              f"({pc['flip_rate']:.2%})  [rows filter-admitted in every replicate]")
    print(f"    filter pass counts per replicate: {res['filter_pass_counts_per_replicate']}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="RQ1: operator consistency across repeated runs")
    parser.add_argument("--domain", choices=["reviews", "abstracts"],
                        help="run a single domain (default: both)")
    parser.add_argument("--backend", choices=["hosted", "local"], default="hosted")
    parser.add_argument("--n-runs", type=int, default=RQ_CONFIGS["rq1"]["n_runs"],
                        help="number of replicates per domain (default 5)")
    parser.add_argument("--limit", type=int, default=None,
                        help="use only the first N rows -- for a cheap pilot")
    parser.add_argument("--force", action="store_true",
                        help="re-run replicates even if results already exist")
    parser.add_argument("--aggregate-only", action="store_true",
                        help="skip LLM calls, just re-aggregate what is on disk")
    parser.add_argument("--yes", action="store_true",
                        help="skip the cost/time confirmation prompt")
    parser.add_argument("--batch-delay", type=float, default=None,
                        help="override inter-batch sleep in seconds. Changing batch timing "
                             "can change throttling and retries; output equivalence across "
                             "delay settings has not been established.")
    parser.add_argument("--patient", action="store_true",
                        help="back off exponentially up to 15 min per retry, for up to "
                             "~8h total, instead of giving up after 90s. Use for "
                             "overnight runs that may need to sleep through a daily "
                             "account limit reset.")
    args = parser.parse_args()

    domains = [args.domain] if args.domain else ["reviews", "abstracts"]
    tag = run_tag(args.backend, args.limit)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 68)
    print("OperatorBench -- RQ1: Output consistency across repeated runs")
    print(f"Backend: {args.backend} ({BACKENDS[args.backend]['model']})")
    print(f"Domains: {', '.join(domains)}   Replicates: {args.n_runs}")
    print(f"Rows: {args.limit or 'all (500)'}   Result namespace: {tag}")
    if args.limit:
        print("PILOT MODE -- results are namespaced separately from the full run.")
    print("=" * 68)

    if not args.aggregate_only:
        preflight(args.backend)
        n_rows = args.limit or 500
        secs, usd = estimate_run(domains, args.n_runs, n_rows, args.backend, args.batch_delay)
        print(f"\nEstimated wall time: ~{fmt_duration(secs)}")
        print(f"Estimated API cost:  ~${usd:.2f}"
              + ("  (local backend -- no API cost, but slower)" if args.backend == "local" else ""))
        print("Replicates are saved as they complete; re-run to resume after a crash.")
        if not args.yes:
            reply = input("\nProceed? [y/N] ").strip().lower()
            if reply != "y":
                print("Aborted.")
                return

        interceptor = LLMInterceptor(
            log_dir=ROOT / "logs" / "llm_calls",
            tag=f"rq1_{args.backend}",
        )
        interceptor.install()

        runner = PipelineRunner(
            model=BACKENDS[args.backend]["model"],
            backend=args.backend,
            seed=BACKENDS[args.backend].get("seed"),
            log_dir=ROOT / "logs",
            cache_path=ROOT / "cache.db",
            cache_enabled=False,  # never cache experimental runs
            patient=args.patient,
            **backend_batch_settings(args.backend, args.batch_delay),
        )

        started = time.time()
        for domain in domains:
            df = load_domain(domain, limit=args.limit)
            print(f"\n{'=' * 68}\nDomain: {domain}  ({len(df)} rows)\n{'=' * 68}")
            for r in range(1, args.n_runs + 1):
                print(f"\n--- {domain} replicate {r}/{args.n_runs} "
                      f"(elapsed {fmt_duration(time.time() - started)}) ---")
                run_replicate(runner, df, domain, tag, r, force=args.force)

        interceptor.uninstall()
        s = interceptor.summary()
        print(f"\nLLM calls: {s['total_calls']}  "
              f"prompt tokens: {s['total_prompt_tokens']}  "
              f"completion tokens: {s['total_completion_tokens']}")

    # --- Aggregate -----------------------------------------------------------
    print(f"\n{'=' * 68}\nRQ1 RESULTS\n{'=' * 68}")
    all_results = {}
    for domain in domains:
        res = aggregate_domain(domain, args.backend, tag, args.n_runs)
        all_results[domain] = res
        print_domain_summary(res)

    out = ROOT / "results" / f"rq1_{tag}_summary.json"
    out.write_text(json.dumps(all_results, indent=2, default=str))
    print(f"\nSaved -> {out}")

    print(f"\n{'=' * 68}")
    print("Interpretation guide:")
    print("  Low filter AND map flip rates  -> operators are individually stable.")
    print("  RQ3 measures cross-operator criterion disagreement for its stated")
    print("  predicate and instruction; stability alone does not explain it.")
    print("=" * 68)


if __name__ == "__main__":
    main()
