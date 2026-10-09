"""
OperatorBench -- RQ3 Preliminary Run

Cross-operator criterion-disagreement rate on full 500-row datasets.
Single run per domain, hosted backend (gpt-4o-mini).

This gives first real numbers for the headline RQ.

Usage:
    cd C:\\Users\\vans\\OneDrive\\OperatorBench
    .venv\\Scripts\\activate
    $env:OPENAI_API_KEY = "sk-..."
    python scripts/run_rq3_preliminary.py
"""

import sys
import json
import argparse
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from harness import (
    PipelineRunner, LLMInterceptor,
    contradiction_rate, schema_violation_rate, format_results_table,
)
from configs.pipelines import PIPELINES, BACKENDS


def run_rq3(domains=None):
    domains = domains or ["reviews", "abstracts"]
    print("=" * 60)
    print(f"OperatorBench -- RQ3 Preliminary (full 500 rows)")
    print(f"Domains: {', '.join(domains)}")
    print("Cross-operator criterion-disagreement rate")
    print("=" * 60)

    # Set up interceptor to capture real LLM prompts
    interceptor = LLMInterceptor(
        log_dir=ROOT / "logs" / "llm_calls",
        tag="rq3_preliminary",
    )
    interceptor.install()

    runner = PipelineRunner(
        model=BACKENDS["hosted"]["model"],
        backend="hosted",
        log_dir=ROOT / "logs",
        cache_path=ROOT / "cache.db",
        cache_enabled=False,  # no caching for real experiments
    )

    all_results = {}

    for domain in domains:
        print(f"\n{'='*60}")
        print(f"Domain: {domain}")
        print(f"{'='*60}")

        cfg = PIPELINES[domain]

        # Load full dataset
        read_kwargs = {"index_col": "row_id"}
        if domain == "abstracts":
            read_kwargs["escapechar"] = "\\"

        df = pd.read_csv(ROOT / cfg["data_file"], **read_kwargs)
        print(f"Loaded {len(df)} rows")

        # Run filter -> map pipeline
        results = runner.run_filter_map(
            df=df,
            domain=domain,
            filter_predicate=cfg["filter"]["predicate"],
            map_instruction=cfg["map"]["instruction"],
            map_output_col=cfg["map"]["output_col"],
            valid_labels=cfg["map"]["valid_labels"],
            run_id=f"rq3_preliminary_{domain}",
        )

        map_df = results["map_results"]

        # RQ3: Contradiction rate
        contra = contradiction_rate(
            map_df=map_df,
            map_col=cfg["map"]["output_col"],
            is_contradiction=cfg["contradiction"]["is_contradiction"],
        )
        print(f"\n{format_results_table(contra, f'RQ3 Contradiction -- {domain}')}")

        # RQ2 bonus: Schema violation rate (free since we already have the data)
        schema = schema_violation_rate(
            map_df=map_df,
            map_col=cfg["map"]["output_col"],
            valid_labels=cfg["map"]["valid_labels"],
        )
        print(f"\n{format_results_table(schema, f'RQ2 Schema Violations -- {domain}')}")

        all_results[domain] = {
            "filter_positive": results["filter_positive_count"],
            "total_rows": results["total_rows"],
            "filter_rate": round(results["filter_positive_count"] / results["total_rows"], 4),
            "contradiction": {k: v for k, v in contra.items() if k != "contradicted_rows"},
            "schema_violations": {k: v for k, v in schema.items() if k != "violation_examples"},
            "contradiction_details": contra["contradicted_rows"],
            "violation_details": schema["violation_examples"],
        }

        # Save per-domain results immediately (so we don't lose data if next domain crashes)
        per_domain_file = ROOT / "results" / f"rq3_preliminary_{domain}.json"
        per_domain_file.write_text(json.dumps(all_results[domain], indent=2, default=str))
        print(f"  Saved {domain} metrics to {per_domain_file}")

    # Interceptor summary
    interceptor.uninstall()
    llm_summary = interceptor.summary()
    print(f"\n{'='*60}")
    print(f"LLM Call Summary")
    print(f"{'='*60}")
    print(f"  Total LLM calls: {llm_summary['total_calls']}")
    print(f"  Unique prompts: {llm_summary['unique_prompts']}")
    print(f"  Prompt tokens: {llm_summary['total_prompt_tokens']}")
    print(f"  Completion tokens: {llm_summary['total_completion_tokens']}")

    # Save prompt templates for paper appendix
    templates = interceptor.get_prompt_templates()
    if templates:
        templates_dir = ROOT / "logs" / "prompt_templates"
        templates_dir.mkdir(parents=True, exist_ok=True)
        for i, t in enumerate(templates):
            (templates_dir / f"template_{i}.json").write_text(t)
        print(f"  Saved {len(templates)} unique prompt templates to logs/prompt_templates/")

    # Save combined results
    results_file = ROOT / "results" / "rq3_preliminary_summary.json"
    results_file.parent.mkdir(exist_ok=True)
    results_file.write_text(json.dumps(all_results, indent=2, default=str))
    print(f"\nResults saved to {results_file}")

    # Print final summary
    print(f"\n{'='*60}")
    print("PRELIMINARY RQ3 RESULTS")
    print(f"{'='*60}")
    for domain, res in all_results.items():
        c = res["contradiction"]
        print(f"\n  {domain.upper()}:")
        print(f"    Filter pass rate: {res['filter_positive']}/{res['total_rows']} ({res['filter_rate']:.1%})")
        print(f"    Contradiction rate: {c['n_contradictions']}/{c['n_filter_positive']} ({c['rate']:.1%})")
        print(f"    95% Wilson CI: [{c['ci_lower']:.1%}, {c['ci_upper']:.1%}]")
        sv = res["schema_violations"]
        print(f"    Schema violations: {sv['n_violations']}/{sv['n_total']} ({sv['rate']:.1%})")

    print(f"\n{'='*60}")
    print("Done. These are preliminary single-run numbers.")
    print("Week 2 will add repeated runs for RQ1 consistency.")
    print(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--domain", choices=["reviews", "abstracts"],
                        help="Run only one domain (skip the other)")
    args = parser.parse_args()
    domains = [args.domain] if args.domain else None
    run_rq3(domains=domains)
