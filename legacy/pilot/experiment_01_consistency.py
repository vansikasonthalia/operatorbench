"""
OperatorBench — Experiment 01: Output Consistency (RQ1)
=======================================================
Runs AI_CLASSIFY on TPCH_SF1.ORDERS data 5 times and measures
whether outputs are identical across runs.

This is the first pilot experiment. We are asking:
"Does AI_CLASSIFY return the same label for the same row
across repeated executions?"

Dataset: Snowflake sample data TPCH_SF1.ORDERS
Operator: AI_CLASSIFY (Snowflake Cortex AI Functions)
N runs: 5
Date: April 2026
"""

import os
import json
import time
import hashlib
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

N_RUNS = 5
N_ROWS = 20          # small pilot — 20 rows, 5 runs = 100 operator calls
OUTPUT_FILE = "results/experiment_01_consistency.json"

# ── Connect ───────────────────────────────────────────────────────────────────

def get_connection():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

# ── Query ─────────────────────────────────────────────────────────────────────

# We use O_COMMENT (free text) from ORDERS and ask Cortex to classify
# the order priority sentiment. This is a realistic semantic classification
# task — exactly what enterprises do with AI_CLASSIFY in production.

CLASSIFY_QUERY = f"""
SELECT
    O_ORDERKEY,
    O_COMMENT,
    AI_CLASSIFY(
        O_COMMENT,
        ['urgent and critical', 'standard processing', 'low priority']
    ) AS priority_label
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
LIMIT {N_ROWS}
"""

# ── Run experiment ────────────────────────────────────────────────────────────

def run_single(conn, run_id):
    """Execute the query once and return results as a dict keyed by order key."""
    cursor = conn.cursor()
    start = time.time()
    cursor.execute(CLASSIFY_QUERY)
    rows = cursor.fetchall()
    elapsed = time.time() - start
    cursor.close()

    results = {}
    for row in rows:
        order_key = row[0]
        comment = row[1]
        raw_label = row[2]

        # AI_CLASSIFY returns JSON like {"label": "urgent and critical"}
        # Handle both string and dict responses
        if isinstance(raw_label, str):
            try:
                parsed = json.loads(raw_label)
                label = parsed.get("label", raw_label)
            except json.JSONDecodeError:
                label = raw_label
        elif isinstance(raw_label, dict):
            label = raw_label.get("label", str(raw_label))
        else:
            label = str(raw_label)

        results[order_key] = {
            "comment": comment,
            "label": label,
            "raw": str(raw_label)
        }

    print(f"  Run {run_id} complete: {len(rows)} rows in {elapsed:.2f}s")
    return results, elapsed

def measure_consistency(all_runs):
    """
    For each row, check if all 5 runs returned the same label.
    Returns per-row consistency and aggregate stats.
    """
    order_keys = list(all_runs[0].keys())
    row_stats = []

    for key in order_keys:
        labels = [run[key]["label"] for run in all_runs if key in run]
        modal_label = max(set(labels), key=labels.count)
        modal_count = labels.count(modal_label)
        is_consistent = (modal_count == N_RUNS)
        unique_labels = list(set(labels))

        row_stats.append({
            "order_key": key,
            "comment_snippet": all_runs[0][key]["comment"][:80],
            "labels_across_runs": labels,
            "modal_label": modal_label,
            "modal_count": modal_count,
            "is_consistent": is_consistent,
            "unique_labels_seen": unique_labels,
            "n_unique": len(unique_labels)
        })

    # Aggregate
    n_rows = len(row_stats)
    n_consistent = sum(1 for r in row_stats if r["is_consistent"])
    consistency_rate = n_consistent / n_rows if n_rows > 0 else 0

    return row_stats, {
        "n_rows": n_rows,
        "n_runs": N_RUNS,
        "n_consistent_rows": n_consistent,
        "n_inconsistent_rows": n_rows - n_consistent,
        "consistency_rate": round(consistency_rate, 4),
        "inconsistency_rate": round(1 - consistency_rate, 4)
    }

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs("results", exist_ok=True)

    print("=" * 60)
    print("OperatorBench — Experiment 01: AI_CLASSIFY Consistency")
    print(f"Query: AI_CLASSIFY on TPCH_SF1.ORDERS.O_COMMENT")
    print(f"Labels: ['urgent and critical', 'standard processing', 'low priority']")
    print(f"Rows: {N_ROWS}  |  Runs: {N_RUNS}  |  Total calls: {N_ROWS * N_RUNS}")
    print("=" * 60)

    conn = get_connection()
    print(f"\nConnected. Starting {N_RUNS} runs...\n")

    all_runs = []
    run_times = []

    for i in range(1, N_RUNS + 1):
        results, elapsed = run_single(conn, i)
        all_runs.append(results)
        run_times.append(elapsed)
        time.sleep(1)  # small pause between runs

    conn.close()

    print("\nComputing consistency metrics...")
    row_stats, aggregate = measure_consistency(all_runs)

    # Print summary
    print("\n" + "=" * 60)
    print("RESULTS SUMMARY")
    print("=" * 60)
    print(f"Total rows tested:       {aggregate['n_rows']}")
    print(f"Total operator calls:    {aggregate['n_rows'] * aggregate['n_runs']}")
    print(f"Consistent rows:         {aggregate['n_consistent_rows']} ({aggregate['consistency_rate']*100:.1f}%)")
    print(f"Inconsistent rows:       {aggregate['n_inconsistent_rows']} ({aggregate['inconsistency_rate']*100:.1f}%)")
    print(f"Avg run time:            {sum(run_times)/len(run_times):.2f}s")

    print("\nPer-row breakdown (inconsistent only):")
    inconsistent = [r for r in row_stats if not r["is_consistent"]]
    if inconsistent:
        for r in inconsistent:
            print(f"  OrderKey {r['order_key']}: {r['labels_across_runs']}")
            print(f"    Comment: \"{r['comment_snippet']}...\"")
    else:
        print("  None — all rows consistent across all 5 runs.")
        print("  (This is a finding: AI_CLASSIFY may be deterministic for this query type)")

    # Save full results
    output = {
        "experiment": "01_consistency",
        "timestamp": datetime.now().isoformat(),
        "config": {
            "n_rows": N_ROWS,
            "n_runs": N_RUNS,
            "operator": "AI_CLASSIFY",
            "system": "Snowflake Cortex AI Functions",
            "database": "SNOWFLAKE_SAMPLE_DATA.TPCH_SF1",
            "labels": ["urgent and critical", "standard processing", "low priority"]
        },
        "aggregate": aggregate,
        "run_times_seconds": run_times,
        "row_stats": row_stats
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nFull results saved to: {OUTPUT_FILE}")
    print("\nNext step: commit results to GitHub and run Experiment 02 (schema violation)")

if __name__ == "__main__":
    main()