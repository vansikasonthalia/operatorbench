"""
OperatorBench — Experiment 02: Diagnosing the Consistency Finding
=================================================================
Experiment 01 found 100% consistency across 5 runs of AI_CLASSIFY.
This experiment diagnoses WHY:

Hypothesis A: Snowflake caches results (same query = same result, no LLM call)
Hypothesis B: Cortex AI Functions uses temperature=0 (deterministic decoding)
Hypothesis C: The query was too easy (unambiguous inputs)

We test all three simultaneously:
  Test 2a: Cache-busting via timestamp injection in prompt
  Test 2b: Ambiguous inputs (borderline cases humans would disagree on)
  Test 2c: Larger label space (5 categories instead of 3)

If 2a shows inconsistency -> caching was the issue in Exp 01
If 2b shows inconsistency -> query difficulty was the issue
If all stay consistent -> Cortex likely uses temperature=0 (publishable finding)

Date: April 2026
"""

import os
import json
import time
import random
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

N_RUNS = 5
N_ROWS = 20
OUTPUT_FILE = "results/experiment_02_diagnosis.json"

def get_connection():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

def parse_label(raw_label):
    """Parse AI_CLASSIFY output regardless of format."""
    if isinstance(raw_label, str):
        try:
            parsed = json.loads(raw_label)
            return parsed.get("label", raw_label)
        except json.JSONDecodeError:
            return raw_label
    elif isinstance(raw_label, dict):
        return raw_label.get("label", str(raw_label))
    return str(raw_label)

def run_query(conn, query, run_id, test_name):
    """Execute a query and return parsed results."""
    cursor = conn.cursor()
    start = time.time()
    cursor.execute(query)
    rows = cursor.fetchall()
    elapsed = time.time() - start
    cursor.close()
    print(f"  [{test_name}] Run {run_id}: {len(rows)} rows in {elapsed:.2f}s")
    return rows, elapsed

def measure_consistency(all_runs_labels):
    """Given list of dicts {key: label} per run, compute consistency stats."""
    if not all_runs_labels:
        return [], {}
    keys = list(all_runs_labels[0].keys())
    row_stats = []
    for key in keys:
        labels = [run[key] for run in all_runs_labels if key in run]
        modal = max(set(labels), key=labels.count)
        row_stats.append({
            "key": key,
            "labels": labels,
            "consistent": len(set(labels)) == 1,
            "n_unique": len(set(labels)),
            "modal": modal
        })
    n = len(row_stats)
    n_consistent = sum(1 for r in row_stats if r["consistent"])
    return row_stats, {
        "n_rows": n,
        "n_consistent": n_consistent,
        "consistency_rate": round(n_consistent / n, 4) if n > 0 else 0,
        "n_inconsistent": n - n_consistent
    }

# ── Test 2a: Cache busting ─────────────────────────────────────────────────────

def run_test_2a(conn):
    """
    Inject a unique random seed string into the prompt each run.
    If Snowflake caches by query text, different query strings bypass cache.
    Same labels across runs -> not a caching issue.
    """
    print("\n[Test 2a] Cache busting — injecting unique prompt per run")
    all_runs = []

    for i in range(1, N_RUNS + 1):
        # unique salt per run forces a different query string
        salt = f"run_{i}_{random.randint(10000,99999)}"
        query = f"""
        SELECT
            O_ORDERKEY,
            AI_CLASSIFY(
                O_COMMENT || ' [eval_id:{salt}]',
                ['urgent and critical', 'standard processing', 'low priority']
            ) AS label
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
        LIMIT {N_ROWS}
        """
        rows, _ = run_query(conn, query, i, "2a-cache-bust")
        run_results = {row[0]: parse_label(row[1]) for row in rows}
        all_runs.append(run_results)
        time.sleep(1)

    return measure_consistency(all_runs)

# ── Test 2b: Ambiguous inputs ──────────────────────────────────────────────────

def run_test_2b(conn):
    """
    Use genuinely ambiguous texts — borderline cases where the correct
    label is unclear. If Cortex is still consistent, it's not query difficulty.
    We construct ambiguous prompts directly rather than relying on TPCH data.
    """
    print("\n[Test 2b] Ambiguous inputs — borderline classification cases")

    # These are intentionally ambiguous — a human would reasonably disagree
    ambiguous_texts = [
        ("The delivery was okay, not great but not terrible either", ["positive", "negative", "neutral"]),
        ("Fast shipping but product arrived slightly damaged", ["positive", "negative", "neutral"]),
        ("Expected better quality for the price point", ["positive", "negative", "neutral"]),
        ("It does what it says but nothing more", ["positive", "negative", "neutral"]),
        ("The customer service resolved my issue eventually", ["positive", "negative", "neutral"]),
    ]

    all_runs = []

    for i in range(1, N_RUNS + 1):
        run_results = {}
        for idx, (text, labels) in enumerate(ambiguous_texts):
            label_list = str(labels).replace('"', "'")
            query = f"""
            SELECT AI_CLASSIFY(
                '{text}',
                {label_list}
            ) AS label
            """
            cursor = conn.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            cursor.close()
            run_results[f"text_{idx}"] = parse_label(row[0])
        all_runs.append(run_results)
        print(f"  [2b-ambiguous] Run {i}: {len(run_results)} texts classified")
        time.sleep(1)

    row_stats, aggregate = measure_consistency(all_runs)

    # Add the original text back for readability
    for stat in row_stats:
        idx = int(stat["key"].split("_")[1])
        stat["text"] = ambiguous_texts[idx][0]

    return row_stats, aggregate

# ── Test 2c: Larger label space ────────────────────────────────────────────────

def run_test_2c(conn):
    """
    Use 5 fine-grained categories instead of 3.
    More categories = more borderline decisions = more opportunity for flip.
    """
    print("\n[Test 2c] Larger label space — 5 categories")
    all_runs = []

    for i in range(1, N_RUNS + 1):
        query = f"""
        SELECT
            O_ORDERKEY,
            AI_CLASSIFY(
                O_COMMENT,
                ['extremely urgent', 'high priority', 'standard', 'low priority', 'no action needed']
            ) AS label
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
        LIMIT {N_ROWS}
        """
        rows, _ = run_query(conn, query, i, "2c-5labels")
        run_results = {row[0]: parse_label(row[1]) for row in rows}
        all_runs.append(run_results)
        time.sleep(1)

    return measure_consistency(all_runs)

# ── Main ──────────────────────────────────────────────────────────────────────

def print_summary(name, row_stats, aggregate):
    print(f"\n  Consistent rows:   {aggregate['n_consistent']}/{aggregate['n_rows']} ({aggregate['consistency_rate']*100:.1f}%)")
    print(f"  Inconsistent rows: {aggregate['n_inconsistent']}/{aggregate['n_rows']}")
    if aggregate['n_inconsistent'] > 0:
        print(f"  Inconsistent cases:")
        for r in row_stats:
            if not r["consistent"]:
                text = r.get("text", r.get("key", ""))
                print(f"    {text[:60]}: {r['labels']}")

def main():
    os.makedirs("results", exist_ok=True)
    print("=" * 60)
    print("OperatorBench — Experiment 02: Diagnosing Consistency")
    print("=" * 60)

    conn = get_connection()
    print("Connected.")

    results = {}

    # Test 2a
    stats_2a, agg_2a = run_test_2a(conn)
    results["test_2a_cache_bust"] = {"aggregate": agg_2a, "rows": stats_2a}
    print("\nTest 2a summary:")
    print_summary("2a", stats_2a, agg_2a)

    # Test 2b
    stats_2b, agg_2b = run_test_2b(conn)
    results["test_2b_ambiguous"] = {"aggregate": agg_2b, "rows": stats_2b}
    print("\nTest 2b summary:")
    print_summary("2b", stats_2b, agg_2b)

    # Test 2c
    stats_2c, agg_2c = run_test_2c(conn)
    results["test_2c_larger_labels"] = {"aggregate": agg_2c, "rows": stats_2c}
    print("\nTest 2c summary:")
    print_summary("2c", stats_2c, agg_2c)

    conn.close()

    # Interpret findings
    print("\n" + "=" * 60)
    print("DIAGNOSIS")
    print("=" * 60)

    if agg_2a["consistency_rate"] < 1.0:
        print("→ Test 2a: INCONSISTENCY FOUND after cache busting.")
        print("  Conclusion: Experiment 01 results were likely cached.")
        print("  Action: All future experiments must use cache-busting.")
    else:
        print("→ Test 2a: Still 100% consistent after cache busting.")
        print("  Conclusion: Caching is NOT the explanation.")

    if agg_2b["consistency_rate"] < 1.0:
        print("→ Test 2b: INCONSISTENCY FOUND on ambiguous inputs.")
        print("  Conclusion: Query difficulty explains Experiment 01 consistency.")
        print("  Action: Use ambiguous inputs in all future experiments.")
    else:
        print("→ Test 2b: Still 100% consistent on ambiguous inputs.")
        print("  Conclusion: Query difficulty is NOT the explanation.")

    if agg_2c["consistency_rate"] < 1.0:
        print("→ Test 2c: INCONSISTENCY FOUND with larger label space.")
        print("  Conclusion: Label space size affects consistency.")
    else:
        print("→ Test 2c: Still 100% consistent with 5 labels.")

    if all(a["consistency_rate"] == 1.0 for a in [agg_2a, agg_2b, agg_2c]):
        print("\n★ ALL TESTS CONSISTENT: Strong evidence Cortex AI Functions")
        print("  uses deterministic decoding (temperature=0 or equivalent).")
        print("  This is a publishable finding: commercial semantic operators")
        print("  may sacrifice non-determinism for production reliability.")
        print("  Next step: test AI_FILTER and chained operators (RQ3).")

    # Save
    output = {
        "experiment": "02_diagnosis",
        "timestamp": datetime.now().isoformat(),
        "hypothesis": {
            "A_caching": "Same query text returns cached result",
            "B_temperature": "Cortex uses temperature=0 (deterministic)",
            "C_easy_query": "TPCH comments too unambiguous to produce variance"
        },
        "results": results
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nFull results saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()