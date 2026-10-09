"""
OperatorBench — Verified Experiment Suite
==========================================
Full rewrite using verified Snowflake Cortex AI Functions syntax.

Verified signatures (from DESCRIBE FUNCTION + live testing, April 2026):

  AI_CLASSIFY(VARCHAR, ARRAY [, OBJECT, BOOLEAN])
    Returns: OBJECT — auto-unwrapped in SELECT to {"labels": [...]}
    Parse:   result["labels"][0] for single-label mode

  AI_FILTER(VARCHAR [, OBJECT, BOOLEAN])
    CRITICAL: AI_FILTER(VARCHAR, VARCHAR) does NOT exist.
    Correct:  AI_FILTER(PROMPT('condition {0}', col)) or single composed string
    Returns:  OBJECT — auto-unwrapped to 1/0 in SELECT
    Parse:    bool(result) or result["value"]

  AI_EXTRACT(VARCHAR, VARIANT [, VARCHAR, OBJECT])
    Second arg must be ARRAY of field names (VARIANT), not a string
    Returns:  {"response": {...}, "error": null}
    Parse:    result["response"]

  ALL functions expose :error field — usable for RQ2 / RQ5 validation.

Experiments:
  01 — AI_CLASSIFY consistency (RQ1) — corrected label parsing
  02 — AI_CLASSIFY schema violation (RQ2) — out-of-label outputs + error field
  03 — AI_FILTER correct syntax test (RQ1 for AI_FILTER)
  04 — Cross-operator coherence (RQ3) — AI_FILTER → AI_CLASSIFY chain
  05 — Context sensitivity drift (RQ4) — minimal vs extended schema
  06 — AI_EXTRACT reliability (RQ1 + RQ2 for extraction)

Run experiments individually:
  python experiments.py --exp 01
  python experiments.py --exp all
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

# ── Connection ─────────────────────────────────────────────────────────────────

def get_conn():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database='SNOWFLAKE_SAMPLE_DATA',
        schema='TPCH_SF1'
    )

def execute(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    return rows

# ── Parsers ────────────────────────────────────────────────────────────────────

def parse_classify(raw):
    """
    AI_CLASSIFY returns OBJECT auto-unwrapped to {"labels": [...]} in Python.
    Single-label mode: labels list has one item.
    """
    if isinstance(raw, dict):
        labels = raw.get("labels", [])
        error = raw.get("error")
        return labels[0] if labels else None, error
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            labels = parsed.get("labels", [])
            error = parsed.get("error")
            return labels[0] if labels else None, error
        except:
            return raw, None
    return str(raw), None

def parse_filter(raw):
    """
    AI_FILTER returns OBJECT auto-unwrapped to 1/0 (bool) in SELECT.
    Access :error via separate call if needed.
    """
    if isinstance(raw, bool):
        return raw, None
    if isinstance(raw, int):
        return bool(raw), None
    if isinstance(raw, dict):
        return bool(raw.get("value")), raw.get("error")
    return bool(raw), None

def parse_extract(raw):
    """
    AI_EXTRACT returns {"response": {...}, "error": null}
    """
    if isinstance(raw, dict):
        return raw.get("response", {}), raw.get("error")
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed.get("response", {}), parsed.get("error")
        except:
            return {}, None
    return {}, None

def save(data, filename):
    os.makedirs("results", exist_ok=True)
    with open(f"results/{filename}", "w") as f:
        json.dump(data, f, indent=2)
    print(f"  Saved: results/{filename}")

# ── Experiment 01: AI_CLASSIFY Consistency (RQ1) ──────────────────────────────

def exp_01(conn):
    """
    RQ1: How stable are AI_CLASSIFY outputs across N runs on identical inputs?
    Corrected from pilot: now correctly parses {"labels": [...]} return format.
    """
    print("\n" + "="*60)
    print("Experiment 01: AI_CLASSIFY Consistency (RQ1)")
    print("="*60)

    N_ROWS = 50
    N_RUNS = 10
    LABELS = ['urgent order', 'standard order', 'low priority order']

    # Fetch rows once
    rows = execute(conn, f"""
        SELECT O_ORDERKEY, O_COMMENT
        FROM ORDERS
        WHERE LENGTH(O_COMMENT) > 20
        LIMIT {N_ROWS}
    """)
    print(f"Fetched {len(rows)} rows. Running {N_RUNS} classification runs...")

    all_runs = []  # list of dicts: {order_key: label}

    for run_id in range(1, N_RUNS + 1):
        run_labels = {}
        run_errors = {}

        for order_key, comment in rows:
            safe = comment.replace("'", "''")
            labels_sql = str(LABELS).replace('"', "'")
            result = execute(conn, f"""
                SELECT AI_CLASSIFY('{safe}', {labels_sql})
            """)
            label, error = parse_classify(result[0][0])
            run_labels[order_key] = label
            if error:
                run_errors[order_key] = error

        all_runs.append(run_labels)
        n_errors = len(run_errors)
        print(f"  Run {run_id}/{N_RUNS} complete. Errors: {n_errors}")
        time.sleep(0.5)

    # Compute consistency
    row_stats = []
    for order_key, comment in rows:
        labels_across_runs = [run[order_key] for run in all_runs]
        unique = list(set(labels_across_runs))
        modal = max(set(labels_across_runs), key=labels_across_runs.count)
        modal_count = labels_across_runs.count(modal)

        row_stats.append({
            "order_key": order_key,
            "comment_snippet": comment[:80],
            "labels_across_runs": labels_across_runs,
            "n_unique_labels": len(unique),
            "modal_label": modal,
            "modal_count": modal_count,
            "is_fully_consistent": len(unique) == 1
        })

    n = len(row_stats)
    n_consistent = sum(1 for r in row_stats if r["is_fully_consistent"])

    print(f"\nRESULTS:")
    print(f"  Rows tested:      {n}")
    print(f"  Runs per row:     {N_RUNS}")
    print(f"  Total calls:      {n * N_RUNS}")
    print(f"  Consistent rows:  {n_consistent}/{n} ({n_consistent/n*100:.1f}%)")
    print(f"  Inconsistent:     {n-n_consistent}/{n} ({(n-n_consistent)/n*100:.1f}%)")

    inconsistent = [r for r in row_stats if not r["is_fully_consistent"]]
    if inconsistent:
        print(f"\n  Inconsistent cases:")
        for r in inconsistent[:5]:
            print(f"    OrderKey {r['order_key']}: {r['labels_across_runs']}")
    else:
        print(f"\n  All rows fully consistent across {N_RUNS} runs.")
        print(f"  Finding: AI_CLASSIFY appears deterministic on Snowflake Cortex.")

    output = {
        "experiment": "01_classify_consistency",
        "timestamp": datetime.now().isoformat(),
        "config": {"n_rows": N_ROWS, "n_runs": N_RUNS, "labels": LABELS},
        "aggregate": {
            "n_rows": n, "n_runs": N_RUNS,
            "n_consistent": n_consistent,
            "consistency_rate": round(n_consistent/n, 4)
        },
        "row_stats": row_stats
    }
    save(output, "exp01_classify_consistency.json")
    return output

# ── Experiment 02: Schema Violation (RQ2) ─────────────────────────────────────

def exp_02(conn):
    """
    RQ2: Does AI_CLASSIFY ever return a label outside the declared label set?
    Also checks the :error field for failure signals.
    Tests three label set sizes: small (2), medium (4), large (8).
    """
    print("\n" + "="*60)
    print("Experiment 02: AI_CLASSIFY Schema Violation (RQ2)")
    print("="*60)

    N_ROWS = 100

    LABEL_SETS = {
        "small_2": ['positive', 'negative'],
        "medium_4": ['billing issue', 'delivery issue', 'product issue', 'other'],
        "large_8": [
            'urgent billing', 'billing dispute', 'late delivery',
            'damaged goods', 'wrong item', 'quality complaint',
            'general inquiry', 'positive feedback'
        ]
    }

    rows = execute(conn, f"""
        SELECT O_ORDERKEY, O_COMMENT
        FROM ORDERS
        WHERE LENGTH(O_COMMENT) > 20
        LIMIT {N_ROWS}
    """)
    print(f"Fetched {len(rows)} rows.")

    results = {}

    for set_name, labels in LABEL_SETS.items():
        print(f"\n  Testing label set '{set_name}' ({len(labels)} labels)...")
        violations = []
        errors = []
        valid = 0

        for order_key, comment in rows:
            safe = comment.replace("'", "''")
            labels_sql = str(labels).replace('"', "'")
            result = execute(conn, f"""
                SELECT AI_CLASSIFY('{safe}', {labels_sql})
            """)
            label, error = parse_classify(result[0][0])

            if error:
                errors.append({"order_key": order_key, "error": error})
            elif label not in labels:
                violations.append({
                    "order_key": order_key,
                    "returned_label": label,
                    "declared_labels": labels,
                    "comment_snippet": comment[:80]
                })
            else:
                valid += 1

        n = len(rows)
        print(f"    Valid labels:    {valid}/{n}")
        print(f"    Violations:      {len(violations)}/{n} "
              f"(label outside declared set)")
        print(f"    Errors:          {len(errors)}/{n}")

        if violations:
            print(f"    Example violations:")
            for v in violations[:3]:
                print(f"      OrderKey {v['order_key']}: returned '{v['returned_label']}'")

        results[set_name] = {
            "n_rows": n,
            "n_labels": len(labels),
            "labels": labels,
            "n_valid": valid,
            "n_violations": len(violations),
            "n_errors": len(errors),
            "violation_rate": round(len(violations)/n, 4),
            "error_rate": round(len(errors)/n, 4),
            "violations": violations,
            "errors": errors
        }

    total_violations = sum(r["n_violations"] for r in results.values())
    print(f"\nRESULTS:")
    for name, r in results.items():
        print(f"  {name}: {r['n_violations']} violations, "
              f"{r['n_errors']} errors out of {r['n_rows']} rows")

    if total_violations > 0:
        print(f"\n  ★ SCHEMA VIOLATIONS FOUND: AI_CLASSIFY returned labels")
        print(f"    outside the declared label set.")
    else:
        print(f"\n  → No schema violations. AI_CLASSIFY strictly respects")
        print(f"    the declared label set.")
        print(f"    Note: check error field for silent failures.")

    output = {
        "experiment": "02_schema_violation",
        "timestamp": datetime.now().isoformat(),
        "config": {"n_rows": N_ROWS},
        "results": results
    }
    save(output, "exp02_schema_violation.json")
    return output

# ── Experiment 03: AI_FILTER Correct Syntax (RQ1 for AI_FILTER) ───────────────

def exp_03(conn):
    """
    RQ1 for AI_FILTER: Consistency test using verified syntax.

    AI_FILTER(VARCHAR, VARCHAR) does NOT exist.
    Correct syntax: AI_FILTER(PROMPT('condition. Text: {0}', col))
    OR: AI_FILTER('condition. Text: ' || col)

    Also tests: does AI_FILTER agree with AI_CLASSIFY on the same rows?
    (Cross-operator coherence setup for Experiment 04)
    """
    print("\n" + "="*60)
    print("Experiment 03: AI_FILTER Verified Syntax + Consistency (RQ1)")
    print("="*60)

    N_ROWS = 50
    N_RUNS = 5

    rows = execute(conn, f"""
        SELECT O_ORDERKEY, O_COMMENT
        FROM ORDERS
        WHERE LENGTH(O_COMMENT) > 20
        LIMIT {N_ROWS}
    """)
    print(f"Fetched {len(rows)} rows.")

    # Test two correct AI_FILTER syntaxes
    CONDITIONS = {
        "delivery_prompt": "Is this text about a delivery or shipping issue?",
        "billing_prompt": "Does this text mention a payment or billing problem?",
        "urgent_prompt": "Does this text indicate the order is urgent or time-sensitive?"
    }

    all_results = {}

    for cond_name, condition in CONDITIONS.items():
        print(f"\n  Condition: '{cond_name}'")
        print(f"  Text: '{condition}'")

        runs = []
        for run_id in range(1, N_RUNS + 1):
            run_results = {}
            for order_key, comment in rows:
                safe = comment.replace("'", "''")
                # Verified syntax: single composed string
                composed = f"{condition} Text: {safe}"
                composed_safe = composed.replace("'", "''")
                result = execute(conn, f"""
                    SELECT AI_FILTER('{composed_safe}')
                """)
                passed, error = parse_filter(result[0][0])
                run_results[order_key] = passed

            runs.append(run_results)
            n_passed = sum(1 for v in run_results.values() if v)
            print(f"    Run {run_id}: {n_passed}/{N_ROWS} passed filter")
            time.sleep(0.5)

        # Consistency across runs
        n_consistent = 0
        n_flipped = 0
        for order_key, _ in rows:
            vals = [run[order_key] for run in runs]
            if len(set(vals)) == 1:
                n_consistent += 1
            else:
                n_flipped += 1

        print(f"    Consistency: {n_consistent}/{N_ROWS} rows consistent "
              f"across {N_RUNS} runs")

        all_results[cond_name] = {
            "condition": condition,
            "n_rows": N_ROWS,
            "n_runs": N_RUNS,
            "n_consistent": n_consistent,
            "consistency_rate": round(n_consistent/N_ROWS, 4),
            "n_flipped": n_flipped,
            "runs_pass_counts": [
                sum(1 for v in run.values() if v) for run in runs
            ]
        }

    print(f"\nRESULTS:")
    for name, r in all_results.items():
        print(f"  {name}: {r['consistency_rate']*100:.1f}% consistent, "
              f"{r['n_flipped']} rows flipped across {N_RUNS} runs")

    output = {
        "experiment": "03_filter_consistency",
        "timestamp": datetime.now().isoformat(),
        "note": ("AI_FILTER(VARCHAR, VARCHAR) does not exist. "
                 "Correct syntax: single composed string passed to AI_FILTER()."),
        "results": all_results
    }
    save(output, "exp03_filter_consistency.json")
    return output

# ── Experiment 04: Cross-Operator Coherence (RQ3) ─────────────────────────────

def exp_04(conn):
    """
    RQ3: When AI_FILTER and AI_CLASSIFY are chained, are their outputs
    logically coherent?

    Design:
      Step 1: AI_FILTER(row, 'delivery issue?') -> True/False
      Step 2: AI_CLASSIFY(same row, [delivery, billing, product, other])
      Coherence rule: if filter=True, classify should = 'delivery issue'

    Even though both operators are deterministic individually,
    they may disagree on borderline cases — producing a coherence violation.
    """
    print("\n" + "="*60)
    print("Experiment 04: Cross-Operator Coherence (RQ3)")
    print("="*60)

    N_ROWS = 100
    N_RUNS = 3

    PIPELINES = [
        {
            "name": "delivery",
            "filter_condition": "Is this text about a delivery or shipping issue?",
            "classify_labels": [
                'delivery issue', 'billing issue',
                'product quality issue', 'general inquiry'
            ],
            "expected_classify_if_filtered": "delivery issue"
        },
        {
            "name": "billing",
            "filter_condition": "Does this text mention a payment or billing problem?",
            "classify_labels": [
                'delivery issue', 'billing issue',
                'product quality issue', 'general inquiry'
            ],
            "expected_classify_if_filtered": "billing issue"
        }
    ]

    rows = execute(conn, f"""
        SELECT O_ORDERKEY, O_COMMENT
        FROM ORDERS
        WHERE LENGTH(O_COMMENT) > 20
        LIMIT {N_ROWS}
    """)
    print(f"Fetched {len(rows)} rows.")

    all_pipeline_results = []

    for pipeline in PIPELINES:
        name = pipeline["name"]
        filter_cond = pipeline["filter_condition"]
        classify_labels = pipeline["classify_labels"]
        expected = pipeline["expected_classify_if_filtered"]
        labels_sql = str(classify_labels).replace('"', "'")

        print(f"\n  Pipeline: '{name}'")
        print(f"  Filter:   '{filter_cond}'")
        print(f"  Expected: if filter=True → classify='{expected}'")

        pipeline_runs = []

        for run_id in range(1, N_RUNS + 1):
            violations = []
            coherent = 0
            filter_true_count = 0

            for order_key, comment in rows:
                safe = comment.replace("'", "''")

                # Step 1: AI_FILTER (verified syntax)
                composed = f"{filter_cond} Text: {safe}"
                composed_safe = composed.replace("'", "''")
                filter_result = execute(conn, f"""
                    SELECT AI_FILTER('{composed_safe}')
                """)
                passed, _ = parse_filter(filter_result[0][0])

                if not passed:
                    continue  # only test rows that passed the filter

                filter_true_count += 1

                # Step 2: AI_CLASSIFY on same row
                classify_result = execute(conn, f"""
                    SELECT AI_CLASSIFY('{safe}', {labels_sql})
                """)
                label, _ = parse_classify(classify_result[0][0])

                is_coherent = (label == expected)
                if is_coherent:
                    coherent += 1
                else:
                    violations.append({
                        "order_key": order_key,
                        "comment_snippet": comment[:100],
                        "filter_passed": True,
                        "classify_label": label,
                        "expected_label": expected
                    })

            n_tested = filter_true_count
            coherence_rate = coherent / n_tested if n_tested > 0 else None

            print(f"    Run {run_id}: {filter_true_count} passed filter, "
                  f"{coherent} coherent, {len(violations)} violations "
                  f"({coherence_rate*100:.1f}% coherent)" if coherence_rate is not None
                  else f"    Run {run_id}: 0 rows passed filter")

            if violations:
                for v in violations[:2]:
                    print(f"      VIOLATION: OrderKey {v['order_key']}")
                    print(f"        Filter said: relevant to '{name}'")
                    print(f"        Classify said: '{v['classify_label']}'")
                    print(f"        Text: \"{v['comment_snippet']}...\"")

            pipeline_runs.append({
                "run_id": run_id,
                "n_scanned": N_ROWS,
                "n_passed_filter": filter_true_count,
                "n_coherent": coherent,
                "n_violations": len(violations),
                "coherence_rate": round(coherence_rate, 4) if coherence_rate else None,
                "violations": violations
            })
            time.sleep(1)

        all_pipeline_results.append({
            "pipeline": name,
            "runs": pipeline_runs
        })

    # Summary
    print(f"\nRESULTS:")
    total_violations = 0
    for p in all_pipeline_results:
        all_v = sum(r["n_violations"] for r in p["runs"])
        all_t = sum(r["n_passed_filter"] for r in p["runs"])
        total_violations += all_v
        avg_coh = (sum(r["coherence_rate"] for r in p["runs"]
                       if r["coherence_rate"] is not None) /
                   max(1, sum(1 for r in p["runs"]
                              if r["coherence_rate"] is not None)))
        print(f"  {p['pipeline']}: {all_v} violations / {all_t} filter-positive rows "
              f"({avg_coh*100:.1f}% coherent)")

    if total_violations > 0:
        print(f"\n  ★ COHERENCE VIOLATIONS FOUND")
        print(f"    AI_FILTER and AI_CLASSIFY disagree on the same rows.")
        print(f"    Both operators are individually deterministic (Exp 01+03)")
        print(f"    but produce logically contradictory outputs when chained.")
        print(f"    This is the core RQ3 finding.")
    else:
        print(f"\n  → No coherence violations on TPCH data.")
        print(f"    TPCH comments may be too short/unambiguous.")
        print(f"    Scale to larger N or richer dataset for stronger signal.")

    output = {
        "experiment": "04_cross_operator_coherence",
        "timestamp": datetime.now().isoformat(),
        "config": {"n_rows": N_ROWS, "n_runs": N_RUNS},
        "results": all_pipeline_results
    }
    save(output, "exp04_coherence.json")
    return output

# ── Experiment 05: Context Sensitivity Drift (RQ4) ────────────────────────────

def exp_05(conn):
    """
    RQ4: Does AI_CLASSIFY output change when schema-irrelevant columns
    are included in the input text?

    Design:
      Minimal input:  AI_CLASSIFY(O_COMMENT, labels)
      Extended input: AI_CLASSIFY(O_COMMENT || ' OrderKey: ' || O_ORDERKEY
                                  || ' Clerk: ' || O_CLERK, labels)

    If outputs differ -> the operator is sensitive to irrelevant context.
    This is a fundamental reliability concern for production pipelines
    where schema evolution adds columns over time.
    """
    print("\n" + "="*60)
    print("Experiment 05: Context Sensitivity Drift (RQ4)")
    print("="*60)

    N_ROWS = 100
    LABELS = ['urgent', 'standard', 'low priority']
    labels_sql = str(LABELS).replace('"', "'")

    rows = execute(conn, f"""
        SELECT O_ORDERKEY, O_COMMENT, O_CLERK, O_ORDERPRIORITY
        FROM ORDERS
        WHERE LENGTH(O_COMMENT) > 20
        LIMIT {N_ROWS}
    """)
    print(f"Fetched {len(rows)} rows.")
    print(f"Testing minimal vs extended schema input...")

    drifted = []
    stable = 0

    for order_key, comment, clerk, priority in rows:
        safe_comment = comment.replace("'", "''")

        # Minimal: comment only
        result_min = execute(conn, f"""
            SELECT AI_CLASSIFY('{safe_comment}', {labels_sql})
        """)
        label_min, _ = parse_classify(result_min[0][0])

        # Extended: comment + irrelevant fields (order key, clerk name)
        extended = f"{comment} [OrderKey: {order_key}, Clerk: {clerk}]"
        safe_extended = extended.replace("'", "''")
        result_ext = execute(conn, f"""
            SELECT AI_CLASSIFY('{safe_extended}', {labels_sql})
        """)
        label_ext, _ = parse_classify(result_ext[0][0])

        if label_min != label_ext:
            drifted.append({
                "order_key": order_key,
                "comment_snippet": comment[:80],
                "label_minimal": label_min,
                "label_extended": label_ext,
                "added_context": f"OrderKey: {order_key}, Clerk: {clerk}"
            })
        else:
            stable += 1

    n = len(rows)
    drift_rate = len(drifted) / n

    print(f"\nRESULTS:")
    print(f"  Rows tested:  {n}")
    print(f"  Stable:       {stable}/{n} ({stable/n*100:.1f}%)")
    print(f"  Drifted:      {len(drifted)}/{n} ({drift_rate*100:.1f}%)")

    if drifted:
        print(f"\n  ★ CONTEXT SENSITIVITY DRIFT FOUND")
        print(f"    Adding irrelevant columns changes AI_CLASSIFY output.")
        print(f"    Example drifted rows:")
        for d in drifted[:3]:
            print(f"      OrderKey {d['order_key']}:")
            print(f"        Minimal:  '{d['label_minimal']}'")
            print(f"        Extended: '{d['label_extended']}'")
            print(f"        Added:    '{d['added_context']}'")
    else:
        print(f"\n  → No context sensitivity drift on this query.")
        print(f"    AI_CLASSIFY output unchanged by adding irrelevant fields.")
        print(f"    Try with more semantically loaded irrelevant fields.")

    output = {
        "experiment": "05_context_sensitivity",
        "timestamp": datetime.now().isoformat(),
        "config": {"n_rows": N_ROWS, "labels": LABELS},
        "aggregate": {
            "n_rows": n,
            "n_stable": stable,
            "n_drifted": len(drifted),
            "drift_rate": round(drift_rate, 4)
        },
        "drifted_rows": drifted
    }
    save(output, "exp05_context_sensitivity.json")
    return output

# ── Experiment 06: AI_EXTRACT Reliability (RQ1 + RQ2) ─────────────────────────

def exp_06(conn):
    """
    RQ1 + RQ2 for AI_EXTRACT:
      - Is AI_EXTRACT consistent across runs?
      - Does AI_EXTRACT ever return fields outside the declared schema?
      - Does AI_EXTRACT populate fields that don't exist in the text (hallucination)?

    Verified syntax:
      AI_EXTRACT(text, ['field1', 'field2', ...])
      Returns: {"response": {"field1": value, "field2": value}, "error": null}
    """
    print("\n" + "="*60)
    print("Experiment 06: AI_EXTRACT Reliability (RQ1 + RQ2)")
    print("="*60)

    N_ROWS = 30
    N_RUNS = 5
    FIELDS = ['order_status', 'delivery_issue', 'urgency_level', 'action_required']

    rows = execute(conn, f"""
        SELECT O_ORDERKEY, O_COMMENT
        FROM ORDERS
        WHERE LENGTH(O_COMMENT) > 30
        LIMIT {N_ROWS}
    """)
    print(f"Fetched {len(rows)} rows. Fields to extract: {FIELDS}")

    all_runs = []

    for run_id in range(1, N_RUNS + 1):
        run_results = {}
        run_errors = 0
        run_hallucinations = 0

        for order_key, comment in rows:
            safe = comment.replace("'", "''")
            fields_sql = str(FIELDS).replace('"', "'")
            result = execute(conn, f"""
                SELECT AI_EXTRACT('{safe}', {fields_sql})
            """)
            response, error = parse_extract(result[0][0])

            # Check for hallucinated fields (fields not in declared schema)
            extra_fields = set(response.keys()) - set(FIELDS)

            run_results[order_key] = {
                "response": response,
                "error": error,
                "extra_fields": list(extra_fields)
            }
            if error:
                run_errors += 1
            if extra_fields:
                run_hallucinations += 1

        all_runs.append(run_results)
        print(f"  Run {run_id}/{N_RUNS}: errors={run_errors}, "
              f"hallucinated_fields={run_hallucinations}")
        time.sleep(0.5)

    # Consistency: same response across runs?
    n_consistent = 0
    inconsistent_examples = []

    for order_key, comment in rows:
        responses = [run[order_key]["response"] for run in all_runs]
        # Compare as JSON strings for exact match
        response_strs = [json.dumps(r, sort_keys=True) for r in responses]
        is_consistent = len(set(response_strs)) == 1

        if is_consistent:
            n_consistent += 1
        else:
            inconsistent_examples.append({
                "order_key": order_key,
                "comment_snippet": comment[:80],
                "responses_across_runs": responses
            })

    n = len(rows)
    print(f"\nRESULTS:")
    print(f"  Consistent extractions: {n_consistent}/{n} "
          f"({n_consistent/n*100:.1f}%)")
    print(f"  Inconsistent:           {n-n_consistent}/{n}")

    total_hallucinations = sum(
        1 for run in all_runs
        for data in run.values()
        if data["extra_fields"]
    )
    print(f"  Hallucinated fields:    {total_hallucinations} instances "
          f"across all runs")

    if inconsistent_examples:
        print(f"\n  ★ AI_EXTRACT IS INCONSISTENT across runs")
        for ex in inconsistent_examples[:2]:
            print(f"    OrderKey {ex['order_key']}:")
            for i, r in enumerate(ex["responses_across_runs"][:3]):
                print(f"      Run {i+1}: {r}")

    output = {
        "experiment": "06_extract_reliability",
        "timestamp": datetime.now().isoformat(),
        "config": {"n_rows": N_ROWS, "n_runs": N_RUNS, "fields": FIELDS},
        "aggregate": {
            "n_rows": n,
            "n_runs": N_RUNS,
            "n_consistent": n_consistent,
            "consistency_rate": round(n_consistent/n, 4),
            "total_hallucinations": total_hallucinations
        },
        "inconsistent_examples": inconsistent_examples
    }
    save(output, "exp06_extract_reliability.json")
    return output

# ── Main ──────────────────────────────────────────────────────────────────────

EXPERIMENTS = {
    "01": ("AI_CLASSIFY Consistency", exp_01),
    "02": ("Schema Violation", exp_02),
    "03": ("AI_FILTER Syntax + Consistency", exp_03),
    "04": ("Cross-Operator Coherence", exp_04),
    "05": ("Context Sensitivity Drift", exp_05),
    "06": ("AI_EXTRACT Reliability", exp_06),
}

def main():
    parser = argparse.ArgumentParser(description="OperatorBench Experiment Suite")
    parser.add_argument("--exp", default="all",
                        help="Experiment to run: 01-06 or 'all'")
    args = parser.parse_args()

    print("OperatorBench — Verified Experiment Suite")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Running: {args.exp}")

    conn = get_conn()

    if args.exp == "all":
        to_run = list(EXPERIMENTS.keys())
    else:
        to_run = [args.exp.zfill(2)]

    for exp_id in to_run:
        if exp_id not in EXPERIMENTS:
            print(f"Unknown experiment: {exp_id}")
            continue
        name, fn = EXPERIMENTS[exp_id]
        print(f"\n{'='*60}")
        print(f"Starting Experiment {exp_id}: {name}")
        fn(conn)

    conn.close()
    print(f"\n{'='*60}")
    print("All experiments complete.")
    print(f"Results saved to: results/")

if __name__ == "__main__":
    main()
