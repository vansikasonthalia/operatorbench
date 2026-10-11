"""
OperatorBench -- Metrics

Core measurements for all four RQs:
  - RQ1: Output consistency (flip rate across repeated runs)
  - RQ2: Schema violation rate (with Wilson confidence interval)
  - RQ3: Cross-operator criterion disagreement (headline metric)
  - RQ4: Context sensitivity drift (label flip under metadata injection)

All metrics return dicts suitable for JSON serialisation and table rendering.
"""

import math
import pandas as pd
from typing import Callable, Optional


def _row_ids(df, id_col="row_id"):
    values = df[id_col] if id_col in df.columns else df.index
    ids = [int(value) for value in values]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate row IDs would overwrite observations")
    return ids


def raw_schema_compliance(map_df, raw_col, valid_labels):
    """Check raw label membership, ignoring case and surrounding whitespace."""
    ids = _row_ids(map_df)
    valid = {label.lower() for label in valid_labels}
    failures = [rid for rid, value in zip(ids, map_df[raw_col])
                if pd.isna(value) or str(value).strip().lower() not in valid]
    n = len(map_df)
    return {"n_total": n, "n_violations": len(failures),
            "rate": round(len(failures) / n, 4) if n else None,
            "violation_rows": failures,
            "measurement": "raw exact label membership; whitespace trimmed; case ignored"}


def disagreement_breakdown(map_df, map_col, valid_labels, is_contradiction):
    """Count invalid outputs separately from valid-label disagreement."""
    _row_ids(map_df)
    valid = {label.lower() for label in valid_labels}
    counts = {"agreement": 0, "valid_label_disagreement": 0, "invalid_output": 0}
    for value in map_df[map_col]:
        if pd.isna(value) or str(value).strip().lower() not in valid:
            counts["invalid_output"] += 1
        elif is_contradiction(True, str(value)):
            counts["valid_label_disagreement"] += 1
        else:
            counts["agreement"] += 1
    return {"n_total": len(map_df), **counts}


# ---------------------------------------------------------------------------
# RQ3: Operational criterion-disagreement rate
# ---------------------------------------------------------------------------

def contradiction_rate(
    map_df: pd.DataFrame,
    map_col: str,
    is_contradiction: Callable[[bool, str], bool],
) -> dict:
    """
    Compute the operational cross-operator criterion-disagreement rate.

    The function and returned `n_contradictions`/`contradicted_rows` fields keep
    their historical names for artifact compatibility. The manuscript maps
    these to criterion disagreement under the frozen predicate.

    Args:
        map_df: DataFrame of filter-positive rows after sem_map
                (every row here passed the filter)
        map_col: column name containing the normalised map label
        is_contradiction: function(filter_passed: bool, map_label: str) -> bool

    Returns dict:
        n_filter_positive: int (denominator)
        n_contradictions: int
        rate: float (0-1)
        ci_lower, ci_upper: Wilson 95% CI bounds
        contradicted_rows: list of (row_id, map_label) pairs
    """
    n = len(map_df)
    if n == 0:
        return {
            "n_filter_positive": 0,
            "n_contradictions": 0,
            "rate": None,
            "ci_lower": None,
            "ci_upper": None,
            "contradicted_rows": [],
        }

    contradicted = []
    for row_id, row in map_df.iterrows():
        label = str(row[map_col])
        if is_contradiction(True, label):  # all rows in map_df passed filter
            contradicted.append((int(row_id), label))

    k = len(contradicted)
    rate = k / n
    ci_lo, ci_hi = wilson_ci(k, n)

    return {
        "n_filter_positive": n,
        "n_contradictions": k,
        "rate": round(rate, 4),
        "ci_lower": round(ci_lo, 4),
        "ci_upper": round(ci_hi, 4),
        "contradicted_rows": contradicted,
    }


# ---------------------------------------------------------------------------
# RQ2: Schema violation rate
# ---------------------------------------------------------------------------

def schema_violation_rate(
    map_df: pd.DataFrame,
    map_col: str,
    valid_labels: set[str],
    case_sensitive: bool = False,
) -> dict:
    """
    Compute rate of map outputs that fall outside the valid label set.

    Args:
        map_df: DataFrame after sem_map
        map_col: column with normalised labels
        valid_labels: set of acceptable labels
        case_sensitive: if False, compare lowered

    Returns dict:
        n_total: int
        n_violations: int
        rate: float
        ci_lower, ci_upper: Wilson 95% CI
        violation_examples: list of (row_id, bad_label)
    """
    n = len(map_df)
    if n == 0:
        return {
            "n_total": 0, "n_violations": 0, "rate": None,
            "ci_lower": None, "ci_upper": None, "violation_examples": [],
        }

    if case_sensitive:
        check = valid_labels
    else:
        check = {lbl.lower() for lbl in valid_labels}

    violations = []
    for row_id, row in map_df.iterrows():
        label = str(row[map_col])
        cmp_label = label if case_sensitive else label.lower()
        if cmp_label not in check:
            violations.append((int(row_id), label))

    k = len(violations)
    rate = k / n
    ci_lo, ci_hi = wilson_ci(k, n)

    return {
        "n_total": n,
        "n_violations": k,
        "rate": round(rate, 4),
        "ci_lower": round(ci_lo, 4),
        "ci_upper": round(ci_hi, 4),
        "violation_examples": violations[:20],  # cap for readability
    }


# ---------------------------------------------------------------------------
# RQ1: Output consistency across runs
# ---------------------------------------------------------------------------

def consistency_across_runs(
    run_results: list[pd.DataFrame],
    label_col: str,
    id_col: str = "row_id",
) -> dict:
    """
    Measure output consistency across repeated pipeline runs.

    Args:
        run_results: list of DataFrames, one per run, each with id_col and label_col
        label_col: column containing the label to compare
        id_col: row identifier column (or index name)

    Returns dict:
        n_runs: int
        n_rows: int (rows present in ALL runs)
        n_stable: int (same label in every run)
        n_flipped: int (different label in at least one run)
        stability_rate: float
        flip_rate: float
        ci_lower, ci_upper: Wilson CI on flip rate
        flip_details: list of {row_id, labels: [per-run labels]}
    """
    if len(run_results) < 2:
        return {"error": "Need at least 2 runs to measure consistency"}

    # Align all runs by row_id
    aligned = {}
    for i, df in enumerate(run_results):
        _row_ids(df, id_col)
        if id_col in df.columns:
            for _, row in df.iterrows():
                rid = int(row[id_col])
                aligned.setdefault(rid, {})[i] = str(row[label_col])
        else:
            # id_col is the index
            for rid, row in df.iterrows():
                aligned.setdefault(int(rid), {})[i] = str(row[label_col])

    n_runs = len(run_results)
    # Only consider rows present in ALL runs
    common_rows = {rid: labels for rid, labels in aligned.items()
                   if len(labels) == n_runs}

    n_rows = len(common_rows)
    flipped = []
    stable = 0

    for rid, labels_dict in sorted(common_rows.items()):
        labels_list = [labels_dict[i] for i in range(n_runs)]
        if len(set(labels_list)) == 1:
            stable += 1
        else:
            flipped.append({"row_id": rid, "labels": labels_list})

    n_flipped = len(flipped)
    flip_rate = n_flipped / n_rows if n_rows > 0 else 0.0
    ci_lo, ci_hi = wilson_ci(n_flipped, n_rows) if n_rows > 0 else (0.0, 0.0)

    return {
        "n_runs": n_runs,
        "n_rows": n_rows,
        "n_stable": stable,
        "n_flipped": n_flipped,
        "stability_rate": round(1 - flip_rate, 4) if n_rows else None,
        "flip_rate": round(flip_rate, 4) if n_rows else None,
        "ci_lower": round(ci_lo, 4) if n_rows else None,
        "ci_upper": round(ci_hi, 4) if n_rows else None,
        "n_rows_union": len(aligned),
        "n_excluded_rows": len(aligned) - n_rows,
        "flip_details": flipped[:50],  # cap for readability
    }


# ---------------------------------------------------------------------------
# RQ4: Context sensitivity drift
# ---------------------------------------------------------------------------

def context_sensitivity_drift(
    baseline_df: pd.DataFrame,
    injected_df: pd.DataFrame,
    label_col: str,
    id_col: str = "row_id",
) -> dict:
    """
    Measure label flip rate between baseline and metadata-injected runs.

    Args:
        baseline_df: results without injected metadata
        injected_df: results with semantics-preserving metadata added
        label_col: column containing the label

    Returns dict:
        n_common: rows present in both
        n_flipped: rows where label changed
        drift_rate: float
        ci_lower, ci_upper: Wilson CI
        flip_details: list of {row_id, baseline, injected}
    """
    def _to_dict(df):
        _row_ids(df, id_col)
        d = {}
        if id_col in df.columns:
            for _, row in df.iterrows():
                d[int(row[id_col])] = str(row[label_col])
        else:
            for rid, row in df.iterrows():
                d[int(rid)] = str(row[label_col])
        return d

    base = _to_dict(baseline_df)
    inj = _to_dict(injected_df)
    common = set(base.keys()) & set(inj.keys())

    flipped = []
    for rid in sorted(common):
        if base[rid] != inj[rid]:
            flipped.append({
                "row_id": rid,
                "baseline": base[rid],
                "injected": inj[rid],
            })

    n = len(common)
    k = len(flipped)
    drift_rate = k / n if n > 0 else 0.0
    ci_lo, ci_hi = wilson_ci(k, n) if n > 0 else (0.0, 0.0)

    return {
        "n_common": n,
        "n_flipped": k,
        "drift_rate": round(drift_rate, 4) if n else None,
        "ci_lower": round(ci_lo, 4) if n else None,
        "ci_upper": round(ci_hi, 4) if n else None,
        "n_excluded_rows": len(set(base) | set(inj)) - n,
        "flip_details": flipped[:50],
    }


# ---------------------------------------------------------------------------
# Wilson confidence interval
# ---------------------------------------------------------------------------

def wilson_ci(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """
    Wilson score interval for a binomial proportion.

    Args:
        k: number of successes (e.g. contradictions)
        n: number of trials (e.g. filter-positive rows)
        z: z-score for confidence level (1.96 = 95%)

    Returns (lower, upper) bounds.
    """
    if n == 0:
        return (0.0, 0.0)

    p_hat = k / n
    denom = 1 + z**2 / n
    centre = (p_hat + z**2 / (2 * n)) / denom
    # NB: half_width, NOT "spread". `spread` elsewhere in this codebase
    # (run_rq3.py / run_rq4.py :: aggregate_domain) means the gap between
    # per-replicate rates -- a different quantity entirely. Do not re-use
    # the name here; the two were previously identical and confusing.
    half_width = z * math.sqrt((p_hat * (1 - p_hat) + z**2 / (4 * n)) / n) / denom

    lower = max(0.0, centre - half_width)
    upper = min(1.0, centre + half_width)
    return (lower, upper)


# ---------------------------------------------------------------------------
# Summary table helper
# ---------------------------------------------------------------------------

def format_results_table(results: dict, title: str = "") -> str:
    """Format a metrics dict as a readable text table."""
    lines = []
    if title:
        lines.append(title)
        lines.append("-" * len(title))
    for key, val in results.items():
        if isinstance(val, list):
            lines.append(f"  {key}: [{len(val)} items]")
        elif val is None:
            lines.append(f"  {key}: not evaluable")
        elif isinstance(val, float):
            lines.append(f"  {key}: {val:.4f}")
        else:
            lines.append(f"  {key}: {val}")
    return "\n".join(lines)
