"""
Unit tests for harness/metrics.py

Run:
    cd OperatorBench
    .venv/Scripts/activate  (Windows) or source .venv/bin/activate (Linux)
    python -m pytest tests/test_metrics.py -v
"""

import math
import pandas as pd
import pytest
import sys
from pathlib import Path

# Ensure harness is importable
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from harness.metrics import (
    wilson_ci,
    contradiction_rate,
    schema_violation_rate,
    consistency_across_runs,
    context_sensitivity_drift,
)


# ---------------------------------------------------------------------------
# wilson_ci
# ---------------------------------------------------------------------------

class TestWilsonCI:
    def test_zero_successes(self):
        lo, hi = wilson_ci(0, 100)
        assert lo == 0.0
        assert hi > 0.0
        assert hi < 0.05  # should be a tight upper bound

    def test_all_successes(self):
        lo, hi = wilson_ci(100, 100)
        assert lo > 0.95
        assert hi > 0.99  # floating-point: min(1.0, ...) may be 0.9999999999999999

    def test_half(self):
        lo, hi = wilson_ci(50, 100)
        assert lo < 0.5
        assert hi > 0.5
        # CI should be roughly [0.40, 0.60] for n=100
        assert 0.35 < lo < 0.45
        assert 0.55 < hi < 0.65

    def test_empty(self):
        lo, hi = wilson_ci(0, 0)
        assert lo == 0.0
        assert hi == 0.0

    def test_single_trial_success(self):
        lo, hi = wilson_ci(1, 1)
        assert lo > 0.0
        assert hi == 1.0

    def test_single_trial_failure(self):
        lo, hi = wilson_ci(0, 1)
        assert lo == 0.0
        assert hi < 1.0

    def test_bounds_ordering(self):
        """Lower bound should always be <= upper bound."""
        for k in range(0, 21):
            lo, hi = wilson_ci(k, 20)
            assert lo <= hi, f"Failed for k={k}, n=20: {lo} > {hi}"

    def test_known_value(self):
        """Verify against a hand-calculated Wilson CI."""
        # k=5, n=100, z=1.96
        # p_hat = 0.05, denom = 1 + 1.96^2/100 = 1.038416
        # centre = (0.05 + 1.96^2/200) / 1.038416
        # half_width = 1.96 * sqrt((0.05*0.95 + 1.96^2/400)/100) / 1.038416
        lo, hi = wilson_ci(5, 100)
        assert abs(lo - 0.0216) < 0.005
        assert abs(hi - 0.1118) < 0.005


# ---------------------------------------------------------------------------
# contradiction_rate
# ---------------------------------------------------------------------------

class TestContradictionRate:
    def _make_df(self, labels):
        """Helper: make a DataFrame with given map labels."""
        return pd.DataFrame(
            {"mapped_label": labels},
            index=range(len(labels)),
        )

    def test_no_contradictions(self):
        # All filter-positive rows map to "negative" (consistent with filter=negative)
        df = self._make_df(["negative", "negative", "negative"])
        is_contra = lambda passed, label: label == "positive"
        result = contradiction_rate(df, "mapped_label", is_contra)
        assert result["n_contradictions"] == 0
        assert result["rate"] == 0.0
        assert result["n_filter_positive"] == 3

    def test_all_contradictions(self):
        df = self._make_df(["positive", "positive", "positive"])
        is_contra = lambda passed, label: label == "positive"
        result = contradiction_rate(df, "mapped_label", is_contra)
        assert result["n_contradictions"] == 3
        assert result["rate"] == 1.0

    def test_partial_contradictions(self):
        df = self._make_df(["negative", "positive", "negative", "positive", "neutral"])
        is_contra = lambda passed, label: label == "positive"
        result = contradiction_rate(df, "mapped_label", is_contra)
        assert result["n_contradictions"] == 2
        assert result["rate"] == 0.4
        assert result["ci_lower"] < 0.4
        assert result["ci_upper"] > 0.4

    def test_empty_dataframe(self):
        df = pd.DataFrame({"mapped_label": []})
        is_contra = lambda passed, label: label == "positive"
        result = contradiction_rate(df, "mapped_label", is_contra)
        assert result["n_contradictions"] == 0
        assert result["rate"] is None
        assert result["ci_lower"] is None and result["ci_upper"] is None

    def test_contradicted_rows_tracking(self):
        df = self._make_df(["negative", "positive", "neutral"])
        is_contra = lambda passed, label: label == "positive"
        result = contradiction_rate(df, "mapped_label", is_contra)
        assert len(result["contradicted_rows"]) == 1
        assert result["contradicted_rows"][0] == (1, "positive")

    def test_abstracts_contradiction_logic(self):
        """Test the actual abstracts contradiction predicate."""
        df = self._make_df(["ML", "theory", "systems", "ML", "other"])
        # Abstracts contradiction: filter says ML-related, but map says non-ML
        is_contra = lambda passed, label: label != "ML"
        result = contradiction_rate(df, "mapped_label", is_contra)
        assert result["n_contradictions"] == 3  # theory, systems, other


# ---------------------------------------------------------------------------
# schema_violation_rate
# ---------------------------------------------------------------------------

class TestSchemaViolationRate:
    def test_no_violations(self):
        df = pd.DataFrame({"label": ["positive", "negative", "neutral"]})
        result = schema_violation_rate(df, "label", {"positive", "negative", "neutral"})
        assert result["n_violations"] == 0
        assert result["rate"] == 0.0

    def test_all_violations(self):
        df = pd.DataFrame({"label": ["garbage", "nonsense", "12345"]})
        result = schema_violation_rate(df, "label", {"positive", "negative", "neutral"})
        assert result["n_violations"] == 3
        assert result["rate"] == 1.0

    def test_partial_violations(self):
        df = pd.DataFrame({"label": ["positive", "garbage", "negative"]})
        result = schema_violation_rate(df, "label", {"positive", "negative", "neutral"})
        assert result["n_violations"] == 1
        assert abs(result["rate"] - 1/3) < 0.01

    def test_case_insensitive(self):
        df = pd.DataFrame({"label": ["Positive", "NEGATIVE", "Neutral"]})
        result = schema_violation_rate(df, "label", {"positive", "negative", "neutral"}, case_sensitive=False)
        assert result["n_violations"] == 0

    def test_case_sensitive(self):
        df = pd.DataFrame({"label": ["Positive", "negative", "Neutral"]})
        result = schema_violation_rate(df, "label", {"positive", "negative", "neutral"}, case_sensitive=True)
        assert result["n_violations"] == 2  # "Positive" and "Neutral"

    def test_empty(self):
        df = pd.DataFrame({"label": []})
        result = schema_violation_rate(df, "label", {"a", "b"})
        assert result["n_total"] == 0
        assert result["rate"] is None
        assert result["ci_lower"] is None and result["ci_upper"] is None

    def test_violation_examples_capped(self):
        """violation_examples should be capped at 20."""
        labels = [f"bad_{i}" for i in range(50)]
        df = pd.DataFrame({"label": labels})
        result = schema_violation_rate(df, "label", {"good"})
        assert result["n_violations"] == 50
        assert len(result["violation_examples"]) == 20


# ---------------------------------------------------------------------------
# consistency_across_runs
# ---------------------------------------------------------------------------

class TestConsistencyAcrossRuns:
    def test_perfect_consistency(self):
        run1 = pd.DataFrame({"label": ["a", "b", "c"]}, index=[0, 1, 2])
        run2 = pd.DataFrame({"label": ["a", "b", "c"]}, index=[0, 1, 2])
        result = consistency_across_runs([run1, run2], "label")
        assert result["n_stable"] == 3
        assert result["n_flipped"] == 0
        assert result["stability_rate"] == 1.0

    def test_all_flipped(self):
        run1 = pd.DataFrame({"label": ["a", "b", "c"]}, index=[0, 1, 2])
        run2 = pd.DataFrame({"label": ["x", "y", "z"]}, index=[0, 1, 2])
        result = consistency_across_runs([run1, run2], "label")
        assert result["n_stable"] == 0
        assert result["n_flipped"] == 3
        assert result["flip_rate"] == 1.0

    def test_partial_flip(self):
        run1 = pd.DataFrame({"label": ["a", "b", "c", "d"]}, index=[0, 1, 2, 3])
        run2 = pd.DataFrame({"label": ["a", "b", "X", "Y"]}, index=[0, 1, 2, 3])
        result = consistency_across_runs([run1, run2], "label")
        assert result["n_stable"] == 2
        assert result["n_flipped"] == 2
        assert result["flip_rate"] == 0.5

    def test_three_runs(self):
        run1 = pd.DataFrame({"label": ["a", "b"]}, index=[0, 1])
        run2 = pd.DataFrame({"label": ["a", "b"]}, index=[0, 1])
        run3 = pd.DataFrame({"label": ["a", "X"]}, index=[0, 1])
        result = consistency_across_runs([run1, run2, run3], "label")
        assert result["n_runs"] == 3
        assert result["n_stable"] == 1  # row 0 stable
        assert result["n_flipped"] == 1  # row 1 flipped in run3

    def test_missing_rows_excluded(self):
        """Only rows present in ALL runs should be counted."""
        run1 = pd.DataFrame({"label": ["a", "b", "c"]}, index=[0, 1, 2])
        run2 = pd.DataFrame({"label": ["a", "b"]}, index=[0, 1])  # missing row 2
        result = consistency_across_runs([run1, run2], "label")
        assert result["n_rows"] == 2  # only rows 0 and 1

    def test_single_run_error(self):
        run1 = pd.DataFrame({"label": ["a"]}, index=[0])
        result = consistency_across_runs([run1], "label")
        assert "error" in result

    def test_flip_details(self):
        run1 = pd.DataFrame({"label": ["a", "b"]}, index=[0, 1])
        run2 = pd.DataFrame({"label": ["a", "X"]}, index=[0, 1])
        result = consistency_across_runs([run1, run2], "label")
        assert len(result["flip_details"]) == 1
        assert result["flip_details"][0]["row_id"] == 1
        assert result["flip_details"][0]["labels"] == ["b", "X"]


# ---------------------------------------------------------------------------
# context_sensitivity_drift
# ---------------------------------------------------------------------------

class TestContextSensitivityDrift:
    def test_no_drift(self):
        base = pd.DataFrame({"label": ["a", "b", "c"]}, index=[0, 1, 2])
        inj = pd.DataFrame({"label": ["a", "b", "c"]}, index=[0, 1, 2])
        result = context_sensitivity_drift(base, inj, "label")
        assert result["n_flipped"] == 0
        assert result["drift_rate"] == 0.0

    def test_all_drift(self):
        base = pd.DataFrame({"label": ["a", "b", "c"]}, index=[0, 1, 2])
        inj = pd.DataFrame({"label": ["x", "y", "z"]}, index=[0, 1, 2])
        result = context_sensitivity_drift(base, inj, "label")
        assert result["n_flipped"] == 3
        assert result["drift_rate"] == 1.0

    def test_partial_drift(self):
        base = pd.DataFrame({"label": ["a", "b", "c"]}, index=[0, 1, 2])
        inj = pd.DataFrame({"label": ["a", "X", "c"]}, index=[0, 1, 2])
        result = context_sensitivity_drift(base, inj, "label")
        assert result["n_flipped"] == 1
        assert abs(result["drift_rate"] - 1/3) < 0.01

    def test_only_common_rows(self):
        base = pd.DataFrame({"label": ["a", "b"]}, index=[0, 1])
        inj = pd.DataFrame({"label": ["a", "X"]}, index=[0, 2])  # row 2 not in base
        result = context_sensitivity_drift(base, inj, "label")
        assert result["n_common"] == 1  # only row 0
        assert result["n_flipped"] == 0

    def test_flip_details_content(self):
        base = pd.DataFrame({"label": ["a", "b"]}, index=[0, 1])
        inj = pd.DataFrame({"label": ["a", "X"]}, index=[0, 1])
        result = context_sensitivity_drift(base, inj, "label")
        assert result["flip_details"][0]["baseline"] == "b"
        assert result["flip_details"][0]["injected"] == "X"


# ---------------------------------------------------------------------------
# Cross-metric consistency checks
# ---------------------------------------------------------------------------

class TestCrossMetricConsistency:
    def test_ci_contains_point_estimate(self):
        """Wilson CI should always contain the point estimate."""
        for k, n in [(0, 10), (3, 10), (5, 10), (10, 10), (50, 100)]:
            lo, hi = wilson_ci(k, n)
            p = k / n
            assert lo <= p + 1e-10, f"CI lower {lo} > p_hat {p} for k={k}, n={n}"
            assert hi >= p - 1e-10, f"CI upper {hi} < p_hat {p} for k={k}, n={n}"

    def test_contradiction_ci_contains_rate(self):
        """Contradiction rate should fall within its own CI."""
        df = pd.DataFrame({"label": ["pos"] * 3 + ["neg"] * 7}, index=range(10))
        is_contra = lambda passed, label: label == "pos"
        result = contradiction_rate(df, "label", is_contra)
        assert result["ci_lower"] <= result["rate"]
        assert result["ci_upper"] >= result["rate"]
