from pathlib import Path

from harness.admission import build_ledger
from scripts import compare_admission_reporting as comparison
from scripts import run_matched_control as control
from scripts.admission_ledger_example import example


def test_identical_totals_can_hide_different_mechanisms():
    witness = comparison.constructed_witnesses()
    excluded = witness["cases"]["exclusion_only"]
    changed = witness["cases"]["common_change_only"]
    assert excluded["rate_only"] == changed["rate_only"]
    assert excluded["counts_and_coverage"] == changed["counts_and_coverage"]
    assert excluded["full_ledger"]["terms"]["removed_by_exclusions"] == 1
    assert changed["full_ledger"]["terms"]["common_rows_resolved_disagreements"] == 1
    assert excluded["paired_common_and_totals"] != changed["paired_common_and_totals"]


def test_paired_report_retains_opposing_changes_and_reconstructs_selection():
    views = comparison.reporting_views(example()["ledger"], lambda label: label != "ML")
    paired = views["paired_common_and_totals"]["common"]
    assert paired["new_disagreements"] == paired["resolved_disagreements"] == 1
    assert paired["before_disagreements"] == paired["after_disagreements"] == 2
    assert paired["saved_label_changes"] == 2
    assert views["reconstructed_from_paired_counts"]["matches_ledger"]


def test_binary_masks_share_valid_cohort_and_keep_invalid_answers_separate(monkeypatch):
    answers = {
        "about_filter": [True, True, True, False],
        "about_map": [False, True, True, True],
        "primary_filter": [True, False, False, True],
        "primary_map": [True, None, False, False],
    }
    jobs = [{"replicate": 1, "cell": cell} for cell in control.CELLS]
    plan = {"source_sha256": comparison.audit.digest(Path(control.__file__)), "replicates": 1,
            "row_ids": [0, 1, 2, 3], "jobs": jobs}
    monkeypatch.setattr(comparison.audit, "load_json", lambda path: plan)

    def checkpoint(folder, plan, job):
        return {"records": [{"row_id": row, "answer": answer,
                             "raw_output": "Answer: False." if answer is None else f"Answer:   {answer}"}
                            for row, answer in enumerate(answers[job["cell"]])]}

    monkeypatch.setattr(control, "read_completed", checkpoint)
    cases, exclusions, _ = comparison.matched_cases(comparison.DEFAULT_RUN)
    assert exclusions[0]["invalid_by_cell"]["primary_map"] == [1]
    assert exclusions[0]["n_eligible"] == 3
    for case in cases:
        ledger = case["views"]["full_ledger"]
        assert ledger["n_input"] == 3
        assert all(1 not in rows for rows in ledger["sets"].values())
        for side in ("before", "after"):
            assert ledger[side]["invalid_outputs"]["raw"]["count"] == 0
    joint = cases[2]["views"]["full_ledger"]
    assert joint["before"]["disagreements"] == joint["after"]["disagreements"] == 1
    assert joint["terms"]["introduced_by_new_admissions"] == 1
    assert joint["terms"]["common_rows_resolved_disagreements"] == 1


def test_empty_cohort_does_not_report_zero_rates_or_coverage():
    ledger = build_ledger({}, {}, [], label_column="label", raw_column="raw",
                          valid_labels={"True", "False"}, disagreement=lambda label: label == "False")
    views = comparison.reporting_views(ledger, lambda label: label == "False")
    assert views["rate_only"] == {"before": None, "after": None}
    assert views["counts_and_coverage"]["before"]["coverage"] is None
    assert views["reconstructed_from_paired_counts"]["matches_ledger"]
