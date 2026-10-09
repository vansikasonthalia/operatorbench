from copy import deepcopy

import pytest

from harness.admission import build_ledger
from harness.revision import check_revision
from scripts.compare_admission_reporting import constructed_witnesses


def ledger(before, after, universe=range(4)):
    def records(labels):
        return {row: {"label": label, "raw": label} for row, label in labels.items()}
    return build_ledger(records(before), records(after), universe, label_column="label", raw_column="raw",
                        valid_labels={"ML", "theory"}, disagreement=lambda label: label != "ML")


def test_intentional_narrowing_passes_subset_but_violates_equal_admissions():
    saved = constructed_witnesses()["cases"]["exclusion_only"]["full_ledger"]
    assert check_revision(saved, admission_relation="subset", preserve_common_labels=True)["status"] == "satisfied"
    strict = check_revision(saved, admission_relation="equal", preserve_common_labels=True)
    assert strict["checks"]["admissions"]["row_ids"] == [0]
    assert strict["status"] == "violated"


def test_cancelled_indicator_changes_are_reported_without_rejecting_all_label_changes():
    saved = ledger({0: "ML", 1: "theory"}, {0: "theory", 1: "ML"})
    assert saved["identity"]["observed_count_change"] == 0
    check = check_revision(saved, admission_relation="equal", forbid_new_common_disagreements=True)
    assert check["checks"]["new_common_disagreements"]["row_ids"] == [0]
    assert check["status"] == "violated"
    assert check_revision(saved, admission_relation="equal")["status"] == "satisfied"


def test_membership_constraints_detect_both_directions_and_ignore_never_admitted_rows():
    saved = ledger({0: "ML", 1: "ML"}, {1: "ML", 2: "ML"})
    assert check_revision(saved, admission_relation="subset")["checks"]["admissions"]["row_ids"] == [2]
    assert check_revision(saved, admission_relation="superset")["checks"]["admissions"]["row_ids"] == [0]
    assert check_revision(saved, admission_relation="equal")["checks"]["admissions"]["row_ids"] == [0, 2]
    assert check_revision(saved, admission_relation="unrestricted")["status"] == "satisfied"


def test_raw_invalidity_is_an_exception_even_when_saved_label_is_valid():
    saved = build_ledger({0: {"label": "ML", "raw": "The label is ML"}},
                         {0: {"label": "ML", "raw": "ML"}}, [0], label_column="label", raw_column="raw",
                         valid_labels={"ML"}, disagreement=lambda label: label != "ML")
    check = check_revision(saved, admission_relation="equal")
    assert check["status"] == "violated"
    assert check["checks"]["output_validity"]["invalid_outputs"]["before"]["raw"]["row_ids"] == [0]


def test_partial_evidence_cannot_certify_full_input_but_can_be_explicitly_scoped():
    saved = ledger({0: "ML"}, {0: "ML"}, [0, 1])
    original = deepcopy(saved)
    full = check_revision(saved, admission_relation="equal", unassessed_rows=[2, 3])
    assert full["status"] == "not_evaluable"
    scoped = check_revision(saved, admission_relation="equal", unassessed_rows=[2, 3], require_complete_input=False)
    assert scoped["status"] == "satisfied"
    assert scoped["scope"] == {"n_evaluated": 2, "unassessed_rows": [2, 3], "n_reference_input": 4, "complete": False}
    assert saved == original


def test_empty_common_rows_are_not_a_label_preservation_success():
    saved = ledger({0: "ML"}, {1: "ML"})
    check = check_revision(saved, admission_relation="unrestricted", preserve_common_labels=True)
    assert check["checks"]["common_labels"]["status"] == "not_evaluable"
    assert check["status"] == "not_evaluable"


def test_known_violation_is_not_hidden_by_missing_evidence():
    saved = ledger({0: "ML"}, {0: "theory"}, [0])
    check = check_revision(saved, admission_relation="equal", preserve_common_labels=True, unassessed_rows=[1])
    assert check["checks"]["complete_input"]["status"] == "not_evaluable"
    assert check["checks"]["common_labels"]["row_ids"] == [0]
    assert check["status"] == "violated"


def test_invalid_requirements_or_overlapping_scope_are_rejected():
    saved = ledger({}, {})
    with pytest.raises(ValueError, match="admission relation"):
        check_revision(saved, admission_relation="approximately_equal")
    with pytest.raises(ValueError, match="overlap"):
        check_revision(saved, admission_relation="equal", unassessed_rows=[0])
