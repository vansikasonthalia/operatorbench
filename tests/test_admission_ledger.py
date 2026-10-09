from copy import deepcopy

import pytest

from harness.admission import build_ledger
from scripts.admission_ledger_example import example


def test_opposing_changes_and_selection_reconcile_without_mutating_records():
    demonstration = example()
    ledger = demonstration["ledger"]
    assert ledger["sets"] == {"common": [0, 1, 2, 3], "excluded": [4], "newly_admitted": [5], "never_admitted": [6]}
    assert ledger["before"]["disagreements"] == ledger["after"]["disagreements"] == 3
    assert ledger["terms"] == {"introduced_by_new_admissions": 1, "removed_by_exclusions": 1,
                               "common_rows_new_disagreements": 1, "common_rows_resolved_disagreements": 1,
                               "common_rows_net": 0}
    assert ledger["common_indicator_changes"] == {"new_disagreements": [0], "resolved_disagreements": [1]}
    assert ledger["identity"] == {"observed_count_change": 0, "accounted_count_change": 0, "holds": True}
    for layer, expected in (("saved", 2), ("raw", 4)):
        changes = ledger["label_transitions"][layer]
        assert sum(t["count"] for t in changes) == 4
        assert sum(t["count"] for t in changes if t["before"] != t["after"]) == expected
    original = deepcopy(demonstration)
    build_ledger(demonstration["before_records"], demonstration["after_records"], demonstration["input_rows"],
                 label_column="label", raw_column="raw", valid_labels={"ML", "systems", "theory", "other"},
                 disagreement=lambda label: label != "ML")
    assert demonstration == original


def test_raw_validity_is_not_silently_replaced_by_saved_validity():
    ledger = example()["ledger"]
    assert ledger["before"]["invalid_outputs"]["raw"] == {"count": 2, "row_ids": [2, 3]}
    assert ledger["before"]["invalid_outputs"]["saved"] == {"count": 1, "row_ids": [3]}
    assert ledger["after"]["invalid_outputs"]["raw"] == {"count": 1, "row_ids": [3]}
    assert ledger["after"]["invalid_outputs"]["saved"] == {"count": 1, "row_ids": [3]}


def test_empty_admissions_have_no_conditional_rate_and_retain_input_universe():
    ledger = build_ledger({}, {}, [1, 2], label_column="label", raw_column="raw",
                          valid_labels={"negative"}, disagreement=lambda label: label == "positive")
    assert ledger["before"]["disagreement_rate"] is None
    assert ledger["after"]["disagreement_rate"] is None
    assert ledger["sets"]["never_admitted"] == [1, 2]
    assert ledger["identity"]["accounted_count_change"] == 0


def test_admitted_ids_cannot_silently_expand_the_input_universe():
    with pytest.raises(ValueError, match="input universe"):
        build_ledger({3: {"label": "negative", "raw": "negative"}}, {}, [1, 2],
                     label_column="label", raw_column="raw", valid_labels={"negative"},
                     disagreement=lambda label: label == "positive")
