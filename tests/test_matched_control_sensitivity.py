from copy import deepcopy

from scripts import run_matched_control as control
from scripts.matched_control_sensitivity import sensitivity_cells


def test_period_sensitivity_does_not_coerce_other_invalid_answers():
    records = {(1, cell): {
        0: {"answer": None, "raw_output": "Answer: False."},
        1: {"answer": None, "raw_output": "Probably False."},
        2: {"answer": True, "raw_output": "Answer: True"},
        3: {"answer": None, "raw_output": "Answer: True."},
    } for cell in control.CELLS}
    original = deepcopy(records)
    strict, period, excluded, changes = sensitivity_cells(records, 1, [2])
    assert records == original
    for cell in control.CELLS:
        assert period[1, cell][0] is False
        assert period[1, cell][1] is None
        assert period[1, cell][2] is True
        assert period[1, cell][3] is True
        assert strict[1, cell][0] is None
        assert excluded[1, cell][0] is None
    assert len(changes) == 8


def test_gap_exclusion_is_shared_by_conditions_and_confined_to_one_replicate():
    records = {(rep, cell): {row: {"answer": True, "raw_output": "Answer: True"}
                            for row in range(3)}
               for rep in (1, 2) for cell in control.CELLS}
    strict, period, excluded, changes = sensitivity_cells(records, 2, [0, 2])
    for cell in control.CELLS:
        assert set(excluded[2, cell]) == {1}
        assert set(excluded[1, cell]) == {0, 1, 2}
        assert set(strict[2, cell]) == {0, 1, 2}
        assert set(period[2, cell]) == {0, 1, 2}
    assert changes == []
