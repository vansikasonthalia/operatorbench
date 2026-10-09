"""Tests for the F13 stamp-removal control (scripts/run_stamp_ablation.py). No API calls."""

import pytest
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from run_stamp_ablation import blank_stamps, blanked_frame, load_rows  # noqa: E402


def test_stamp_and_date_blanked_with_equal_length():
    text = "arXiv:1512.00608v1 [math.AC] 2 Dec 2015\n\nON THE RING"
    new, n = blank_stamps(text)
    assert n == 1
    assert len(new) == len(text)
    assert "math.AC" not in new and "arXiv" not in new and "2015" not in new
    assert new.endswith("\n\nON THE RING")


def test_stamp_outside_window_is_left_alone():
    text = "x" * 4000 + "arXiv:1512.00608v1 [cs.LG] 2 Dec 2015"
    new, n = blank_stamps(text)
    assert n == 0 and new == text


def test_text_without_stamp_is_unchanged():
    text = "A paper with a bracket [12] and a citation arXiv preprint."
    assert blank_stamps(text) == (text, 0)


@pytest.mark.archive
def test_frozen_frame_audit_is_clean():
    df = load_rows()
    blanked, audit = blanked_frame(df)
    assert len(blanked) == 500
    assert audit["labelled_rows_blanked"] == audit["labelled_rows"] == 449
    assert audit["unknown_rows_changed"] == 0
    assert audit["rows_with_category_code_still_in_window"] == []
