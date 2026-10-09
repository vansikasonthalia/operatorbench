"""
Tests that the label-version choice is explicit and the retired column is fenced off.

THE HAZARD
----------
The frozen abstracts dataset carries two label columns. `topic_label` is
F9-defective: `prep_datasets.py` derived it from `ccdv/arxiv-classification`
class indices, and two of the four classes it labelled "ML" were commutative
algebra and control theory. `topic_label_v2` is the authors' own declared arXiv
primary category and is the only one any reported figure may use.

D-W3-05 deliberately keeps the defective column in the frozen data so the record
of what the hand-check instrument displayed is not rewritten after the fact. The
cost of that decision is a loaded gun: `df.topic_label` is valid Python that
returns wrong labels silently.

`test_no_script_reads_the_retired_column` is the fence. It reads the source of
every analysis script and fails if one touches the retired column outside a small
allowlist of files that legitimately must (the generator, the corrector, and the
config line the hand-check sheet reads). If someone adds `df.topic_label` to a new
analysis script, this test fails and names the file.

The allowlist is deliberately short and each entry carries its reason. Adding to
it should feel like a decision, because it is one.
"""

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from harness.labels import (
    CURRENT_LABEL_COL,
    RETIRED_LABEL_COL,
    LABEL_VERSION,
    RetiredLabelError,
    label_provenance,
    labelled_row_ids,
    load_abstract_labels,
    ml_row_ids,
)

# Files permitted to mention the retired column, with the reason each is exempt.
RETIRED_COL_ALLOWLIST = {
    "scripts/prep_datasets.py":
        "creates the column; frozen, never re-run (D-09)",
    "scripts/derive_topic_label_v2.py":
        "reads v1 deliberately to report v1-vs-v2 disagreement",
    "configs/pipelines.py":
        "ground_truth_col is retained by D-W3-05 for the hand-check evidence trail",
    "harness/labels.py":
        "this module is the fence itself",
    "scripts/filter_precision.py":
        "mentions it only in prose asserting it is NOT read",
}


def _source_files():
    for d in ("scripts", "harness", "configs"):
        for p in sorted((ROOT / d).glob("*.py")):
            yield p


def test_default_is_the_corrected_version():
    assert LABEL_VERSION == "v2"
    assert CURRENT_LABEL_COL == "topic_label_v2"
    _, prov = load_abstract_labels()
    assert prov["label_version"] == "v2"
    assert prov["label_column"] == "topic_label_v2"


def test_retired_version_refused_without_explicit_consent():
    with pytest.raises(RetiredLabelError) as e:
        load_abstract_labels("v1")
    assert "F9" in str(e.value)


def test_retired_version_available_with_explicit_consent_and_flagged():
    _, prov = load_abstract_labels("v1", allow_retired_v1=True)
    assert prov["label_version"] == "v1"
    assert "WARNING" in prov


def test_bad_version_rejected():
    with pytest.raises(ValueError):
        load_abstract_labels("v3")


def test_provenance_records_what_a_reader_needs():
    prov = label_provenance()
    for key in ("label_version", "label_column", "label_file",
                "label_file_sha256", "caveat"):
        assert prov.get(key), f"provenance missing {key}"
    # the proxy must never be described as ground truth without the caveat
    assert "proxy" in prov["caveat"].lower()
    assert len(prov["label_file_sha256"]) == 64


def test_row_id_helpers_match_the_known_dataset_shape():
    labels, _ = load_abstract_labels()
    assert len(labels) == 500
    assert len(labelled_row_ids(labels)) == 449      # 51 'unknown' excluded
    assert ml_row_ids(labels) <= labelled_row_ids(labels)


def test_no_script_reads_the_retired_column():
    """
    The fence. Fails, naming the file, if analysis code touches `topic_label`.

    Matches the bare column name only -- `topic_label_v2` and `topic_label_v1`
    are fine, since the negative lookahead excludes a following `_`.
    """
    pattern = re.compile(r"\b" + re.escape(RETIRED_LABEL_COL) + r"(?!_)\b")
    offenders = {}
    for path in _source_files():
        rel = path.relative_to(ROOT).as_posix()
        if rel in RETIRED_COL_ALLOWLIST:
            continue
        hits = [i for i, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), 1)
            if pattern.search(line)]
        if hits:
            offenders[rel] = hits
    assert not offenders, (
        f"These files read the F9-defective '{RETIRED_LABEL_COL}' column: "
        f"{offenders}. Load labels via harness.labels.load_abstract_labels() "
        f"instead. If a file genuinely needs the retired column, add it to "
        f"RETIRED_COL_ALLOWLIST in this test with a stated reason."
    )


def test_allowlist_entries_all_still_exist_and_still_need_the_exemption():
    """An allowlist that outlives its reasons is how fences rot."""
    stale = []
    pattern = re.compile(r"\b" + re.escape(RETIRED_LABEL_COL) + r"(?!_)\b")
    for rel in RETIRED_COL_ALLOWLIST:
        p = ROOT / rel
        if not p.exists():
            stale.append(f"{rel} (file gone)")
        elif not pattern.search(p.read_text(encoding="utf-8")):
            stale.append(f"{rel} (no longer mentions it -- drop the exemption)")
    assert not stale, f"Stale allowlist entries: {stale}"
