"""
OperatorBench -- the one supported way to load author-category proxy labels.

WHY THIS MODULE EXISTS
----------------------
There are two label columns for the abstracts dataset and reading the wrong one
silently produces wrong numbers:

  `topic_label`     -- **RETIRED.** Built by `prep_datasets.py` from
                       `ccdv/arxiv-classification` class indices. F9 established
                       that mapping was wrong: two of the four classes it called
                       "ML" were commutative algebra and control theory. Any
                       precision or recall figure computed against this column is
                       invalid.
  `topic_label_v2`  -- the corrected author-declared arXiv primary-category
                       proxy, parsed from the arXiv stamp inside the same
                       4000-char window the operators saw
                       (`scripts/derive_topic_label_v2.py`). External,
                       author-supplied, and not produced by us or by a model.

The frozen datasets still carry `topic_label`, deliberately: D-W3-05 keeps it so
the evidence trail of what the hand-check instrument displayed is not rewritten
after the fact. That decision is sound, but it leaves a loaded gun in the repo --
a future script can write `df.topic_label` and get F9-defective labels with no
error and no warning.

This module closes that gap. **New analysis code loads labels through here and
nowhere else.** Asking for the retired column requires saying so explicitly, and
`tests/test_label_version.py` fails the build if any script reads it directly.

WHAT YOU GET BACK
-----------------
`load_abstract_labels()` returns `(DataFrame, provenance)`. The provenance dict
is meant to be embedded verbatim in run metadata and in any summary JSON, so
every result records which label version produced it and the sha256 of the file
it came from. A number whose label version is not recorded is a number nobody can
check later.

Usage:
    from harness.labels import load_abstract_labels, ml_row_ids, labelled_row_ids

    labels, prov = load_abstract_labels()          # v2, the only sane default
    ml = ml_row_ids(labels)                        # author-declared ML rows
    known = labelled_row_ids(labels)               # excludes the 51 'unknown'
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
LABELS_CSV = ROOT / "data" / "abstracts" / "abstracts_v1_labels_v2.csv"
LABELS_MANIFEST = ROOT / "data" / "abstracts" / "manifest_labels_v2.json"

#: The current corrected proxy column. Analysis code should never hardcode this name.
CURRENT_LABEL_COL = "topic_label_v2"

#: The F9-defective column. Named here so the static test can find it by import
#: rather than by a string literal scattered across the codebase.
RETIRED_LABEL_COL = "topic_label"

#: Label version identifier recorded in provenance and run metadata.
LABEL_VERSION = "v2"

ML_VALUE = "machine_learning"
UNKNOWN_VALUE = "unknown"


class RetiredLabelError(RuntimeError):
    """Raised when the F9-defective label column is requested without consent."""


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def label_provenance() -> dict:
    """
    Provenance for the label file, for embedding in run metadata.

    Includes the sha256 so a result can be tied to the exact label file that
    produced it, and the F9 caveat so the proxy is never silently upgraded to
    "ground truth" downstream.
    """
    prov = {
        "label_version": LABEL_VERSION,
        "label_column": CURRENT_LABEL_COL,
        "label_file": str(LABELS_CSV.relative_to(ROOT)).replace("\\", "/"),
        "label_file_sha256": _sha256(LABELS_CSV) if LABELS_CSV.exists() else None,
        "retired_column_not_used": RETIRED_LABEL_COL,
        "caveat": (
            "Author-declared arXiv PRIMARY CATEGORY. This answers a different "
            "question from an 'is this about X?' filter predicate, so agreement "
            "with it is a proxy measurement, not filter accuracy."
        ),
    }
    if LABELS_MANIFEST.exists():
        try:
            m = json.loads(LABELS_MANIFEST.read_text())
            prov["label_manifest"] = {
                k: m[k] for k in ("generated", "method", "coverage", "label_counts",
                                  "sha256_prefix")
                if k in m
            }
        except (json.JSONDecodeError, OSError):
            prov["label_manifest"] = "unreadable"
    return prov


def load_abstract_labels(
    version: str = LABEL_VERSION,
    allow_retired_v1: bool = False,
) -> tuple[pd.DataFrame, dict]:
    """
    Load abstracts labels, indexed by row_id, together with their provenance.

    Args:
        version: "v2" (default, corrected proxy) or "v1" (the retired, F9-defective
            column). "v1" is refused unless `allow_retired_v1=True`, so it can
            never be selected by a typo or a copied default.
        allow_retired_v1: explicit consent to read the defective column. The
            only legitimate use is reproducing the F9 defect itself, as
            `scripts/derive_topic_label_v2.py` does when it reports v1-vs-v2
            disagreement.

    Returns:
        (labels, provenance). `labels` always exposes the requested version
        under the column name `topic_label` is NOT reused for -- v2 stays
        `topic_label_v2`, v1 stays `topic_label_v1` -- so a caller cannot mix
        them up by column name alone.

    Raises:
        RetiredLabelError: version="v1" without explicit consent.
        FileNotFoundError: the label file has not been generated.
    """
    if version not in ("v1", "v2"):
        raise ValueError(f"version must be 'v1' or 'v2', got {version!r}")

    if version == "v1" and not allow_retired_v1:
        raise RetiredLabelError(
            "Refusing to load 'topic_label' (v1). F9 established that this "
            "column's ML class is wrong -- two of its four 'ML' classes were "
            "commutative algebra and control theory -- so any precision or "
            "recall figure computed against it is invalid. Use version='v2' "
            "(the authors' declared arXiv category). If you genuinely need v1 "
            "to reproduce the F9 defect, pass allow_retired_v1=True and say in "
            "a comment why."
        )

    if not LABELS_CSV.exists():
        raise FileNotFoundError(
            f"{LABELS_CSV.relative_to(ROOT)} is missing. "
            "Run: python scripts/derive_topic_label_v2.py"
        )

    df = pd.read_csv(LABELS_CSV, index_col="row_id")
    prov = label_provenance()

    if version == "v1":
        if "topic_label_v1" not in df.columns:
            raise KeyError("topic_label_v1 not present in the label file")
        prov = {**prov, "label_version": "v1",
                "label_column": "topic_label_v1",
                "WARNING": "F9-DEFECTIVE COLUMN, read with explicit consent"}
    return df, prov


def labelled_row_ids(labels: pd.DataFrame) -> set[int]:
    """Rows carrying a declared category. Excludes 'unknown' (51 of 500)."""
    return set(labels[labels[CURRENT_LABEL_COL] != UNKNOWN_VALUE].index)


def ml_row_ids(labels: pd.DataFrame) -> set[int]:
    """Rows whose authors declared an ML primary category."""
    return set(labels[labels[CURRENT_LABEL_COL] == ML_VALUE].index)
