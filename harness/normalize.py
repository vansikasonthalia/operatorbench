"""
OperatorBench -- label normalisation, deliberately free of framework imports.

WHY THIS IS ITS OWN MODULE
--------------------------
`normalize_label` is a pure string function: no LOTUS, no litellm, no network, no
state. It used to live in `harness/runner.py`, which imports LOTUS at module
level -- so anything wanting the function paid for the whole framework.

That cost was not theoretical. `tests/test_normalize_label.py` verifies
cross-process determinism by running the function in a fresh interpreter under
eight different PYTHONHASHSEED values. With the function behind runner.py, each of
those eight subprocesses imported LOTUS (and pydantic, and litellm) just to
normalise one string: the suite took 4.5 minutes on Windows, looked hung, and was
interrupted by hand once. With the function here, each subprocess imports a single
stdlib-only module.

So: keep this module import-light. `re` is all it needs. Do not add a framework
import here to save a line elsewhere.

`harness.runner` re-exports `normalize_label`, and `harness/__init__.py` still
exports it, so every existing import path keeps working.
"""

import re

def normalize_label(raw: str, valid_labels: set[str] | None = None) -> str:
    """
    Extract a clean label from a potentially verbose model response.

    Examples:
        "The sentiment of the review is negative." -> "negative"
        "Positive"                                 -> "positive"
        "ML"                                       -> "ML"

    Strategy:
      1. If raw matches a valid label (case-insensitive), return it.
      2. Search for a valid label anywhere in the text: the label whose LAST
         occurrence sits furthest right wins ("the model restates, then answers").
      3. Fall back to stripped/lowered raw text.

    DETERMINISM — why this is not a plain `for lbl in valid_labels` loop
    -------------------------------------------------------------------
    `valid_labels` is a **set**. Iterating it yields an order that depends on
    the hash of its elements, and Python randomises `str` hashing per process
    unless PYTHONHASHSEED is fixed. The previous implementation kept the last
    label that matched *in iteration order* while its docstring promised the
    last match *in the text*. For any response mentioning two valid labels
    ("not positive, negative") that meant:

      - the wrong label could win, since iteration order is unrelated to the
        text, and
      - **the same response could normalise differently in two runs of the
        same script**, which is a reproducibility defect in a project whose
        subject is reproducibility.

    Scanned 2178 stored raw responses across all 14 result CSVs on 28 Sept 2026:
    **zero** match more than one valid label, so the defect was latent and no
    published number is affected. This rewrite is behaviour-preserving on all
    historical data (locked by a regression test) and removes the hazard for
    any future run. Do not "simplify" it back into a set loop.

    Ordering is fully determined: rightmost last-occurrence, then longest label
    (the more specific match), then alphabetical. No step consults set order.
    """
    raw = raw.strip()
    if not raw:
        return raw

    if valid_labels:
        raw_lower = raw.lower()
        for lbl in sorted(valid_labels):          # sorted: never set order
            if raw_lower == lbl.lower():
                return lbl

        best = None                                # (last_start, len, label)
        for lbl in sorted(valid_labels):
            pattern = r'\b' + re.escape(lbl) + r'\b'
            starts = [m.start() for m in re.finditer(pattern, raw, flags=re.IGNORECASE)]
            if not starts:
                continue
            key = (max(starts), len(lbl), lbl)
            if best is None or key > best[0]:
                best = (key, lbl)
        if best is not None:
            return best[1]

    return raw.lower()
