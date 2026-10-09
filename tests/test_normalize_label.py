"""
Regression tests for harness.runner.normalize_label.

WHY THESE EXIST
---------------
`normalize_label` used to iterate `valid_labels`, which is a **set**, and keep
the last label that matched. Its docstring promised the last match *in the
text*. Those are different things, and the gap had two consequences:

  1. For a response naming two valid labels ("not positive, it is negative")
     the winner was decided by set iteration order, which has nothing to do
     with the text -- so the wrong label could be returned.
  2. Python randomises `str` hashing per process unless PYTHONHASHSEED is set,
     so set order -- and therefore the returned label -- could DIFFER BETWEEN
     RUNS OF THE SAME SCRIPT. In a project about reproducibility, that is the
     more serious half.

A scan of all 2178 stored raw responses across the 14 result CSVs (28 Sept 2026)
found ZERO that match more than one valid label, so the defect never fired and
no published number depends on it. These tests lock in both properties: the
historical behaviour is unchanged, and the hazard cannot return.

`test_no_set_iteration_order_dependence` is the one that would have caught the
original bug. If someone "simplifies" the implementation back into a plain
`for lbl in valid_labels` loop, it fails.
"""

import itertools
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from harness.runner import normalize_label

SENTIMENT = {"positive", "negative", "neutral"}
TOPIC = {"ML", "systems", "theory", "other"}


# --- behaviour that must not change -----------------------------------------

@pytest.mark.parametrize("raw,valid,expected", [
    ("positive", SENTIMENT, "positive"),
    ("Positive", SENTIMENT, "positive"),
    ("  NEGATIVE  ", SENTIMENT, "negative"),
    ("neutral", SENTIMENT, "neutral"),
    ("ML", TOPIC, "ML"),
    ("ml", TOPIC, "ML"),
    ("theory", TOPIC, "theory"),
    ("The sentiment of the review is negative.", SENTIMENT, "negative"),
    ("I would classify this as positive", SENTIMENT, "positive"),
    ("This paper is about systems", TOPIC, "systems"),
    ("", SENTIMENT, ""),
    ("   ", SENTIMENT, ""),
    ("something else entirely", SENTIMENT, "something else entirely"),
    ("mixed", SENTIMENT, "mixed"),          # the observed schema violation
])
def test_single_label_behaviour_unchanged(raw, valid, expected):
    assert normalize_label(raw, valid) == expected


def test_no_valid_labels_falls_back_to_lowered_text():
    assert normalize_label("  Whatever  ") == "whatever"
    assert normalize_label("Whatever", None) == "whatever"


def test_substring_does_not_match_without_word_boundary():
    # "MLops" must not be read as the label "ML"
    assert normalize_label("MLops", TOPIC) == "mlops"


# --- the semantics the docstring always promised ----------------------------

@pytest.mark.parametrize("raw,expected", [
    ("not positive, it is negative", "negative"),
    ("negative? no - positive", "positive"),
    ("neutral, or perhaps positive", "positive"),
    ("positive ... actually neutral", "neutral"),
])
def test_last_occurrence_in_text_wins(raw, expected):
    """Rightmost occurrence, not whichever the set happened to yield last."""
    assert normalize_label(raw, SENTIMENT) == expected


# --- the regression that matters --------------------------------------------

def test_implementation_module_stays_import_light():
    """
    harness/normalize.py must be loadable without pulling in the framework.

    This is MEASURED, not inspected. An earlier version of this test only grepped
    the module's own import lines, which passed while the real cost sat elsewhere:
    `from harness.normalize import ...` executes harness/__init__.py, which imports
    LOTUS. The suite stayed at 250s on Windows and the grep-based test happily
    reported everything was fine.

    So: load the file the way the determinism test does -- directly by path -- in a
    fresh interpreter, and assert the framework never reached sys.modules.
    """
    src = (ROOT / "harness" / "normalize.py").read_text(encoding="utf-8")
    forbidden = ("lotus", "litellm", "pandas", "numpy", "openai", "datasets", "torch")
    offenders = [ln for ln in src.splitlines()
                 if ln.startswith(("import ", "from "))
                 and any(f in ln.lower() for f in forbidden)]
    assert not offenders, (
        f"harness/normalize.py must stay import-light; found: {offenders}")

    code = (
        "import importlib.util, sys\n"
        "spec = importlib.util.spec_from_file_location('_norm', %r)\n"
        "mod = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(mod)\n"
        "leaked = sorted(m for m in sys.modules\n"
        "                if m.split('.')[0] in %r)\n"
        "print('LEAKED:' + ','.join(leaked))\n"
        % (str(ROOT / "harness" / "normalize.py"), forbidden)
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True,
                         text=True, env={**os.environ}, timeout=60)
    assert out.returncode == 0, f"probe failed: {out.stderr[-500:]}"
    line = [ln for ln in out.stdout.splitlines() if ln.startswith("LEAKED:")]
    assert line, f"probe produced no marker. stdout={out.stdout!r} stderr={out.stderr[-300:]}"
    leaked = [m for m in line[0][len("LEAKED:"):].split(",") if m]
    assert not leaked, (
        f"loading harness/normalize.py pulled in {leaked}. Something imports the "
        "framework -- check that the load bypasses harness/__init__.py.")


def test_no_set_iteration_order_dependence():
    """
    The result must not depend on how the set was constructed.

    The old implementation failed this: building the same set from different
    insertion orders could change which label won.
    """
    text = "not positive, it is negative"
    results = {normalize_label(text, set(p))
               for p in itertools.permutations(["positive", "negative", "neutral"])}
    assert results == {"negative"}, f"order-dependent: {results}"


@pytest.mark.parametrize("seed", ["0", "1", "2", "3", "5", "7", "11", "42"])
def test_stable_across_process_hash_seeds(seed):
    """
    The same input must normalise identically in separate processes.

    PYTHONHASHSEED changes `str` hashing and therefore set iteration order. Run
    the function in a fresh interpreter per seed and require one answer. The old
    implementation returned 'positive' under some seeds and 'negative' under
    others.

    PORTABILITY -- this test was broken once, learn from it. The first version
    passed `env={"PYTHONHASHSEED": seed, "PATH": "/usr/bin:/bin"}`, which
    REPLACES the whole environment. On Windows the child interpreter then cannot
    start at all (no SYSTEMROOT, no usable PATH), stdout comes back empty, and the
    assertion died with IndexError instead of saying what went wrong. It passed
    only on the Linux box it was written on.

    So: inherit os.environ and override exactly one variable, check the return
    code before the output, and put stderr in every failure message.
    """
    # Load harness/normalize.py DIRECTLY BY PATH, bypassing the package.
    #
    # `from harness.normalize import ...` is not enough: importing any submodule
    # executes harness/__init__.py, which does `from .runner import ...`, which
    # imports LOTUS. Moving the function into its own module therefore bought
    # nothing on its own -- the suite still took 250s on Windows because all 8
    # subprocesses paid for the framework through the package init. Loading the
    # file directly with importlib skips __init__.py entirely, so the child
    # imports nothing but `re`.
    code = (
        "import importlib.util, sys\n"
        "spec = importlib.util.spec_from_file_location('_norm', %r)\n"
        "mod = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(mod)\n"
        "assert 'lotus' not in sys.modules, 'framework leaked into the child'\n"
        "print(mod.normalize_label('not positive, it is negative',"
        " {'positive','negative','neutral'}))\n"
        % str(ROOT / "harness" / "normalize.py")
    )
    env = {**os.environ, "PYTHONHASHSEED": seed}
    try:
        out = subprocess.run([sys.executable, "-c", code], capture_output=True,
                             text=True, env=env, timeout=60)
    except subprocess.TimeoutExpired:
        pytest.fail(f"child interpreter did not finish within 60s (seed {seed}). "
                    "This test must import harness.normalize, not harness.runner -- "
                    "check that no framework import crept in.")
    assert out.returncode == 0, (
        f"child interpreter failed (seed {seed}, rc={out.returncode}). "
        f"stderr: {out.stderr[-600:]}")
    lines = [ln.strip() for ln in out.stdout.splitlines() if ln.strip()]
    assert lines, (f"child produced no stdout (seed {seed}). "
                   f"stderr: {out.stderr[-600:]}")
    assert lines[-1] == "negative", (
        f"seed {seed}: got {lines[-1]!r}, expected 'negative'. "
        f"full stdout {out.stdout!r}")


def test_longer_label_wins_at_equal_position():
    """Overlapping labels resolve to the more specific one, deterministically."""
    valid = {"ML", "ML systems"}
    assert normalize_label("this is about ML systems", valid) == "ML systems"
