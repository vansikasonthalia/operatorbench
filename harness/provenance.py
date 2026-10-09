"""
OperatorBench -- environment and configuration provenance for every run.

WHY THIS EXISTS
---------------
The paper claims reproducibility, and §Reproducibility promises a repo anyone can
re-run. But the findings depend on **LOTUS 1.2.4 internals specifically** -- the
two system prompts F7 rests on are implementation details of that version, and a
newer LOTUS could reword them without breaking any API. An unpinned environment
therefore makes the paper unreproducible *by accident*, and a run whose metadata
does not say which LOTUS produced it cannot be checked later at all.

An audit listed what was missing from `{run_id}_meta.json`: package versions,
temperature, the real seed behaviour, truncation, and a prompt/config version.
This module supplies all of it in one dict so it lands in every future run's
metadata without each script remembering to add it.

WHAT IT RECORDS, AND WHY EACH ITEM MATTERS
------------------------------------------
- **Package versions** (lotus, litellm, pandas, openai, datasets) read via
  `importlib.metadata`, which does not import the package. LOTUS is the one that
  can silently invalidate F7.
- **Seed behaviour, stated honestly.** The experiment's hosted configuration
  does not set a seed, and the runner does not pass one to LiteLLM. So replicates
  are independent executions, **not seeded repeats**, and `seed: null` in
  metadata means "no seed was sent", never "seed 0". The separate temperature
  fields record the framework default and any value observed on a live LM object;
  neither proves what the provider applied.
- **Truncation.** Inputs are cut to `max_text_chars` (4000). That window is also
  the window `derive_topic_label_v2.py` parsed the arXiv stamp from, so the
  number is load-bearing for the label proxy, not just cosmetic.
- **Prompt/config version.** A sha256 over the frozen predicates, map
  instructions and valid-label sets. If anyone edits a predicate, this hash
  changes, and a run's metadata will no longer match the runs it is being
  compared against. That is the point: the frozen-predicate rule becomes
  checkable instead of merely stated.
- **Label version**, delegated to `harness.labels`, so every result records which
  label column produced it.

Usage:
    from harness.provenance import run_provenance
    meta["provenance"] = run_provenance(max_text_chars=4000)
"""

from __future__ import annotations

import hashlib
import json
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

#: Packages whose version can change a result. LOTUS first -- it is the one whose
#: internals F7 depends on.
TRACKED_PACKAGES = ("lotus-ai", "litellm", "openai", "pandas", "datasets", "numpy")


def package_versions() -> dict:
    """Installed versions of the packages that can change a result.

    Uses importlib.metadata so nothing is imported and no side effects occur.
    A package that is absent records None rather than raising.
    """
    try:
        from importlib.metadata import PackageNotFoundError, version
    except ImportError:                                    # pragma: no cover
        return {"error": "importlib.metadata unavailable"}
    out = {}
    for name in TRACKED_PACKAGES:
        try:
            out[name] = version(name)
        except PackageNotFoundError:
            out[name] = None
    return out


def prompt_config_version() -> dict:
    """
    A hash over the frozen predicates, so "the predicates were not changed" is
    verifiable rather than asserted.

    Covers, per domain: the filter predicate, the map instruction, the map output
    column and the valid-label set. Deliberately excludes file paths and seeds,
    which are environment, not experimental condition.
    """
    try:
        sys.path.insert(0, str(ROOT))
        from configs.pipelines import PIPELINES
    except Exception as exc:                               # pragma: no cover
        return {"error": f"could not import PIPELINES: {exc}"}

    canonical = {}
    for domain in sorted(PIPELINES):
        cfg = PIPELINES[domain]
        canonical[domain] = {
            "filter_predicate": cfg["filter"]["predicate"]
            if isinstance(cfg.get("filter"), dict) else None,
            "map_instruction": cfg["map"]["instruction"],
            "map_output_col": cfg["map"]["output_col"],
            "valid_labels": sorted(cfg["map"]["valid_labels"]),
        }
    blob = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return {
        "prompt_config_sha256": hashlib.sha256(blob.encode()).hexdigest(),
        "domains": canonical,
        "note": ("If this sha changes, the frozen predicates changed and runs "
                 "either side of the change are not comparable (D-W2-03)."),
    }


def dataset_provenance() -> dict:
    """sha256 of each frozen dataset file, so a run is tied to exact inputs."""
    out = {}
    for rel in ("data/abstracts/abstracts_v1.csv",
                "data/abstracts/abstracts_v1_injection.csv",
                "data/abstracts/abstracts_v1_labels_v2.csv",
                "data/reviews/reviews_v1.csv"):
        p = ROOT / rel
        out[rel] = (hashlib.sha256(p.read_bytes()).hexdigest()
                    if p.exists() else None)
    return out


def lotus_temperature_default() -> dict:
    """
    The temperature default of the INSTALLED LOTUS, read from its own signature.

    Why this is not a hardcoded 0.0. The project has always asserted temperature
    0.0 without an observable source, and run metadata recorded `null` because
    this code never passes temperature -- so an audit correctly objected that
    nothing verified the value. Hardcoding 0.0 here would repeat the error with
    extra confidence.

    Instead this introspects `lotus.models.LM.__init__` in the pinned package and
    reports what the signature actually declares, with the source location. That
    is citable: in lotus-ai 1.2.4 it is `lotus/models/lm.py` (parameter
    `temperature: float = 0.0`, stored into `self.kwargs` and forwarded to
    litellm). If LOTUS is not importable the value is None and `source` says so --
    a missing observation is reported as missing, never as an assumption.

    NOTE the distinction the paper must preserve: this is the FRAMEWORK DEFAULT,
    read from the installed version. It is not a measurement of what the provider
    applied to a given call. The F12 ablation's two replicates, byte-identical on
    all 594 logged decisions, are *consistent with* temperature 0 but do not by
    themselves prove the parameter's value.
    """
    try:
        import inspect
        from lotus.models import LM
        sig = inspect.signature(LM.__init__)
        param = sig.parameters.get("temperature")
        if param is None:
            return {"value": None,
                    "source": "lotus.models.LM.__init__ has no `temperature` parameter",
                    "kind": "framework_default_unavailable"}
        default = None if param.default is inspect.Parameter.empty else param.default
        try:
            src_file = inspect.getsourcefile(LM) or "lotus/models/lm.py"
        except Exception:
            src_file = "lotus/models/lm.py"
        return {
            "value": default,
            "source": f"inspect.signature(lotus.models.LM.__init__)['temperature'].default",
            "source_file": src_file,
            "kind": "framework_default_read_from_installed_package",
            "caveat": ("This is the installed framework's declared default, not a "
                       "measurement of what the provider applied to any call. This "
                       "project never passes temperature explicitly."),
        }
    except Exception as exc:
        return {"value": None,
                "source": f"LOTUS not importable here ({type(exc).__name__})",
                "kind": "unobserved",
                "caveat": ("Temperature is UNOBSERVED in this capture. Do not "
                           "record it as 0.0 on the strength of project notes.")}


def seed_behaviour(backend: str, seed) -> dict:
    """
    Record the value configured on LOTUS separately from reproducibility.

    PipelineRunner forwards a non-null constructor seed to the LOTUS LM. That
    does not prove the backend honored it or that repeated outputs are
    deterministic. A null seed means no seed value was configured.
    """
    hosted = backend == "hosted"
    configured = seed is not None
    if not configured:
        semantics = "unseeded hosted executions" if hosted else "unseeded executions"
        why = "No seed value was configured on the LOTUS LM."
        verified = False
    else:
        semantics = (
            "seed configured on LOTUS LM; provider support and deterministic effect unverified"
        )
        why = (
            "PipelineRunner passed this seed to the LOTUS LM. This records the "
            "configured value, not proof that the backend applied it or that "
            "outputs are reproducible."
        )
        verified = None
    return {
        "seed_configured_on_lm": seed,
        "seeded_reproducibility_verified": verified,
        "replicate_semantics": semantics,
        "why": why,
    }


def run_provenance(
    temperature: float | None = None,
    max_text_chars: int | None = 4000,
    backend: str = "hosted",
    seed=None,
    cache_enabled: bool | None = None,
) -> dict:
    """The full provenance block to embed in a run's metadata."""
    try:
        from harness.labels import label_provenance
        labels = label_provenance()
    except Exception as exc:                               # pragma: no cover
        labels = {"error": f"label provenance unavailable: {exc}"}

    return {
        "captured_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "packages": package_versions(),
        "temperature_observed_on_lm": temperature,
        "temperature_framework_default": lotus_temperature_default(),
        "temperature_note": (
            "`temperature_observed_on_lm` is what could be read off the live LM "
            "object for this run; null means it could not be observed (for "
            "example when provenance is captured outside a run). "
            "`temperature_framework_default` is the installed LOTUS's declared "
            "default, read from its signature. Neither is a measurement of what "
            "the provider applied. This project never sets temperature."
        ),
        "max_text_chars": max_text_chars,
        "truncation_note": (
            "Inputs are truncated to max_text_chars. This is the same window "
            "derive_topic_label_v2.py parsed the arXiv stamp from, so the value "
            "is load-bearing for the label proxy."
        ),
        "cache_enabled": cache_enabled,
        "seed": seed_behaviour(backend, seed),
        "labels": labels,
        "prompt_config": prompt_config_version(),
        "datasets": dataset_provenance(),
        "evaluation_source_sha256": {
            rel: hashlib.sha256((ROOT / rel).read_bytes()).hexdigest()
            for rel in ("configs/pipelines.py", "harness/normalize.py", "harness/metrics.py")
        },
    }
