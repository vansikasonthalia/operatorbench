"""Run only offline analyses, redirecting their output into the audit folder."""
import contextlib
import argparse
import io
import json
from pathlib import Path
import runpy
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "audits/2026-10-07/recomputed"
JOBS = [("rq1_fleiss_kappa", []), ("f10_f11_frozen", []),
        ("f7_precision_paired", []), ("rq4_paired", []),
        ("rq3_uncertainty", []), ("check_handcheck_consistency", []),
        ("run_stamp_ablation", ["--aggregate-only"]),
        ("run_wrapper_test", ["--aggregate-only"])]


def differences(old, new, path=""):
    """Record numerical/result changes separately from added disclosures/prose."""
    found = []
    if isinstance(old, dict) and isinstance(new, dict):
        for key in sorted(old):
            if key in new:
                found.extend(differences(old[key], new[key], f"{path}.{key}"))
            elif not isinstance(old[key], str):
                found.append({"field": f"{path}.{key}", "old": old[key], "missing": True})
    elif isinstance(old, list) and isinstance(new, list):
        if len(old) != len(new):
            found.append({"field": path + ".length", "old": len(old), "new": len(new)})
        for i, (a, b) in enumerate(zip(old, new)):
            found.extend(differences(a, b, f"{path}[{i}]"))
    elif not isinstance(old, str) and (old != new or isinstance(old, bool) != isinstance(new, bool)):
        found.append({"field": path, "old": old, "new": new})
    return found


def recompute(output):
    output = output.resolve()
    if not output.is_relative_to(ROOT / "audits"):
        raise ValueError("Recomputed outputs must be under audits/")
    output.mkdir(parents=True, exist_ok=True)
    original_write = Path.write_text
    reports = []
    for name, arguments in JOBS:
        produced = []
        def redirected_write(path, data, *args, **kwargs):
            resolved = path.resolve()
            if not resolved.is_relative_to(ROOT / "results"):
                raise ValueError(f"Offline analysis attempted an unexpected write: {path}")
            target = output / path.name
            produced.append((resolved, target))
            return original_write(target, data, *args, **kwargs)
        stream = io.StringIO()
        status = 0
        try:
            with patch.object(Path, "write_text", redirected_write), patch.object(sys, "argv", [name, *arguments]), contextlib.redirect_stdout(stream):
                runpy.run_path(str(ROOT / f"scripts/{name}.py"), run_name="__main__")
        except SystemExit as exc:
            status = exc.code or 0
        except Exception as exc:
            status = 1
            stream.write(f"{type(exc).__name__}: {exc}\n")
        original_write(output / f"{name}.txt", stream.getvalue(), encoding="utf-8")
        comparisons = []
        for frozen, target in produced:
            changes = differences(json.loads(frozen.read_text()), json.loads(target.read_text())) if frozen.exists() else [{"missing_frozen_output": True}]
            comparisons.append({"frozen": frozen.relative_to(ROOT).as_posix(),
                                "recomputed": target.relative_to(ROOT).as_posix(),
                                "numeric_or_boolean_changes": changes})
        reports.append({"analysis": name, "exit_status": status, "comparisons": comparisons})
    original_write(output / "comparison.json", json.dumps(reports, indent=2), encoding="utf-8")
    return reports


def failed(reports):
    return any(r["exit_status"] or not r["comparisons"] or
               any(c["numeric_or_boolean_changes"] for c in r["comparisons"]) for r in reports)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUT)
    reports = recompute(parser.parse_args().output)
    print(json.dumps(reports, indent=2))
    return int(failed(reports))


if __name__ == "__main__":
    raise SystemExit(main())
