"""Recompute saved evidence without model calls or writes to experiment archives."""
import argparse
import contextlib
from datetime import datetime, timezone
from importlib.metadata import version
import io
import json
from pathlib import Path
import platform
import socket
import sys
from unittest.mock import patch

from harness.admission import build_ledger
from scripts.admission_ledger_example import example
from scripts import audit_frozen_evidence as audit
from scripts import audit_revision_contracts as contracts
from scripts import compare_admission_reporting as reporting
from scripts import matched_control_sensitivity as sensitivity
from scripts import recompute_evidence_audit as historical
from scripts import run_matched_control as control

ROOT = control.ROOT
DEFAULT_OUTPUT = ROOT / "audits/reproduction"


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def inventory():
    folders = ("configs", "harness", "scripts", "tests", "docs", "paper", "data", "results", "logs",
               "audits/2026-10-07", "audits/2026-10-08")
    files = [ROOT / name for name in ("README.md", "requirements.txt", "requirements.lock.txt",
                                     "requirements-artifact.txt", "logs_manifest.json")]
    for folder in folders:
        directory = ROOT / folder
        if not directory.is_dir():
            raise FileNotFoundError(directory)
        files.extend(p for p in directory.rglob("*") if p.is_file()
                     and "__pycache__" not in p.parts and p.suffix != ".pyc")
    return {p.relative_to(ROOT).as_posix(): {"sha256": audit.digest(p), "bytes": p.stat().st_size}
            for p in sorted(files)}


def admission_ledger():
    universe = set(audit.indexed(audit.read_csv(ROOT / "data/abstracts/abstracts_v1.csv", source=True)))
    reports = []
    for rep in (1, 2):
        before = audit.indexed(audit.read_csv(ROOT / f"results/rq3_abstracts_rep{rep}_map.csv"))
        after = audit.indexed(audit.read_csv(ROOT / f"results/f7_ablation_abstracts_rep{rep}_map.csv"))
        ledger = build_ledger(before, after, universe, label_column="mapped_topic", raw_column="_raw_mapped_topic",
                              valid_labels={"ML", "systems", "theory", "other"},
                              disagreement=lambda label: audit.disagreement("abstracts", label))
        legacy = {"replicate": rep, "common_rows": len(ledger["sets"]["common"]),
                  "excluded_rows": len(ledger["sets"]["excluded"]), "new_rows": len(ledger["sets"]["newly_admitted"]),
                  "saved_label_changes": sum(t["count"] for t in ledger["label_transitions"]["saved"] if t["before"] != t["after"]),
                  "raw_label_changes": sum(t["count"] for t in ledger["label_transitions"]["raw"] if t["before"] != t["after"]),
                  "excluded_disagreements": ledger["terms"]["removed_by_exclusions"]}
        reports.append({**legacy, **ledger, "disagreement_rule": "Every saved label other than ML, ignoring case and outer whitespace, including invalid labels."})
    return reports


def matched_summary(folder):
    plan = audit.load_json(folder / "plan.json")
    frame = control.load_rows(len(plan["row_ids"]))
    windows = frame["abstract_text"].str[:plan["max_text_chars"]]
    bindings = {"dataset_sha256": audit.digest(control.DATA),
                "input_sha256": control.sha256(control.canonical(list(zip(frame.index.tolist(), windows)))),
                "source_sha256": audit.digest(Path(control.__file__))}
    for key, checksum in bindings.items():
        if plan[key] != checksum:
            raise ValueError(f"Matched-control {key} does not match the saved plan")
    if plan["jobs"] != control.schedule(plan["row_ids"], plan["replicates"], plan["block_size"], plan["schedule_seed"]):
        raise ValueError("Matched-control schedule does not reproduce")
    summary = control.analyse(folder, plan)
    if summary != audit.load_json(folder / "summary.json"):
        raise ValueError("Matched-control summary does not reproduce exactly")
    return summary


def tables(ledger, matched, robustness):
    lines = ["# Recomputed table inputs", "", "Historical table inputs are in `historical/*.json`; these tables show the admission ledger and all matched-control contrasts.", "",
             "| Replicate | Common | Excluded | New | Saved label changes | Raw label changes | Excluded disagreements |",
             "|---|---:|---:|---:|---:|---:|---:|"]
    for report in ledger:
        fields = ("replicate", "common_rows", "excluded_rows", "new_rows", "saved_label_changes", "raw_label_changes", "excluded_disagreements")
        lines.append("| " + " | ".join(str(report[key]) for key in fields) + " |")
    lines += ["", "## Full admission accounting", "",
              "| Replicate | Before D/n | After D/n | Never admitted | New-admission disagreements | Excluded disagreements | Common added / resolved | Count change | Identity |",
              "|---|---|---|---:|---:|---:|---|---:|---|"]
    for report in ledger:
        terms = report["terms"]
        lines.append(f"| {report['replicate']} | {report['before']['disagreements']}/{report['before']['admitted']} | {report['after']['disagreements']}/{report['after']['admitted']} | {len(report['sets']['never_admitted'])} | {terms['introduced_by_new_admissions']} | {terms['removed_by_exclusions']} | {terms['common_rows_new_disagreements']} / {terms['common_rows_resolved_disagreements']} | {report['identity']['observed_count_change']} | {report['identity']['holds']} |")
    for report in ledger:
        lines += ["", f"### Replicate {report['replicate']} common-row label transitions", "",
                  "| Layer | Before | After | Rows |", "|---|---|---|---:|"]
        for layer, changes in report["label_transitions"].items():
            lines += [f"| {layer} | {t['before']} | {t['after']} | {t['count']} |" for t in changes]
        lines += ["", f"Raw invalid counts before/after: {report['before']['invalid_outputs']['raw']['count']} / {report['after']['invalid_outputs']['raw']['count']}. Saved invalid counts before/after: {report['before']['invalid_outputs']['saved']['count']} / {report['after']['invalid_outputs']['saved']['count']}."]
    demonstration = example()["ledger"]
    lines += ["", "## Synthetic procedure example", "", "`admission_ledger_example.json` contains constructed records, not experimental observations.", "",
              "Both configurations admit 5 rows and count 3 disagreements. The ledger has 1 introduced disagreement, 1 removed disagreement, and opposing common-row changes (1 new, 1 resolved). Net change: 1 - 1 + (1 - 1) = 0, despite 2 saved-label and 4 raw-output changes.", "",
              "| Layer | Invalid before | Invalid after |", "|---|---:|---:|"]
    for layer in ("raw", "saved"):
        lines.append(f"| {layer} | {demonstration['before']['invalid_outputs'][layer]['count']} | {demonstration['after']['invalid_outputs'][layer]['count']} |")
    for name, reports in (("Primary strict analysis", matched["replicates"]),
                          ("Exploratory terminal-period alternative", robustness["period_tolerant"]["replicates"]),
                          ("Exploratory gap-block exclusion", robustness["gap_excluded"]["replicates"])):
        lines += ["", f"## {name}", "", "| Replicate | Contrast | Valid pairs | Left only | Right only | Difference % | Holm p |",
                  "|---|---|---:|---:|---:|---:|---:|"]
        for report in reports:
            for contrast, result in report["contrasts"].items():
                lines.append(f"| {report['replicate']} | {contrast} | {result['n_valid_pairs']} | {result['left_true_only']} | {result['right_true_only']} | {100 * result['difference_rate']:.2f} | {result['holm_p']:.8g} |")
    lines += ["", robustness["inference"], ""]
    return "\n".join(lines)


def run(output):
    output = output.resolve()
    if output == ROOT / "audits" or not output.is_relative_to(ROOT / "audits") or any(
            output.is_relative_to(ROOT / name) for name in ("audits/2026-10-07", "audits/2026-10-08")):
        raise ValueError("Use a separate output directory under audits/, outside the dated audits")
    output.mkdir(parents=True, exist_ok=True)
    write_json(output / "report.json", {"passed": False, "status": "running"})
    before = inventory()
    write_json(output / "manifest.json", {"algorithm": "SHA-256", "scope": "scientific source, documentation, saved data/results/logs and dated audits; excludes generated reproduction outputs and local environments", "files": before})
    with patch.object(socket.socket, "connect", side_effect=RuntimeError("Network access is disabled during offline reproduction")), \
            patch.object(socket.socket, "connect_ex", side_effect=RuntimeError("Network access is disabled during offline reproduction")):
        print("Recomputing eight historical analyses...", flush=True)
        reports = historical.recompute(output / "historical")
        stream = io.StringIO()
        print("Auditing the frozen rows and historical logs...", flush=True)
        with patch.object(sys, "argv", ["audit_frozen_evidence", "--output", str(output / "historical_audit")]), contextlib.redirect_stdout(stream):
            audit.main()
        evidence = audit.load_json(output / "historical_audit/evidence_audit.json")
        print("Recomputing the admission ledger, matched control and sensitivities...", flush=True)
        ledger = admission_ledger()
        matched = matched_summary(sensitivity.DEFAULT_RUN)
        robustness = sensitivity.analyse(sensitivity.DEFAULT_RUN)
        comparison = reporting.analyse(ledger)
        requirements = contracts.analyse(comparison)
        write_json(output / "admission_ledger.json", ledger)
        write_json(output / "admission_ledger_example.json", example())
        write_json(output / "matched_control_summary.json", matched)
        write_json(output / "matched_control_sensitivity.json", robustness)
        write_json(output / "admission_reporting_comparison.json", comparison)
        (output / "REPORTING-COMPARISON.md").write_text(reporting.markdown(comparison), encoding="utf-8")
        write_json(output / "revision_contracts.json", requirements)
        (output / "REVISION-CONTRACTS.md").write_text(contracts.markdown(requirements), encoding="utf-8")
        (output / "TABLES.md").write_text(tables(ledger, matched, robustness), encoding="utf-8")
    saved_ledger = audit.load_json(ROOT / "audits/2026-10-08/admission_ledger_check.json")
    checks = {
        "historical_analyses_reproduced": not historical.failed(reports),
        "historical_headline_counts_reproduced": all(c["match"] for c in evidence["claims"]),
        "historical_rows_structurally_consistent": not any(r["issues"] for r in evidence["runs"]),
        "historical_log_manifest_matches": not any(evidence["manifest_check"][key] for key in ("missing", "extra", "changed")),
        "admission_ledger_reproduced": [{key: report[key] for key in saved}
                                       for report, saved in zip(ledger, saved_ledger)] == saved_ledger,
        "matched_control_reproduced": True,
        "sensitivities_reproduced": robustness == audit.load_json(ROOT / "audits/2026-10-08/matched_control_sensitivity.json"),
        "reporting_comparison_reconciled": all(comparison["checks"].values()),
        "archive_unchanged": evidence["frozen_files_unchanged"] and before == inventory(),
    }
    gaps = {"invalid_historical_log_lines": sum(len(log["invalid_lines"]) for log in evidence["logs"]),
            "runs_with_provenance_gaps": sum(bool(run["gaps"]) for run in evidence["runs"]),
            "limitations": evidence["limitations"]}
    report = {"completed_utc": datetime.now(timezone.utc).isoformat(), "passed": all(checks.values()),
              "checks": checks, "historical_evidence_gaps": gaps,
              "analysis_environment": {"python": platform.python_version(), "platform": platform.platform(),
                                       "packages": {name: version(name) for name in ("pandas", "numpy")}},
              "scope": "Reanalysis of recorded outputs, not a model rerun, independent correctness assessment, or PDF compilation."}
    write_json(output / "report.json", report)
    print(json.dumps(report, indent=2))
    return int(not report["passed"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return run(parser.parse_args().output)


if __name__ == "__main__":
    raise SystemExit(main())
