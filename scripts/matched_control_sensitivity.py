"""Offline, exploratory sensitivities; preserves the matched-control archive."""
import argparse
from datetime import datetime
import json
from pathlib import Path

from scripts import run_matched_control as control

DEFAULT_RUN = control.ROOT / "results/matched_control/n500_r4_s20261007"
DEFAULT_OUTPUT = control.ROOT / "audits/2026-10-08/matched_control_sensitivity.json"


def contrast_reports(cells, plan):
    reports = []
    p_values = {}
    for rep in range(1, plan["replicates"] + 1):
        contrasts = {name: control.compare(cells[rep, left], cells[rep, right])
                     for name, (left, right) in control.CONTRASTS.items()}
        for name, result in contrasts.items():
            p_value = result["mcnemar_exact_p"]
            p_values[f"{rep}:{name}"] = 1.0 if p_value is None else p_value
        reports.append({"replicate": rep, "contrasts": contrasts})
    adjusted = control.holm(p_values)
    for report in reports:
        for name, result in report["contrasts"].items():
            result["holm_p"] = adjusted[f"{report['replicate']}:{name}"] if result["n_valid_pairs"] else None
    return reports


def sensitivity_cells(records, excluded_replicate, excluded_rows):
    strict = {key: {row: record["answer"] for row, record in rows.items()}
              for key, rows in records.items()}
    period_tolerant = {key: rows.copy() for key, rows in strict.items()}
    changed = []
    for (rep, cell), rows in records.items():
        for row, record in rows.items():
            raw = record["raw_output"].strip()
            if record["answer"] is None and raw.endswith("."):
                answer = control.parse_answer(raw[:-1])
                if answer is not None:
                    period_tolerant[rep, cell][row] = answer
                    changed.append({"replicate": rep, "cell": cell,
                                    "row_id": row, "raw_output": record["raw_output"],
                                    "alternative_answer": answer})
    gap_excluded = {key: rows.copy() for key, rows in strict.items()}
    for cell in control.CELLS:
        for row in excluded_rows:
            del gap_excluded[excluded_replicate, cell][row]
    return strict, period_tolerant, gap_excluded, changed


def analyse(folder):
    plan = json.loads((folder / "plan.json").read_text(encoding="utf-8"))
    summary = json.loads((folder / "summary.json").read_text(encoding="utf-8"))
    plan_hash = control.sha256(control.canonical(plan))
    if summary["plan_sha256"] != plan_hash:
        raise ValueError("Summary does not match the saved plan")
    records = {(rep, cell): {} for rep in range(1, plan["replicates"] + 1)
               for cell in control.CELLS}
    checkpoints = []
    for job in plan["jobs"]:
        saved = control.read_completed(folder, plan, job)
        if saved is None:
            raise ValueError(f"Incomplete run: job {job['job']}")
        records[job["replicate"], job["cell"]].update(
            (record["row_id"], record) for record in saved["records"])
        checkpoints.append(saved)
    before, after = max(zip(checkpoints, checkpoints[1:]), key=lambda pair:
                        (datetime.fromisoformat(pair[1]["started_utc"])
                         - datetime.fromisoformat(pair[0]["finished_utc"])).total_seconds())
    left, right = before["job"], after["job"]
    if (left["replicate"], left["block"], left["row_ids"]) != (right["replicate"], right["block"], right["row_ids"]):
        raise ValueError("Largest execution gap is not within a shared row block")
    strict, period, excluded, changes = sensitivity_cells(records, left["replicate"], left["row_ids"])
    strict_reports = contrast_reports(strict, plan)
    expected = [{"replicate": rep["replicate"], "contrasts": rep["contrasts"]}
                for rep in summary["replicates"]]
    if strict_reports != expected:
        raise ValueError("Original paired results do not reproduce")
    return {
        "scope": "Exploratory analyses chosen after inspecting results; original strict analysis remains primary.",
        "plan_sha256": plan_hash,
        "analysis_source_sha256": control.sha256(Path(__file__).read_bytes()),
        "shared_analysis_source_sha256": control.sha256(Path(control.__file__).read_bytes()),
        "original_contrasts_reproduced": True,
        "inference": "Holm across the original 16 contrasts separately within each alternative; no correction across the two post-hoc alternatives or other analyst choices. Wilson intervals are conditional row-level summaries, not population inference.",
        "period_tolerant": {"rule": "Allow one terminal period after an otherwise valid Answer: True/False response.",
                            "changed_answers": changes,
                            "replicates": contrast_reports(period, plan)},
        "gap_excluded": {"replicate": left["replicate"], "block": left["block"],
                         "excluded_rows": left["row_ids"],
                         "before_job": left["job"], "after_job": right["job"],
                         "gap_minutes": (datetime.fromisoformat(after["started_utc"])
                                         - datetime.fromisoformat(before["finished_utc"])).total_seconds() / 60,
                         "rule": "Exclude the same row block from all four conditions of the affected replicate only; retain strict parsing.",
                         "replicates": contrast_reports(excluded, plan)},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, default=DEFAULT_RUN)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    folder = args.run_dir.resolve()
    output = args.output.resolve()
    if output.is_relative_to(folder):
        raise ValueError("Sensitivity output must be outside the original experiment directory")
    report = analyse(folder)
    output.parent.mkdir(parents=True, exist_ok=True)
    control.write_json(output, report)
    print(f"Saved exploratory sensitivities to {output}")


if __name__ == "__main__":
    main()
