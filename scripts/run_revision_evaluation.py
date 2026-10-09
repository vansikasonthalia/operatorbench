"""Execute or reanalyse the fixed revision evaluation; defaults to offline preflight."""
import argparse
from collections import Counter
from datetime import datetime, timezone
from importlib.metadata import distribution, version
import json
import os
from pathlib import Path
import platform
import socket
import time
from unittest.mock import patch

import pandas as pd

from harness.admission import build_ledger
from harness.revision import check_revision
from harness.revision_assertions import check_records
from scripts import audit_frozen_evidence as audit
from scripts import compare_admission_reporting as reporting
from scripts import prepare_revision_evaluation as preparation
from scripts import run_matched_control as control

ROOT = control.ROOT
DEFAULT_RUN = ROOT / "results/revision_evaluation/n100_r4_s20261008"
FRAMEWORK_FILES = ("lotus/templates/task_instructions.py", "lotus/sem_ops/sem_filter.py",
                   "lotus/sem_ops/sem_map.py", "lotus/sem_ops/postprocessors.py", "lotus/models/lm.py")


def load_frame(plan):
    rows = audit.indexed(audit.read_csv(control.DATA, source=True))
    return pd.DataFrame({"abstract_text": [rows[row]["abstract_text"][:plan["max_text_chars"]]
                                          for row in plan["row_ids"]]}, index=plan["row_ids"])


def evaluate_cell(frame, cell, plan):
    instruction = plan["instructions"][cell]
    if cell.endswith("_filter"):
        result = frame.sem_filter(instruction, return_all=True, return_raw_outputs=True,
                                  suffix="decision", strategy=None, cascade_args=None)
    else:
        result = frame.sem_map(instruction, return_raw_outputs=True, suffix="decision", strategy=None)
    if not result.index.is_unique or set(result.index) != set(frame.index):
        raise ValueError("Operator did not return each planned row exactly once")
    return [{"row_id": int(row), "raw_output": str(result.loc[row, "raw_outputdecision"]),
             "answer": control.parse_answer(result.loc[row, "raw_outputdecision"]),
             "operator_output": str(result.loc[row, "decision"])} for row in frame.index]


def preflight(plan, frame):
    import lotus
    from lotus.models import LM
    from lotus.types import LMOutput

    lm = LM(model=plan["model"], temperature=plan["temperature_requested"],
            max_tokens=plan["max_output_tokens"], max_batch_size=plan["block_size"])
    prompts = {}
    captured = []

    def capture(self, messages, **kwargs):
        captured.extend(messages)
        return LMOutput(outputs=["Answer: False"] * len(messages))

    with patch.object(LM, "__call__", capture), lotus.settings.context(lm=lm, enable_cache=False):
        for cell in plan["questions"]:
            captured.clear()
            evaluate_cell(frame, cell, plan)
            if len(captured) != len(frame):
                raise ValueError("Native prompt coverage differs from the input rows")
            hashes = [control.sha256(control.canonical(messages)) for messages in captured]
            if len(set(hashes)) != len(hashes):
                raise ValueError("Duplicate row prompts prevent an unambiguous request join")
            prompts[cell] = {str(row): messages for row, messages in zip(frame.index, captured)}
    manifest = {
        "protocol_sha256": control.sha256(control.canonical(plan)),
        "runner_source_sha256": audit.digest(Path(__file__)),
        "interceptor_source_sha256": audit.digest(ROOT / "harness/intercept.py"),
        "python": platform.python_version(),
        "packages": {name: version(name) for name in ("lotus-ai", "litellm", "pandas")},
        "framework_source_sha256": {name: audit.digest(Path(distribution("lotus-ai").locate_file(name)))
                                    for name in FRAMEWORK_FILES},
        "native_prompts_sha256": control.sha256(control.canonical(prompts)),
        "requested_lm_kwargs": {"model": lm.model, "temperature": lm.kwargs["temperature"],
                                "max_completion_tokens": lm.kwargs["max_completion_tokens"]},
        "analysis_evidence_rule": "Revision-specific assessability: both filters must be valid; a map must be valid only if its corresponding filter admits. Unneeded map format failures remain recorded. Required filter/map format failures violate output validity even when semantic checks are unassessed. Known filter membership exceptions remain visible outside the joint assessable cohort.",
    }
    return manifest, prompts


def join_requests(folder, request_file, records, job, plan, prompts):
    path = folder / request_file
    if path.parent != folder:
        raise ValueError("Request log must belong to the run directory")
    calls = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    expected = {control.sha256(control.canonical(prompts[job["cell"]][str(row)])): row
                for row in job["row_ids"]}
    rows = {r["row_id"]: r for r in records}
    joins = {}
    for line, call in enumerate(calls, 1):
        checksum = control.sha256(control.canonical(call["messages"]))
        if checksum not in expected or call["model"] != plan["model"]:
            raise ValueError("Request does not match the bound native prompt or model")
        row = expected[checksum]
        if row in joins or call["response_text"] != rows[row]["raw_output"]:
            raise ValueError("Request response does not join uniquely to the saved row")
        joins[row] = {"request_line": line, "native_prompt_sha256": checksum}
    if set(joins) != set(job["row_ids"]) or len(calls) != len(job["row_ids"]):
        raise ValueError("Request coverage mismatch; block cannot be marked complete")
    return joins


def read_completed(folder, plan, job, manifest, prompts):
    saved = control.read_completed(folder, plan, job)
    if saved is None:
        return None
    if saved["run_manifest_sha256"] != control.sha256(control.canonical(manifest)):
        raise ValueError("Checkpoint does not match the execution binding")
    joins = join_requests(folder, saved["request_file"], saved["records"], job, plan, prompts)
    if any(record["request_join"] != joins[record["row_id"]] for record in saved["records"]):
        raise ValueError("Checkpoint request-to-row join changed")
    return saved


def bind_run(folder, plan, manifest, prompts):
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / "run_manifest.json"
    if path.exists():
        if audit.load_json(path) != manifest or audit.load_json(folder / "plan.json") != plan:
            raise ValueError("Existing execution binding differs; use a new run directory")
        if audit.load_json(folder / "native_prompts.json") != prompts:
            raise ValueError("Existing native prompts changed")
    else:
        if any(p.name != ".run.lock" for p in folder.iterdir()):
            raise ValueError("Run directory contains files without a complete execution binding")
        control.write_json(folder / "plan.json", plan)
        control.write_json(folder / "native_prompts.json", prompts)
        control.write_json(path, manifest)


def execute(folder, plan, frame, manifest, prompts):
    from harness.intercept import LLMInterceptor
    import lotus
    from lotus.models import LM

    if not os.environ.get("OPENAI_API_KEY"):
        raise ValueError("OPENAI_API_KEY is required for execution")
    folder.mkdir(parents=True, exist_ok=True)
    lock = folder / ".run.lock"
    with lock.open("x", encoding="utf-8") as stream:
        stream.write(str(os.getpid()))
    try:
        bind_run(folder, plan, manifest, prompts)
        lm = LM(model=plan["model"], temperature=plan["temperature_requested"],
                max_tokens=plan["max_output_tokens"], max_batch_size=plan["block_size"])
        with lotus.settings.context(lm=lm, enable_cache=False):
            for job in plan["jobs"]:
                if read_completed(folder, plan, job, manifest, prompts) is not None:
                    continue
                attempt = len(list(folder.glob(f"attempt_{job['job']:04d}_*.json"))) + 1
                tag = f"job_{job['job']:04d}_{attempt:03d}"
                state_path = folder / f"attempt_{job['job']:04d}_{attempt:03d}.json"
                state = {"job": job, "attempt": attempt, "started_utc": datetime.now(timezone.utc).isoformat(),
                         "status": "started", "run_manifest_sha256": control.sha256(control.canonical(manifest))}
                control.write_json(state_path, state)
                interceptor = LLMInterceptor(log_dir=folder, tag=tag)
                interceptor.install()
                try:
                    records = evaluate_cell(frame.loc[job["row_ids"]], job["cell"], plan)
                    request_file = f"llm_calls_{tag}.jsonl"
                    joins = join_requests(folder, request_file, records, job, plan, prompts)
                    for record in records:
                        record["request_join"] = joins[record["row_id"]]
                    control.write_json(folder / f"job_{job['job']:04d}.json", {
                        "plan_sha256": control.sha256(control.canonical(plan)), "job": job,
                        "run_manifest_sha256": state["run_manifest_sha256"],
                        "started_utc": state["started_utc"], "finished_utc": datetime.now(timezone.utc).isoformat(),
                        "attempt": attempt, "records": records, "records_sha256": control.sha256(control.canonical(records)),
                        "request_file": request_file, "request_sha256": audit.digest(folder / request_file),
                        "lm_kwargs": manifest["requested_lm_kwargs"], "usage": interceptor.summary(),
                    })
                    state["status"] = "completed"
                except BaseException as error:
                    state.update(status="failed", error_type=type(error).__name__)
                    raise
                finally:
                    interceptor.uninstall()
                    state.update(finished_utc=datetime.now(timezone.utc).isoformat(), usage=interceptor.summary())
                    control.write_json(state_path, state)
                print(f"Completed {job['job'] + 1}/{len(plan['jobs'])}: {job['cell']}", flush=True)
                if job != plan["jobs"][-1]:
                    time.sleep(plan["batch_delay_seconds"])
    finally:
        lock.unlink()


def rule_evidence(checks):
    result = {}
    for name, value in checks.items():
        if name == "output_validity":
            ids = sorted({row for side in value["invalid_outputs"].values()
                          for layer in side.values() for row in layer["row_ids"]})
        else:
            ids = value["unassessed_rows"] if name == "complete_input" else value["row_ids"]
        result[name] = {"status": value["status"], "row_ids": ids}
    return result


def revision_report(cells, revision, universe):
    required = set(revision["before"] + revision["after"])
    invalid = {cell: sorted(row for row in universe if cells[cell][row]["answer"] is None) for cell in sorted(required)}
    unassessed, needed_invalid = set(), set()
    for filter_cell, map_cell in (revision["before"], revision["after"]):
        for row in universe:
            decision = cells[filter_cell][row]["answer"]
            if decision is None or (decision and cells[map_cell][row]["answer"] is None):
                unassessed.add(row)
                needed_invalid.add(row)
    eligible = sorted(set(universe) - unassessed)

    def admitted(pair):
        filter_cell, map_cell = pair
        return {row: {"label": str(cells[map_cell][row]["answer"]), "raw": cells[map_cell][row]["raw_output"]}
                for row in eligible if cells[filter_cell][row]["answer"]}

    before, after = admitted(revision["before"]), admitted(revision["after"])
    predicate = lambda label: label == "False"
    ledger = build_ledger(before, after, eligible, label_column="label", raw_column="raw",
                          valid_labels={"True", "False"}, disagreement=predicate)
    for side, records in (("before", before), ("after", after)):
        ids = sorted(row for row, record in records.items() if control.parse_answer(record["raw"]) is None)
        ledger[side]["invalid_outputs"]["raw"] = {"count": len(ids), "row_ids": ids}
    ledger["measurement"]["validity_rule"] = "Strict Answer: True/False raw parser; exact True/False saved labels. Required invalid responses outside the joint assessable cohort are reported and checked separately."
    contract = {key: revision[key] for key in ("admission_relation", "preserve_common_labels",
                                               "require_valid_outputs", "require_complete_input")}
    audited = check_revision(ledger, **contract, unassessed_rows=unassessed)
    direct = check_records(before, after, eligible, label_column="label", **contract,
                           unassessed_rows=unassessed, disagreement=predicate,
                           saved_valid=lambda label: label in {"True", "False"},
                           raw_valid=lambda record: control.parse_answer(record["raw"]) is not None)
    left_filter, right_filter = revision["before"][0], revision["after"][0]
    known_exceptions = []
    known_pairs = []
    for row in universe:
        left, right = cells[left_filter][row]["answer"], cells[right_filter][row]["answer"]
        if left is not None and right is not None:
            known_pairs.append(row)
            if ((contract["admission_relation"] == "equal" and left != right) or
                    (contract["admission_relation"] == "subset" and right and not left) or
                    (contract["admission_relation"] == "superset" and left and not right)):
                known_exceptions.append(row)
    supplemental = {
        "required_response_format": {"status": "violated" if needed_invalid else "satisfied", "row_ids": sorted(needed_invalid)},
        "known_filter_membership": {"status": "violated" if known_exceptions else
                                    "satisfied" if known_pairs else "not_evaluable",
                                    "row_ids": sorted(known_exceptions)},
    }
    for result in (audited, direct):
        result["checks"].update(supplemental)
        if any(value["status"] == "violated" for value in supplemental.values()):
            result["status"] = "violated"
    agreement = audited["status"] == direct["status"] and rule_evidence(audited["checks"]) == direct["checks"]
    if not agreement:
        raise ValueError("Ledger checker and direct assertions disagree; resolve before interpretation")
    return {"required_cells": sorted(required), "invalid_responses_by_cell": invalid,
            "known_filter_pair_rows": known_pairs,
            "unassessed_rows": sorted(unassessed), "views": reporting.reporting_views(ledger, predicate),
            "ledger_verdict": audited, "direct_assertions": direct, "baseline_agrees": agreement,
            "scope": "Core ledger checks describe the joint assessable cohort. Supplemental checks retain known required format and filter-membership exceptions on the original input; invalid answers are never recoded. Unneeded map failures are reported but do not exclude filter-negative rows."}


def analyse(folder, plan, manifest, prompts):
    cells = {(rep, cell): {} for rep in range(1, plan["replicates"] + 1) for cell in plan["questions"]}
    usage = Counter()
    for job in plan["jobs"]:
        saved = read_completed(folder, plan, job, manifest, prompts)
        if saved is None:
            raise ValueError(f"Missing job {job['job']}; incomplete experiment cannot be aggregated")
        cells[job["replicate"], job["cell"]].update((record["row_id"], record) for record in saved["records"])
        usage.update({key: saved["usage"][key] for key in
                      ("total_calls", "total_prompt_tokens", "total_completion_tokens")})
    reports = []
    for rep in range(1, plan["replicates"] + 1):
        rows = {cell: cells[rep, cell] for cell in plan["questions"]}
        if any(set(records) != set(plan["row_ids"]) for records in rows.values()):
            raise ValueError("Completed cells do not cover the planned input")
        reports.append({"replicate": rep, "revisions": {name: revision_report(rows, revision, plan["row_ids"])
                                                        for name, revision in plan["revisions"].items()}})
    attempts = [audit.load_json(path) for path in sorted(folder.glob("attempt_*.json"))]
    return {"protocol_sha256": manifest["protocol_sha256"], "run_manifest_sha256": control.sha256(control.canonical(manifest)),
            "analysis_source_sha256": audit.digest(Path(__file__)), "analysis": plan["analysis"],
            "replicates": reports, "successful_checkpoint_usage": dict(usage), "attempts": attempts,
            "limitations": "Descriptive revision views, not correctness or detection accuracy. Successful usage excludes failed attempts, whose available usage is retained separately. Unintercepted provider retries or failed calls may incur additional usage. Full raw logs, timestamps and attempt history must accompany interpretation."}


def load_binding(folder, plan):
    manifest = audit.load_json(folder / "run_manifest.json")
    prompts = audit.load_json(folder / "native_prompts.json")
    if audit.load_json(folder / "plan.json") != plan or manifest["protocol_sha256"] != control.sha256(control.canonical(plan)):
        raise ValueError("Saved run does not match the frozen protocol")
    if manifest["runner_source_sha256"] != audit.digest(Path(__file__)):
        raise ValueError("Runner source changed since execution")
    if manifest["native_prompts_sha256"] != control.sha256(control.canonical(prompts)):
        raise ValueError("Bound native prompts changed")
    return manifest, prompts


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--aggregate-only", action="store_true")
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--protocol-dir", type=Path, default=preparation.DEFAULT_OUTPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_RUN)
    args = parser.parse_args(argv)
    if args.execute and not args.yes:
        parser.error("Paid execution requires --execute --yes")
    plan = preparation.verify(args.protocol_dir.resolve())
    folder = args.output_dir.resolve()
    if folder == ROOT / "results/revision_evaluation" or not folder.is_relative_to(ROOT / "results/revision_evaluation"):
        parser.error("Use a run directory under results/revision_evaluation/")
    if args.aggregate_only:
        manifest, prompts = load_binding(folder, plan)
        control.write_json(folder / "summary.json", analyse(folder, plan, manifest, prompts))
        return
    frame = load_frame(plan)
    with patch.object(socket.socket, "connect", side_effect=RuntimeError("Network disabled during native preflight")), \
            patch.object(socket.socket, "connect_ex", side_effect=RuntimeError("Network disabled during native preflight")):
        manifest, prompts = preflight(plan, frame)
    print(json.dumps({"protocol_sha256": manifest["protocol_sha256"],
                      "run_manifest_sha256": control.sha256(control.canonical(manifest)),
                      "native_prompts": sum(len(rows) for rows in prompts.values()),
                      "estimate": plan["estimate"], "model_availability": plan["model_availability"]}, indent=2))
    if args.execute:
        execute(folder, plan, frame, manifest, prompts)
        control.write_json(folder / "summary.json", analyse(folder, plan, manifest, prompts))
    else:
        print("Offline preflight complete: no provider calls or experiment files written.")


if __name__ == "__main__":
    main()
