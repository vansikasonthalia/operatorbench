"""Interleaved criterion-by-operator control. Defaults to an offline dry run."""
import argparse
from datetime import datetime, timezone
import hashlib
from importlib.metadata import distribution, version
import json
import math
import os
from pathlib import Path
import random
import re
import sys
import time

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

QUESTIONS = {
    "about": "Is this paper about machine learning?",
    "primary": "Is machine learning the primary topic of this paper?",
}
CELLS = tuple(f"{criterion}_{operator}" for criterion in QUESTIONS
              for operator in ("filter", "map"))
CONTRASTS = {
    "operator_about": ("about_filter", "about_map"),
    "operator_primary": ("primary_filter", "primary_map"),
    "criterion_filter": ("about_filter", "primary_filter"),
    "criterion_map": ("about_map", "primary_map"),
}
FORMAT = "Respond exactly as Answer: True or Answer: False."
DATA = ROOT / "data/abstracts/abstracts_v1.csv"


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def canonical(data):
    return json.dumps(data, sort_keys=True, ensure_ascii=False).encode("utf-8")


def instruction(cell):
    criterion, _ = cell.rsplit("_", 1)
    return f"{QUESTIONS[criterion]} {FORMAT} {{abstract_text}}"


def parse_answer(raw):
    match = re.fullmatch(r"Answer:\s*(True|False)", str(raw).strip(), re.IGNORECASE)
    return match.group(1).lower() == "true" if match else None


def schedule(row_ids, replicates, block_size, seed):
    rng = random.Random(seed)
    cells = list(CELLS)
    rng.shuffle(cells)
    jobs = []
    for rep in range(1, replicates + 1):
        rows = list(row_ids)
        rng.shuffle(rows)
        for block, start in enumerate(range(0, len(rows), block_size)):
            offset = (block + rep - 1) % len(cells)
            order = cells[offset:] + cells[:offset]
            for cell in order:
                jobs.append({"job": len(jobs), "replicate": rep,
                             "block": block, "cell": cell,
                             "row_ids": rows[start:start + block_size]})
    return jobs


def load_rows(limit):
    frame = pd.read_csv(DATA, escapechar="\\", index_col="row_id")
    if list(frame.index) != list(range(500)) or frame["abstract_text"].isna().any():
        raise ValueError("Frozen abstracts must contain rows 0..499 with text")
    return frame[["abstract_text"]].iloc[:limit].copy()


def make_plan(args, frame):
    window = frame["abstract_text"].str[:args.max_text_chars]
    return {
        "protocol": "matched_control_v1", "model": args.model,
        "replicates": args.replicates, "block_size": args.block_size,
        "schedule_seed": args.schedule_seed, "provider_seed": None,
        "temperature_requested": 0.0, "max_output_tokens": args.max_output_tokens,
        "batch_delay_seconds": args.batch_delay, "cache_enabled": False,
        "max_text_chars": args.max_text_chars,
        "dataset_sha256": sha256(DATA.read_bytes()),
        "input_sha256": sha256(canonical(list(zip(frame.index.tolist(), window)))),
        "source_sha256": sha256(Path(__file__).read_bytes()),
        "framework_source_sha256": {
            name: sha256(distribution("lotus-ai").locate_file(name).read_bytes())
            for name in ("lotus/templates/task_instructions.py", "lotus/sem_ops/sem_filter.py",
                         "lotus/sem_ops/sem_map.py", "lotus/sem_ops/postprocessors.py",
                         "lotus/models/lm.py")
        },
        "packages": {name: version(name) for name in ("lotus-ai", "litellm", "pandas")},
        "questions": QUESTIONS, "instructions": {cell: instruction(cell) for cell in CELLS},
        "row_ids": frame.index.tolist(),
        "jobs": schedule(frame.index.tolist(), args.replicates,
                         args.block_size, args.schedule_seed),
        "analysis": {"contrasts": CONTRASTS,
                     "invalid_answers": "excluded pairwise, reported separately",
                     "multiple_testing": "Holm across all four contrasts and all planned replicates; no pooled p-value",
                     "scope": "operator access and native prompt construction; not a system-message-only effect"},
    }


def estimate(plan, frame, input_price, output_price):
    calls = len(frame) * len(CELLS) * plan["replicates"]
    text_tokens = sum(math.ceil(len(text[:plan["max_text_chars"]]) / 4)
                      for text in frame["abstract_text"])
    input_tokens = (text_tokens + 128 * len(frame)) * len(CELLS) * plan["replicates"]
    output_tokens = calls * plan["max_output_tokens"]
    return {
        "row_calls": calls, "operator_blocks": len(plan["jobs"]),
        "approx_input_tokens": input_tokens, "output_token_allowance": output_tokens,
        "approx_usd": round((input_tokens * input_price + output_tokens * output_price) / 1e6, 2),
        "input_usd_per_million": input_price, "output_usd_per_million": output_price,
        "minimum_delay_minutes": round(max(0, len(plan["jobs"]) - 1)
                                       * plan["batch_delay_seconds"] / 60, 1),
        "note": "Input estimate uses characters/4 plus 128 prompt tokens per call; not a spending cap. Latency and provider retries are extra.",
    }


def write_json(path, data):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    temporary.replace(path)


def evaluate_cell(frame, cell):
    _, operator = cell.rsplit("_", 1)
    if operator == "filter":
        result = frame.sem_filter(instruction(cell), return_all=True,
                                  return_raw_outputs=True, suffix="decision",
                                  strategy=None, cascade_args=None)
    else:
        result = frame.sem_map(instruction(cell), return_raw_outputs=True,
                               suffix="decision", strategy=None)
    if not result.index.is_unique or set(result.index) != set(frame.index):
        raise ValueError("Operator did not return every input row exactly once")
    return [{"row_id": int(row_id), "raw_output": str(result.loc[row_id, "raw_outputdecision"]),
             "answer": parse_answer(result.loc[row_id, "raw_outputdecision"]),
             "operator_output": str(result.loc[row_id, "decision"])}
            for row_id in frame.index]


def read_completed(folder, plan, job):
    path = folder / f"job_{job['job']:04d}.json"
    if not path.exists():
        return None
    saved = json.loads(path.read_text(encoding="utf-8"))
    if saved["plan_sha256"] != sha256(canonical(plan)) or saved["job"] != job:
        raise ValueError(f"Incompatible checkpoint: {path}")
    requests = folder / saved["request_file"]
    if requests.parent != folder or sha256(requests.read_bytes()) != saved["request_sha256"]:
        raise ValueError(f"Request archive changed: {path}")
    if saved["records_sha256"] != sha256(canonical(saved["records"])):
        raise ValueError(f"Result checkpoint changed: {path}")
    if [record["row_id"] for record in saved["records"]] != job["row_ids"]:
        raise ValueError(f"Wrong checkpoint rows: {path}")
    for record in saved["records"]:
        if record["answer"] is not parse_answer(record["raw_output"]):
            raise ValueError(f"Parsed answer differs from raw response: {path}")
    return saved


def execute(folder, plan, frame):
    from harness.intercept import LLMInterceptor
    import lotus
    from lotus.models import LM

    if not os.environ.get("OPENAI_API_KEY"):
        raise ValueError("OPENAI_API_KEY is required for execution")
    folder.mkdir(parents=True, exist_ok=True)
    manifest = folder / "plan.json"
    if manifest.exists():
        if json.loads(manifest.read_text(encoding="utf-8")) != json.loads(canonical(plan)):
            raise ValueError("Existing plan differs; use a new output directory")
    else:
        if any(folder.iterdir()):
            raise ValueError("Output directory has files but no plan; use a new directory")
        write_json(manifest, plan)
    lm = LM(model=plan["model"], temperature=plan["temperature_requested"],
            max_tokens=plan["max_output_tokens"], max_batch_size=plan["block_size"])
    frame = frame.copy()
    frame["abstract_text"] = frame["abstract_text"].str[:plan["max_text_chars"]]
    with lotus.settings.context(lm=lm, enable_cache=False):
        for job in plan["jobs"]:
            if read_completed(folder, plan, job) is not None:
                continue
            # Keep failed-attempt logs when resuming an interrupted block.
            attempt = len(list(folder.glob(f"llm_calls_job_{job['job']:04d}_*.jsonl"))) + 1
            tag = f"job_{job['job']:04d}_{attempt:03d}"
            interceptor = LLMInterceptor(log_dir=folder, tag=tag)
            started = datetime.now(timezone.utc).isoformat()
            interceptor.install()
            try:
                records = evaluate_cell(frame.loc[job["row_ids"]], job["cell"])
            finally:
                interceptor.uninstall()
            request_file = folder / f"llm_calls_{tag}.jsonl"
            if len(interceptor.calls) != len(job["row_ids"]):
                raise ValueError("Request coverage mismatch; block not marked complete")
            write_json(folder / f"job_{job['job']:04d}.json", {
                "plan_sha256": sha256(canonical(plan)), "job": job,
                "started_utc": started, "finished_utc": datetime.now(timezone.utc).isoformat(),
                "attempt": attempt, "records": records,
                "records_sha256": sha256(canonical(records)),
                "request_file": request_file.name,
                "request_sha256": sha256(request_file.read_bytes()),
                "lm_kwargs": lm.kwargs,
                "usage": interceptor.summary(),
            })
            print(f"Completed {job['job'] + 1}/{len(plan['jobs'])}: {job['cell']}")
            if job != plan["jobs"][-1]:
                time.sleep(plan["batch_delay_seconds"])


def holm(p_values):
    adjusted = {}
    previous = 0.0
    for position, (key, p_value) in enumerate(sorted(p_values.items(), key=lambda pair: pair[1])):
        previous = max(previous, min(1.0, p_value * (len(p_values) - position)))
        adjusted[key] = previous
    return adjusted


def compare(left, right):
    from harness.metrics import wilson_ci
    from scripts.f7_precision_paired import mcnemar_exact
    common = sorted(left.keys() & right.keys())
    valid = [row for row in common if left[row] is not None and right[row] is not None]
    left_only = [row for row in valid if left[row] and not right[row]]
    right_only = [row for row in valid if right[row] and not left[row]]
    n = len(valid)
    return {"n_total": len(common), "n_valid_pairs": n,
            "n_excluded_invalid": len(common) - n,
            "left_true_only": len(left_only), "right_true_only": len(right_only),
            "differing_rows": sorted(left_only + right_only),
            "difference_rate": (len(left_only) + len(right_only)) / n if n else None,
            "difference_rate_wilson_ci": list(wilson_ci(len(left_only) + len(right_only), n)) if n else None,
            "right_minus_left_pp": 100 * (len(right_only) - len(left_only)) / n if n else None,
            "mcnemar_exact_p": mcnemar_exact(len(left_only), len(right_only)) if n else None}


def analyse(folder, plan):
    cells = {(rep, cell): {} for rep in range(1, plan["replicates"] + 1) for cell in CELLS}
    for job in plan["jobs"]:
        saved = read_completed(folder, plan, job)
        if saved is None:
            raise ValueError(f"Missing job {job['job']}; incomplete experiment cannot be aggregated")
        cells[job["replicate"], job["cell"]].update(
            (record["row_id"], record["answer"]) for record in saved["records"])
    reports = []
    all_p = {}
    for rep in range(1, plan["replicates"] + 1):
        results = {name: compare(cells[rep, left], cells[rep, right])
                   for name, (left, right) in CONTRASTS.items()}
        for name, result in results.items():
            # Keep the full planned family, including untestable contrasts.
            p_value = result["mcnemar_exact_p"]
            all_p[f"{rep}:{name}"] = 1.0 if p_value is None else p_value
        reports.append({"replicate": rep,
                        "cells": {cell: {"n_true": sum(v is True for v in cells[rep, cell].values()),
                                         "invalid_rows": sorted(row for row, v in cells[rep, cell].items() if v is None)}
                                  for cell in CELLS}, "contrasts": results})
    adjusted = holm(all_p)
    for report in reports:
        for name, result in report["contrasts"].items():
            result["holm_p"] = adjusted[f"{report['replicate']}:{name}"] if result["n_valid_pairs"] else None
    repeatability = {}
    for cell in CELLS:
        valid = [row for row in plan["row_ids"]
                 if all(cells[rep, cell][row] is not None for rep in range(1, plan["replicates"] + 1))]
        flips = [row for row in valid if len({cells[rep, cell][row]
                 for rep in range(1, plan["replicates"] + 1)}) > 1]
        repeatability[cell] = {"n_valid_all_replicates": len(valid), "flipping_rows": flips,
                               "flip_rate": len(flips) / len(valid) if valid else None}
    return {"plan_sha256": sha256(canonical(plan)),
            "analysis_source_sha256": sha256(Path(__file__).read_bytes()),
            "replicates": reports,
            "repeatability": repeatability, "analysis": plan["analysis"]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--aggregate-only", action="store_true")
    parser.add_argument("--yes", action="store_true", help="Required together with --execute")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--model", default="gpt-4o-mini-2024-07-18")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--replicates", type=int, default=4)
    parser.add_argument("--block-size", type=int, default=10)
    parser.add_argument("--schedule-seed", type=int, default=20261007)
    parser.add_argument("--max-text-chars", type=int, default=4000)
    parser.add_argument("--max-output-tokens", type=int, default=16)
    parser.add_argument("--batch-delay", type=float, default=12)
    parser.add_argument("--input-usd-per-million", type=float)
    parser.add_argument("--output-usd-per-million", type=float)
    args = parser.parse_args(argv)
    if args.aggregate_only:
        if args.output_dir is None:
            parser.error("--aggregate-only requires --output-dir")
        plan = json.loads((args.output_dir / "plan.json").read_text(encoding="utf-8"))
        write_json(args.output_dir / "summary.json", analyse(args.output_dir, plan))
        return
    if args.model in ("gpt-4o-mini", "gpt-4o-mini-2024-07-18"):
        args.input_usd_per_million = 0.15 if args.input_usd_per_million is None else args.input_usd_per_million
        args.output_usd_per_million = 0.60 if args.output_usd_per_million is None else args.output_usd_per_million
    elif args.input_usd_per_million is None or args.output_usd_per_million is None:
        parser.error("Supply both token prices when using another model")
    if not 1 <= args.limit <= 500 or args.replicates < 2 or args.block_size < 1:
        parser.error("limit must be 1..500, replicates >=2, block-size >=1")
    if min(args.max_text_chars, args.max_output_tokens) < 1 or args.batch_delay < 0:
        parser.error("text/output limits must be positive and batch delay nonnegative")
    if min(args.input_usd_per_million, args.output_usd_per_million) < 0:
        parser.error("Token prices must be nonnegative")
    if args.execute and not args.yes:
        parser.error("Paid execution requires both --execute and --yes")
    frame = load_rows(args.limit)
    plan = make_plan(args, frame)
    estimate_result = estimate(plan, frame, args.input_usd_per_million, args.output_usd_per_million)
    print(json.dumps({"plan_sha256": sha256(canonical(plan)), "estimate": estimate_result,
                      "instructions": plan["instructions"], "first_jobs": plan["jobs"][:4]}, indent=2))
    folder = args.output_dir or ROOT / "results/matched_control" / f"n{args.limit}_r{args.replicates}_s{args.schedule_seed}"
    if args.execute:
        execute(folder, plan, frame)
        write_json(folder / "summary.json", analyse(folder, plan))
    else:
        print("Offline dry run: no model calls or experiment files written.")


if __name__ == "__main__":
    main()
