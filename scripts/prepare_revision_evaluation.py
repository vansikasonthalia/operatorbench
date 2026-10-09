"""Freeze a local evaluation protocol without obtaining new model outputs."""
import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import random

from scripts import audit_frozen_evidence as audit
from scripts import run_matched_control as control

ROOT = control.ROOT
DEFAULT_OUTPUT = ROOT / "artifacts/revision-evaluation-20261008"
QUESTIONS = {
    "reference_filter": "Is this paper about machine learning?",
    "reference_map": "Is this paper about machine learning?",
    "repeat_filter": "Is this paper about machine learning?",
    "repeat_map": "Is this paper about machine learning?",
    "paraphrase_filter": "Does this paper concern machine learning?",
    "narrow_filter": "Is machine learning a substantive research topic of this paper, rather than only background or a passing mention?",
    "paraphrase_map": "Does this paper concern machine learning?",
}
REVISIONS = {
    "identical_prompt_repeat": {"after": ["repeat_filter", "repeat_map"],
                                "admission_relation": "equal", "preserve_common_labels": True},
    "filter_paraphrase": {"after": ["paraphrase_filter", "reference_map"],
                          "admission_relation": "equal", "preserve_common_labels": True},
    "filter_narrowing": {"after": ["narrow_filter", "reference_map"],
                         "admission_relation": "subset", "preserve_common_labels": True},
    "map_paraphrase": {"after": ["reference_filter", "paraphrase_map"],
                       "admission_relation": "equal", "preserve_common_labels": True},
}
SOURCES = ("scripts/prepare_revision_evaluation.py", "harness/admission.py",
           "harness/revision.py", "harness/revision_assertions.py",
           "scripts/run_matched_control.py", "docs/REVISION-EVALUATION.md")


def make_plan():
    rows = audit.indexed(audit.read_csv(control.DATA, source=True))
    ids = sorted(random.Random(20261008).sample(sorted(rows), 100))
    windows = [(row, rows[row]["abstract_text"][:4000]) for row in ids]
    rng = random.Random(20261009)
    cells = list(QUESTIONS)
    rng.shuffle(cells)
    jobs = []
    for rep in range(1, 5):
        order = ids.copy()
        rng.shuffle(order)
        for block, start in enumerate(range(0, len(order), 10)):
            offset = (block + rep - 1) % len(cells)
            for cell in cells[offset:] + cells[:offset]:
                jobs.append({"job": len(jobs), "replicate": rep, "block": block,
                             "cell": cell, "row_ids": order[start:start + 10]})
    n_calls = len(ids) * len(cells) * 4
    return {
        "protocol": "revision_evaluation_v1", "status": "prepared_not_executed",
        "prepared_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "Locally fixed before new outputs; previously analysed documents and informed prompt choices. Not an external preregistration, document holdout, correctness study, or proof of novelty.",
        "model": "gpt-4o-mini-2024-07-18", "model_availability": "Not checked; failure must be reported, not silently substituted.",
        "temperature_requested": 0.0, "max_output_tokens": 16, "provider_seed": None,
        "cache_enabled": False, "replicates": 4, "block_size": 10,
        "batch_delay_seconds": 12, "max_text_chars": 4000,
        "row_ids": ids, "selection_seed": 20261008, "schedule_seed": 20261009,
        "dataset_sha256": audit.digest(control.DATA),
        "input_reader": "audit_frozen_evidence.read_csv(source=True); preserve embedded NULs and original newlines, then take the first 4000 characters.",
        "input_sha256": control.sha256(control.canonical(windows)),
        "source_sha256": {name: audit.digest(ROOT / name) for name in SOURCES},
        "questions": QUESTIONS,
        "instructions": {cell: f"{question} {control.FORMAT} {{abstract_text}}" for cell, question in QUESTIONS.items()},
        "revisions": {name: {"before": ["reference_filter", "reference_map"],
                              **revision, "require_valid_outputs": True,
                              "require_complete_input": True}
                      for name, revision in REVISIONS.items()},
        "jobs": jobs,
        "analysis": {
            "baselines": ["conditional_rate", "counts_and_coverage", "common_row_paired_counts", "direct_record_assertions"],
            "primary_outcomes": ["per-requirement status and exception IDs", "agreement with direct assertions",
                                 "admission and common-label changes per replicate", "invalid and unassessed rows"],
            "validity": "Use the original strict Answer: True/False parser. Determine assessability separately for each revision, using its required cells; unrelated cells cannot exclude rows. Report format-invalid responses, unassessed rows and observed-cohort versus full-input verdicts separately. Never recode an invalid answer as False.",
            "inference": "Descriptive per-replicate results for all four revisions. No pooled independence assumption, significance tests, equivalence claim, or detection-accuracy score. Identical-prompt repeats contextualize variation; they do not establish a causal noise correction.",
            "interpretation": "Contracts state intended relationships, not proven semantic equivalence. Direct assertions have the same information and requirements, so better detection is not assumed. Filter-only revisions share reference map answers; map-only revisions share reference filter answers. Preservation of those shared components is by construction. All-row masks are derived views, not composed-pipeline executions.",
            "change_policy": "No tuning after new outputs are inspected. Changed prompts, requirements, rows, parser, settings or analysis create a separately identified amended protocol. Any pilot must use a separate plan and remain excluded from this evaluation.",
        },
        "execution": {"implemented": False, "note": "This tool prepares and verifies the protocol only. A runner must bind native prompts, installed framework sources, checkpoints, request logs and this plan before any execution. No paid execution command is supplied."},
        "estimate": {"row_calls": n_calls, "operator_blocks": len(jobs),
                     "approx_input_tokens": (sum(math.ceil(len(text) / 4) for _, text in windows) + 128 * len(ids)) * len(cells) * 4,
                     "output_token_allowance": n_calls * 16,
                     "minimum_delay_minutes": (len(jobs) - 1) * 12 / 60,
                     "note": "Characters/4 plus 128 prompt tokens per row; heuristic, not a spending cap or runtime prediction. Retries and latency are additional. No dollar prices assumed."},
    }


def freeze(output):
    plan = make_plan()
    output.mkdir(parents=True, exist_ok=False)
    payload = (json.dumps(plan, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    (output / "plan.json").write_bytes(payload)
    (output / "plan.sha256").write_text(control.sha256(payload) + "\n", encoding="utf-8")
    return plan


def verify(output):
    payload = (output / "plan.json").read_bytes()
    if control.sha256(payload) != (output / "plan.sha256").read_text().strip():
        raise ValueError("Frozen plan bytes changed")
    plan = json.loads(payload)
    if audit.digest(control.DATA) != plan["dataset_sha256"]:
        raise ValueError("Frozen dataset changed")
    for name, expected in plan["source_sha256"].items():
        if audit.digest(ROOT / name) != expected:
            raise ValueError(f"Frozen protocol source changed: {name}")
    rows = audit.indexed(audit.read_csv(control.DATA, source=True))
    windows = [(row, rows[row]["abstract_text"][:plan["max_text_chars"]]) for row in plan["row_ids"]]
    if control.sha256(control.canonical(windows)) != plan["input_sha256"]:
        raise ValueError("Frozen input windows changed")
    return plan


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    output = args.output.resolve()
    if output.parent != ROOT / "artifacts":
        raise ValueError("Use a new directory directly under artifacts/")
    plan = verify(output) if args.verify else freeze(output)
    print(json.dumps({"output": str(output), "status": plan["status"],
                      "verified": args.verify, "estimate": plan["estimate"]}, indent=2))


if __name__ == "__main__":
    main()
