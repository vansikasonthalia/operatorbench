"""Compare reporting views of saved outputs; no new pipeline executions."""
import argparse
import json
from pathlib import Path

from harness.admission import build_ledger
from scripts import audit_frozen_evidence as audit
from scripts import run_matched_control as control

DEFAULT_RUN = control.ROOT / "results/matched_control/n500_r4_s20261007"
DEFAULT_OUTPUT = control.ROOT / "audits/reporting_comparison"


def reporting_views(ledger, disagreement):
    counts = {side: {key: ledger[side][key] for key in
                     ("admitted", "disagreements", "disagreement_rate")}
              for side in ("before", "after")}
    for side in counts:
        counts[side]["coverage"] = counts[side]["admitted"] / ledger["n_input"] if ledger["n_input"] else None
    transitions = ledger["label_transitions"]["saved"]
    common = len(ledger["sets"]["common"])
    paired = {
        "n_common": common,
        "before_disagreements": sum(t["count"] for t in transitions if disagreement(t["before"])),
        "after_disagreements": sum(t["count"] for t in transitions if disagreement(t["after"])),
        "new_disagreements": sum(t["count"] for t in transitions
                                 if not disagreement(t["before"]) and disagreement(t["after"])),
        "resolved_disagreements": sum(t["count"] for t in transitions
                                      if disagreement(t["before"]) and not disagreement(t["after"])),
        "saved_label_changes": sum(t["count"] for t in transitions if t["before"] != t["after"]),
    }
    reconstructed = {
        "introduced_by_new_admissions": counts["after"]["disagreements"] - paired["after_disagreements"],
        "removed_by_exclusions": counts["before"]["disagreements"] - paired["before_disagreements"],
        "common_rows_new_disagreements": paired["new_disagreements"],
        "common_rows_resolved_disagreements": paired["resolved_disagreements"],
        "common_rows_net": paired["after_disagreements"] - paired["before_disagreements"],
    }
    sizes = {"common": common, "excluded": counts["before"]["admitted"] - common,
             "newly_admitted": counts["after"]["admitted"] - common,
             "never_admitted": ledger["n_input"] - counts["before"]["admitted"] - counts["after"]["admitted"] + common}
    agrees = reconstructed == ledger["terms"] and sizes == {name: len(rows) for name, rows in ledger["sets"].items()}
    if not agrees:
        raise ValueError("Paired counts do not reconstruct the ledger")
    return {
        "rate_only": {side: counts[side]["disagreement_rate"] for side in counts},
        "counts_and_coverage": {"n_input": ledger["n_input"], **counts},
        "paired_common_and_totals": {"totals": counts, "n_input": ledger["n_input"], "common": paired},
        "reconstructed_from_paired_counts": {"terms": reconstructed, "partition_sizes": sizes,
                                              "matches_ledger": agrees},
        "full_ledger": ledger,
    }


def matched_cases(folder):
    plan = audit.load_json(folder / "plan.json")
    if plan["source_sha256"] != audit.digest(Path(control.__file__)):
        raise ValueError("Matched-control execution source does not match the plan")
    cells = {(rep, cell): {} for rep in range(1, plan["replicates"] + 1) for cell in control.CELLS}
    for job in plan["jobs"]:
        saved = control.read_completed(folder, plan, job)
        if saved is None:
            raise ValueError(f"Incomplete matched-control job: {job['job']}")
        cells[job["replicate"], job["cell"]].update((r["row_id"], r) for r in saved["records"])
    cases = []
    exclusions = []
    for rep in range(1, plan["replicates"] + 1):
        rows = {cell: cells[rep, cell] for cell in control.CELLS}
        if any(set(records) != set(plan["row_ids"]) for records in rows.values()):
            raise ValueError("Matched-control cells do not cover the planned rows")
        invalid = {cell: sorted(row for row, record in records.items() if record["answer"] is None)
                   for cell, records in rows.items()}
        eligible = sorted(set(plan["row_ids"]) - set().union(*map(set, invalid.values())))
        exclusions.append({"replicate": rep, "n_input": len(plan["row_ids"]), "n_eligible": len(eligible),
                           "invalid_by_cell": invalid})

        def admitted(filter_cell, map_cell):
            return {row: {"label": str(rows[map_cell][row]["answer"]),
                          "raw": rows[map_cell][row]["raw_output"]}
                    for row in eligible if rows[filter_cell][row]["answer"]}

        for name, left, right in (
            ("filter_change", ("about_filter", "about_map"), ("primary_filter", "about_map")),
            ("map_change", ("about_filter", "about_map"), ("about_filter", "primary_map")),
            ("both_change", ("about_filter", "about_map"), ("primary_filter", "primary_map")),
        ):
            before, after = admitted(*left), admitted(*right)
            ledger = build_ledger(before, after, eligible,
                                  label_column="label", raw_column="raw", valid_labels={"True", "False"},
                                  disagreement=lambda label: label == "False")
            for side, records in (("before", before), ("after", after)):
                invalid_rows = sorted(row for row, record in records.items()
                                      if control.parse_answer(record["raw"]) is None)
                ledger[side]["invalid_outputs"]["raw"] = {"count": len(invalid_rows), "row_ids": invalid_rows}
            ledger["measurement"]["validity_rule"] = "Saved labels are True/False; raw responses use the original strict Answer: True/False parser. All four cells must be valid before admission masks are applied."
            cases.append({"name": f"matched_r{rep}_{name}", "evidence": "derived_from_recorded_answers",
                          "replicate": rep, "change": name, "before_cells": list(left), "after_cells": list(right),
                          "views": reporting_views(ledger, lambda label: label == "False")})
    return cases, exclusions, control.sha256(control.canonical(plan))


def constructed_witnesses():
    def ledger(before, after):
        def records(labels):
            return {row: {"label": label, "raw": label} for row, label in labels.items()}
        return build_ledger(records(before), records(after), range(3), label_column="label", raw_column="raw",
                            valid_labels={"ML", "theory"}, disagreement=lambda label: label != "ML")
    pairs = {
        "exclusion_only": ledger({0: "theory", 1: "ML"}, {1: "ML"}),
        "common_change_only": ledger({0: "theory", 1: "ML"}, {0: "ML"}),
    }
    views = {name: reporting_views(value, lambda label: label != "ML") for name, value in pairs.items()}
    return {"scope": "Constructed non-identifiability witnesses, not model observations or detection benchmarks.",
            "cases": views,
            "same_counts_and_coverage": views["exclusion_only"]["counts_and_coverage"] == views["common_change_only"]["counts_and_coverage"],
            "different_accounting_terms": pairs["exclusion_only"]["terms"] != pairs["common_change_only"]["terms"]}


def analyse(historical_ledgers, folder=DEFAULT_RUN):
    cases = [{"name": f"historical_r{ledger['replicate']}", "evidence": "executed_historical_pipeline",
              "replicate": ledger["replicate"],
              "views": reporting_views(ledger, lambda label: audit.disagreement("abstracts", label))}
             for ledger in historical_ledgers]
    matched, exclusions, plan_hash = matched_cases(folder)
    cases.extend(matched)
    witness = constructed_witnesses()
    return {
        "scope": "Post-hoc descriptive reporting comparison. Binary pipelines are masks applied offline to saved all-row answers, not new executions of composed pipelines. Cases share documents and responses; they are not independent trials.",
        "rule": "Historical saved non-ML labels count as disagreement. Derived binary cases count False map answers after True filter admission; this measures criterion agreement, not correctness. Changing criteria may change the intended task.",
        "matched_cohort": "Per replicate, retain only rows valid in all four cells, shared by all three derived comparisons. Invalid exclusions are separate from semantic filter exclusions; no invalid answer is recoded.",
        "matched_plan_sha256": plan_hash,
        "analysis_source_sha256": audit.digest(Path(__file__)),
        "cases": cases, "format_exclusions": exclusions, "constructed_witnesses": witness,
        "checks": {"paired_counts_reconstruct_all_recorded_ledgers": all(c["views"]["reconstructed_from_paired_counts"]["matches_ledger"] for c in cases),
                   "constructed_counts_collision_verified": witness["same_counts_and_coverage"] and witness["different_accounting_terms"]},
        "interpretation": "Rates alone omit coverage. Totals plus coverage expose contraction but do not identify the accounting mechanism in general. Common-row paired counts plus totals reconstruct every aggregate ledger term and partition size. The full ledger makes those calculations explicit and additionally retains row IDs, raw transitions and separate validity reporting. This is not a demonstrated improvement in error detection, a new theorem, or evidence of generality.",
    }


def markdown(report):
    lines = ["# Offline comparison of admission reporting", "", report["scope"], "", report["rule"], "", report["matched_cohort"], "",
             "Historical cases are executed pipelines. The remaining cases reuse binary answers under three offline masks per replicate; do not pool them.", "",
             "| Case | Eligible | Before D/n | After D/n | New D | Excluded D | Common added / resolved | Saved changes | Paired counts reconstruct ledger |",
             "|---|---:|---|---|---:|---:|---|---:|---|"]
    for case in report["cases"]:
        views = case["views"]
        ledger = views["full_ledger"]
        terms = ledger["terms"]
        paired = views["paired_common_and_totals"]["common"]
        lines.append(f"| {case['name']} | {ledger['n_input']} | {ledger['before']['disagreements']}/{ledger['before']['admitted']} | {ledger['after']['disagreements']}/{ledger['after']['admitted']} | {terms['introduced_by_new_admissions']} | {terms['removed_by_exclusions']} | {terms['common_rows_new_disagreements']} / {terms['common_rows_resolved_disagreements']} | {paired['saved_label_changes']} | Yes |")
    lines += ["", "## Format exclusions before binary masking", "",
              "The shared eligible cohorts have " + ", ".join(str(e["n_eligible"]) for e in report["format_exclusions"]) + " rows. JSON retains excluded row IDs per cell.", "",
              "## Constructed ambiguity", "",
              "Both witnesses start with 1 disagreement among 2 admissions and end with 0 among 1 (input universe 3). One excludes the disagreement; the other resolves it on a retained row and excludes a previously agreeing row. Their rates, counts, and coverage are identical. Paired counts distinguish them. This is a mathematical example, not an empirical failure rate.", "",
              "## Interpretation", "", report["interpretation"], "",
              "No blinded diagnosis experiment, detection accuracy, scientific correctness assessment, or model rerun was performed.", ""]
    return "\n".join(lines)


def main():
    from scripts.reproduce_artifact import admission_ledger, matched_summary
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    output = parser.parse_args().output.resolve()
    if output == control.ROOT / "audits" or not output.is_relative_to(control.ROOT / "audits") or any(
            output.is_relative_to(control.ROOT / name) for name in ("audits/2026-10-07", "audits/2026-10-08")):
        raise ValueError("Use a separate output directory under audits/, outside the dated audits")
    matched_summary(DEFAULT_RUN)
    report = analyse(admission_ledger())
    output.mkdir(parents=True, exist_ok=True)
    (output / "comparison.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    (output / "COMPARISON.md").write_text(markdown(report), encoding="utf-8")
    print(json.dumps({"output": str(output), "recorded_comparisons": len(report["cases"]), "checks": report["checks"]}, indent=2))


if __name__ == "__main__":
    main()
