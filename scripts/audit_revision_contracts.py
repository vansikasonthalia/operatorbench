"""Post-hoc illustrations of declared requirements on saved revision views."""
from harness.revision import check_revision
from scripts import audit_frozen_evidence as audit
from scripts import run_matched_control as control


def analyse(comparison):
    omitted = {record["replicate"]: sorted(set().union(*map(set, record["invalid_by_cell"].values())))
               for record in comparison["format_exclusions"]}
    cases = []
    for case in comparison["cases"]:
        ledger = case["views"]["full_ledger"]
        change = case.get("change", "filter_change")
        declared = {"admission_relation": "equal" if change == "map_change" else "subset",
                    "preserve_common_labels": change == "filter_change"}
        unassessed = omitted[case["replicate"]] if case["evidence"] == "derived_from_recorded_answers" else []
        cases.append({"name": case["name"], "evidence": case["evidence"],
                      "preserve_results": check_revision(ledger, admission_relation="equal", preserve_common_labels=True,
                                                          unassessed_rows=unassessed),
                      "declared_change_full_input": check_revision(ledger, **declared, unassessed_rows=unassessed),
                      "declared_change_observed_cohort": check_revision(ledger, **declared, unassessed_rows=unassessed,
                                                                         require_complete_input=False)})
    return {"scope": "Post-hoc prototype demonstration. Requirements were chosen after seeing the saved changes; these verdicts are not detection accuracy, prospective validation, or evidence of novelty.",
            "policy": "Preserve-results requires equal admission sets and unchanged common saved labels. A declared filter change permits narrowing and requires unchanged common labels. A declared map change preserves admissions but permits label changes. A declared joint change permits narrowing and label changes. All require valid observed raw and saved outputs. Full-input assessments require complete evidence; observed-cohort assessments explicitly waive that requirement.",
            "checks_source_sha256": audit.digest(control.ROOT / "harness/revision.py"),
            "analysis_source_sha256": audit.digest(control.ROOT / "scripts/audit_revision_contracts.py"),
            "reporting_source_sha256": comparison["analysis_source_sha256"],
            "cases": cases,
            "interpretation": "Requirements make intent explicit but do not discover intent or judge whether a changed question is appropriate. These checks use information already available in a complete paired report. Observed-cohort satisfaction does not imply a full-input verdict; input omissions remain visible. For filter-only cases, the common four-cell validity policy omits rows even though their required filter/map answers are valid. Not evaluable concerns this supplied view, not the absence of all original responses; a separately derived full-input filter-only view could be evaluated."}


def markdown(report):
    lines = ["# Revision requirements: offline prototype", "", report["scope"], "", report["policy"], "",
             "| Case | Preserve results | Declared change, full input | Declared change, observed cohort | Unassessed rows |",
             "|---|---|---|---|---:|"]
    for case in report["cases"]:
        full = case["declared_change_full_input"]
        lines.append(f"| {case['name']} | {case['preserve_results']['status']} | {full['status']} | {case['declared_change_observed_cohort']['status']} | {len(full['scope']['unassessed_rows'])} |")
    lines += ["", report["interpretation"], "", "JSON retains the exact contract, each requirement's status, and exception row IDs. No performance score is calculated from these post-hoc examples.", ""]
    return "\n".join(lines)
