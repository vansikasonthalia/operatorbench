"""Check declared revision requirements against an admission ledger."""


def check_revision(ledger, *, admission_relation, preserve_common_labels=False,
                   forbid_new_common_disagreements=False, require_valid_outputs=True,
                   unassessed_rows=(), require_complete_input=True):
    if admission_relation not in {"equal", "subset", "superset", "unrestricted"}:
        raise ValueError("Unknown admission relation")
    unassessed = sorted(set(unassessed_rows))
    if set(unassessed) & set().union(*map(set, ledger["sets"].values())):
        raise ValueError("Unassessed rows overlap the ledger's evaluated input")
    contract = {"admission_relation": admission_relation,
                "preserve_common_labels": preserve_common_labels,
                "forbid_new_common_disagreements": forbid_new_common_disagreements,
                "require_valid_outputs": require_valid_outputs,
                "require_complete_input": require_complete_input}
    checks = {}
    if require_complete_input:
        checks["complete_input"] = {"status": "not_evaluable" if unassessed else "satisfied",
                                    "unassessed_rows": unassessed}
    rows = ledger["sets"]
    forbidden = {"equal": rows["excluded"] + rows["newly_admitted"],
                 "subset": rows["newly_admitted"], "superset": rows["excluded"],
                 "unrestricted": []}[admission_relation]
    checks["admissions"] = {"status": "violated" if forbidden else "satisfied",
                            "row_ids": sorted(forbidden), "relation": admission_relation}
    if preserve_common_labels:
        changed = sorted(row for transition in ledger["label_transitions"]["saved"]
                         if transition["before"] != transition["after"] for row in transition["row_ids"])
        checks["common_labels"] = {"status": "violated" if changed else
                                   "satisfied" if rows["common"] else "not_evaluable",
                                   "n_checked": len(rows["common"]), "row_ids": changed}
    if forbid_new_common_disagreements:
        added = ledger["common_indicator_changes"]["new_disagreements"]
        checks["new_common_disagreements"] = {"status": "violated" if added else
                                              "satisfied" if rows["common"] else "not_evaluable",
                                              "n_checked": len(rows["common"]), "row_ids": added}
    if require_valid_outputs:
        invalid = {side: ledger[side]["invalid_outputs"] for side in ("before", "after")}
        n_records = sum(ledger[side]["admitted"] for side in invalid)
        has_invalid = any(value["count"] for side in invalid.values() for value in side.values())
        checks["output_validity"] = {"status": "violated" if has_invalid else
                                     "satisfied" if n_records else "not_evaluable",
                                     "n_records": n_records, "invalid_outputs": invalid}
    statuses = {check["status"] for check in checks.values()}
    status = "violated" if "violated" in statuses else "not_evaluable" if "not_evaluable" in statuses else "satisfied"
    return {"contract": contract, "status": status, "checks": checks,
            "scope": {"n_evaluated": ledger["n_input"], "unassessed_rows": unassessed,
                      "n_reference_input": ledger["n_input"] + len(unassessed),
                      "complete": not unassessed},
            "interpretation": "A verdict concerns the supplied requirements and observed cohort, not semantic truth. Known violations remain violations when other requirements cannot be assessed. Empty common cohorts cannot certify label preservation or absence of new common-row disagreement."}
