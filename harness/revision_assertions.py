"""Direct assertions on records, independent of the ledger implementation."""


def check_records(before, after, universe, *, label_column, raw_valid, saved_valid,
                  disagreement, admission_relation, preserve_common_labels=False,
                  forbid_new_common_disagreements=False, require_valid_outputs=True,
                  unassessed_rows=(), require_complete_input=True):
    universe = set(universe)
    left, right = set(before), set(after)
    missing = set(unassessed_rows)
    if not (left | right) <= universe or missing & universe:
        raise ValueError("Record IDs or unassessed rows do not match the evaluated universe")
    if admission_relation not in {"equal", "subset", "superset", "unrestricted"}:
        raise ValueError("Unknown admission relation")
    common = left & right
    exceptions = {"equal": left ^ right, "subset": right - left,
                  "superset": left - right, "unrestricted": set()}[admission_relation]
    checks = {"admissions": {"status": "violated" if exceptions else "satisfied",
                             "row_ids": sorted(exceptions)}}
    if require_complete_input:
        checks["complete_input"] = {"status": "not_evaluable" if missing else "satisfied",
                                    "row_ids": sorted(missing)}
    for name, enabled, predicate in (
        ("common_labels", preserve_common_labels,
         lambda row: before[row][label_column] != after[row][label_column]),
        ("new_common_disagreements", forbid_new_common_disagreements,
         lambda row: not disagreement(before[row][label_column]) and disagreement(after[row][label_column])),
    ):
        if enabled:
            bad = sorted(row for row in common if predicate(row))
            checks[name] = {"status": "violated" if bad else "satisfied" if common else "not_evaluable",
                            "row_ids": bad}
    if require_valid_outputs:
        invalid = sorted({row for run in (before, after) for row, record in run.items()
                          if not saved_valid(record[label_column]) or not raw_valid(record)})
        checks["output_validity"] = {"status": "violated" if invalid else
                                     "satisfied" if before or after else "not_evaluable",
                                     "row_ids": invalid}
    statuses = {check["status"] for check in checks.values()}
    return {"status": "violated" if "violated" in statuses else
            "not_evaluable" if "not_evaluable" in statuses else "satisfied", "checks": checks}
