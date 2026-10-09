"""Admission accounting for two saved runs with a fixed disagreement rule."""
from collections import defaultdict


def transitions(before, after, common, column):
    groups = defaultdict(list)
    for row in sorted(common):
        groups[before[row][column], after[row][column]].append(row)
    return [{"before": left, "after": right, "count": len(rows), "row_ids": rows}
            for (left, right), rows in groups.items()]


def build_ledger(before, after, universe, *, label_column, raw_column,
                 valid_labels, disagreement):
    universe = set(universe)
    if not (before.keys() | after.keys()) <= universe:
        raise ValueError("Admitted row IDs must belong to the input universe")
    common = before.keys() & after.keys()
    excluded = before.keys() - after.keys()
    new = after.keys() - before.keys()
    valid = {label.strip().lower() for label in valid_labels}
    indicators = [{row: bool(disagreement(record[label_column])) for row, record in run.items()}
                  for run in (before, after)]

    def summary(run, flags):
        count = sum(flags.values())
        invalid = {}
        for name, column in (("raw", raw_column), ("saved", label_column)):
            rows = sorted(row for row, record in run.items()
                          if str(record[column]).strip().lower() not in valid)
            invalid[name] = {"count": len(rows), "row_ids": rows}
        return {"admitted": len(run), "disagreements": count,
                "disagreement_rate": count / len(run) if run else None,
                "invalid_outputs": invalid}

    added = sorted(row for row in common if not indicators[0][row] and indicators[1][row])
    resolved = sorted(row for row in common if indicators[0][row] and not indicators[1][row])
    terms = {"introduced_by_new_admissions": sum(indicators[1][row] for row in new),
             "removed_by_exclusions": sum(indicators[0][row] for row in excluded),
             "common_rows_new_disagreements": len(added),
             "common_rows_resolved_disagreements": len(resolved),
             "common_rows_net": len(added) - len(resolved)}
    before_summary, after_summary = [summary(run, flags) for run, flags in zip((before, after), indicators)]
    observed = after_summary["disagreements"] - before_summary["disagreements"]
    accounted = terms["introduced_by_new_admissions"] - terms["removed_by_exclusions"] + terms["common_rows_net"]
    if observed != accounted:
        raise ValueError("Admission ledger does not reconcile the disagreement counts")
    return {
        "measurement": {"label_column": label_column, "raw_column": raw_column,
                        "valid_labels": sorted(valid_labels),
                        "validity_rule": "Exact label membership, ignoring case and outer whitespace; validity does not change the supplied disagreement rule."},
        "n_input": len(universe), "before": before_summary, "after": after_summary,
        "sets": {"common": sorted(common), "excluded": sorted(excluded),
                 "newly_admitted": sorted(new), "never_admitted": sorted(universe - (before.keys() | after.keys()))},
        "terms": terms,
        "common_indicator_changes": {"new_disagreements": added, "resolved_disagreements": resolved},
        "label_transitions": {"saved": transitions(before, after, common, label_column),
                              "raw": transitions(before, after, common, raw_column)},
        "identity": {"observed_count_change": observed, "accounted_count_change": accounted, "holds": True},
    }
