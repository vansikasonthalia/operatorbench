"""Synthetic procedure demonstration; contains no model observations."""
from harness.admission import build_ledger


def example():
    def row(saved, raw):
        return {"label": saved, "raw": raw}
    before = {0: row("ML", "ML"), 1: row("theory", "theory"),
              2: row("ML", "The label is ML"), 3: row("mixed", "mixed"),
              4: row("theory", "theory")}
    after = {0: row("theory", "theory"), 1: row("ML", "ML"),
             2: row("ML", "ML"), 3: row("mixed", ""),
             5: row("systems", "systems")}
    return {"scope": "Synthetic procedure example, not an experiment or robustness analysis.",
            "rule": "Every saved label other than ML counts as disagreement, including invalid labels. Raw/saved validity is reported separately; exact membership ignores case and outer whitespace.",
            "input_rows": list(range(7)), "before_records": before, "after_records": after,
            "ledger": build_ledger(before, after, range(7), label_column="label", raw_column="raw",
                                   valid_labels={"ML", "systems", "theory", "other"},
                                   disagreement=lambda label: label.strip().lower() != "ml")}
