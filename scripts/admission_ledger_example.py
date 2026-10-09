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


def main():
    import argparse
    import json

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Print complete records and ledger")
    args = parser.parse_args()
    report = example()
    if args.json:
        print(json.dumps(report, indent=2))
        return
    ledger = report["ledger"]
    print("Constructed example; no model calls or experimental observations.")
    print("Both versions admit 5 of 7 rows and count 3 disagreements.")
    for name, rows in ledger["sets"].items():
        print(f"{name}: {rows}")
    print("One excluded disagreement and one new disagreement cancel.")
    print("Two common-row label changes act in opposite directions.")
    print(f"Disagreement count identity: {json.dumps(ledger['identity'])}")
    print("Use --json to inspect raw/saved validity, transitions and records.")


if __name__ == "__main__":
    main()
