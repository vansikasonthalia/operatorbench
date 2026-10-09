"""Read-only JSONL integrity audit. Never repairs or rewrites frozen logs."""
import argparse
import json
from pathlib import Path


def audit(path):
    invalid = []
    valid = 0
    with Path(path).open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            try:
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise ValueError("record is not an object")
                messages = record.get("messages")
                if not isinstance(messages, list) or any(not isinstance(m, dict) for m in messages):
                    raise ValueError("messages are not an individual conversation")
                valid += 1
            except (ValueError, TypeError) as exc:
                invalid.append({"line": line_number, "error": str(exc)})
    return {"path": str(path), "valid_records": valid, "invalid_lines": invalid}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", type=Path)
    reports = [audit(path) for path in parser.parse_args().paths]
    print(json.dumps(reports, indent=2))
    raise SystemExit(1 if any(r["invalid_lines"] for r in reports) else 0)
