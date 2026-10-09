"""
OperatorBench -- Call-log deduplication

`CallLogger` writes append-only JSONL. When a replicate is re-run (crash,
interrupt, or `--force`), the CSV is overwritten but the JSONL keeps BOTH
passes. On 10 Aug 2026 `rq1_hosted_abstracts_rep4_filter.jsonl` ended up with
1000 records for 500 rows -- every row logged twice.

That is harmless for the metrics, which read the CSVs, but the call logs are
what ships in the paper's reproducibility appendix. A reviewer counting records
would see double.

This script keeps the LAST record per (run_id, operator, row_id) -- the last
write corresponds to the run that produced the surviving CSV -- and rewrites
the file. Originals are backed up to `logs/_pre_dedup/` first.

Usage:
    python scripts/dedup_call_logs.py            # report only, changes nothing
    python scripts/dedup_call_logs.py --apply    # rewrite affected files
"""

import sys
import json
import shutil
import argparse
from pathlib import Path
from collections import Counter, OrderedDict

ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = ROOT / "logs"
BACKUP_DIR = LOG_DIR / "_pre_dedup"

KEY_FIELDS = ("run_id", "operator", "row_id")


def key_of(record: dict):
    return tuple(record.get(f) for f in KEY_FIELDS)


def scan(path: Path):
    """Return (records, n_dupe_keys). Malformed lines are preserved verbatim."""
    records, malformed = [], 0
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                malformed += 1
    counts = Counter(key_of(r) for r in records)
    n_dupes = sum(1 for v in counts.values() if v > 1)
    return records, n_dupes, malformed


def dedup(records):
    """Keep the last occurrence of each key, preserving first-seen order."""
    out = OrderedDict()
    for r in records:
        out[key_of(r)] = r
    return list(out.values())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="actually rewrite files (default is a dry run)")
    ap.add_argument("--glob", default="*.jsonl",
                    help="which logs to check (default all top-level JSONL)")
    args = ap.parse_args()

    files = sorted(LOG_DIR.glob(args.glob))
    if not files:
        print(f"No files matching {args.glob} in {LOG_DIR}")
        return

    affected = []
    print(f"{'file':<52} {'records':>8} {'unique':>8} {'dupes':>7}")
    print("-" * 78)
    for path in files:
        records, n_dupes, malformed = scan(path)
        unique = len(dedup(records))
        flag = "  <-- duplicated" if n_dupes else ""
        print(f"{path.name:<52} {len(records):>8} {unique:>8} {n_dupes:>7}{flag}")
        if malformed:
            print(f"{'':<52} {malformed} malformed line(s) skipped")
        if n_dupes:
            affected.append((path, records, unique))

    if not affected:
        print("\nNo duplicates found. Nothing to do.")
        return

    print(f"\n{len(affected)} file(s) affected.")
    if not args.apply:
        print("Dry run -- nothing written. Re-run with --apply to fix.")
        return

    BACKUP_DIR.mkdir(exist_ok=True)
    for path, records, _ in affected:
        shutil.copy2(path, BACKUP_DIR / path.name)
        cleaned = dedup(records)
        with path.open("w", encoding="utf-8") as fh:
            for r in cleaned:
                fh.write(json.dumps(r) + "\n")
        print(f"  {path.name}: {len(records)} -> {len(cleaned)} records "
              f"(backup in logs/_pre_dedup/)")

    print("\nDone. Verify with a re-run of this script -- it should report zero.")


if __name__ == "__main__":
    main()
