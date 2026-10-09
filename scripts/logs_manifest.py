"""
OperatorBench -- manifest of the raw call logs, so the archive is verifiable.

WHY THE LOGS ARE NOT IN GIT
---------------------------
`logs/` is ~121 MB, including single JSONL files of 43.7 MB and 36.1 MB. Git
stores every version forever, so committing them would (a) make every clone pay
121 MB, (b) trip GitHub's large-file warnings, and (c) mean a history rewrite if
they ever need removing. Standard practice for a research artifact is to keep bulk
raw data out of git and publish it as a versioned archive -- a GitHub release asset
or, better for a paper, a Zenodo deposit with a DOI that Reproducibility can cite.

But an archive nobody can verify is not evidence. So THIS manifest is tracked in
git: it records every log file's size, line count and sha256. Anyone who downloads
the archive can confirm byte-for-byte that it is the one the paper was written
against, and the repo alone is enough to detect a tampered or truncated download.

That split -- checksums in git, bytes in the archive -- is the point.

Usage:
    python scripts/logs_manifest.py            # write logs_manifest.json
    python scripts/logs_manifest.py --verify    # check logs/ against the manifest

To build the archive for release (run when publishing, not routinely -- it writes
a large file):
    python -c "import shutil; shutil.make_archive('operatorbench-logs','zip','logs')"
"""

import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "logs"
MANIFEST = ROOT / "logs_manifest.json"


def sha256(path: Path, chunk=1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def scan() -> dict:
    files = {}
    total = 0
    for p in sorted(LOGS.rglob("*")):
        if not p.is_file():
            continue
        rel = p.relative_to(ROOT).as_posix()
        size = p.stat().st_size
        total += size
        lines = None
        if p.suffix in (".jsonl", ".json", ".md", ".txt"):
            try:
                with p.open("rb") as f:
                    lines = sum(1 for _ in f)
            except OSError:
                pass
        files[rel] = {"bytes": size, "lines": lines, "sha256": sha256(p)}
    return {
        "purpose": ("checksums for the raw call logs, which are distributed as a "
                    "separate archive rather than committed to git"),
        "why_not_in_git": ("~121 MB including files of 43.7 MB and 36.1 MB; git "
                           "history is permanent and every clone would pay for it"),
        "archive_contents": "the logs/ directory, zipped at its root",
        "n_files": len(files),
        "total_bytes": total,
        "total_mb": round(total / 1048576, 1),
        "files": files,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--verify", action="store_true",
                    help="check logs/ against the committed manifest; write nothing")
    args = ap.parse_args()

    if not LOGS.is_dir():
        print(f"no logs/ directory at {LOGS}")
        return 1

    if args.verify:
        if not MANIFEST.exists():
            print("no logs_manifest.json to verify against")
            return 1
        want = json.loads(MANIFEST.read_text())["files"]
        have = scan()["files"]
        missing = sorted(set(want) - set(have))
        extra = sorted(set(have) - set(want))
        changed = [k for k in sorted(set(want) & set(have))
                   if want[k]["sha256"] != have[k]["sha256"]]
        print(f"manifest: {len(want)} files   on disk: {len(have)}")
        for label, items in (("MISSING", missing), ("UNTRACKED", extra),
                             ("CHANGED", changed)):
            if items:
                print(f"  {label} ({len(items)}):")
                for i in items[:10]:
                    print(f"     {i}")
        ok = not (missing or extra or changed)
        print("  VERIFIED — logs/ matches the manifest exactly" if ok
              else "  MISMATCH — see above")
        return 0 if ok else 1

    m = scan()
    MANIFEST.write_text(json.dumps(m, indent=2))
    print(f"{m['n_files']} files, {m['total_mb']} MB")
    print(f"wrote {MANIFEST.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
