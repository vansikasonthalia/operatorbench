"""Package saved scientific evidence for local review; never uploads files."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import zipfile

from scripts.reproduce_artifact import ROOT, DEFAULT_OUTPUT, inventory

PREFIX = "OperatorBench/"
MANIFEST = PREFIX + "artifact-manifest.json"
REVIEW_DOCS = {"docs/ARTIFACT-README.md", "docs/matched_control.md",
               "docs/REVISION-EVALUATION.md", "docs/REVISION-RUNNER.md",
               "docs/RESULTS.md", "docs/CODE-GUIDE.md"}
REVIEW_PAPER = {"paper/reproducibility.tex", "paper/README.md"}
PROTOCOL_FILES = ("artifacts/revision-evaluation-20261008/plan.json",
                  "artifacts/revision-evaluation-20261008/plan.sha256")


def digest(data):
    return hashlib.sha256(data).hexdigest()


def verify_archive(path):
    with zipfile.ZipFile(path) as archive:
        manifest = json.loads(archive.read(MANIFEST))
        expected = {PREFIX + name for name in manifest["files"]} | {MANIFEST}
        names = archive.namelist()
        if len(names) != len(set(names)) or set(names) != expected:
            raise ValueError("Archive entries do not match its manifest")
        for name, record in manifest["files"].items():
            data = archive.read(PREFIX + name)
            if len(data) != record["bytes"] or digest(data) != record["sha256"]:
                raise ValueError(f"Archive checksum mismatch: {name}")
    return manifest


def write_archive(path, files, generated):
    records = {}
    with zipfile.ZipFile(path, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for name, (source, expected) in sorted(files.items()):
            data = source.read_bytes()
            observed = {"sha256": digest(data), "bytes": len(data)}
            if observed != expected:
                raise ValueError(f"Source changed before packaging: {name}")
            archive.writestr(PREFIX + name, data)
            records[name] = observed
        for name, data in sorted(generated.items()):
            if name in records:
                raise ValueError(f"Duplicate generated entry: {name}")
            archive.writestr(PREFIX + name, data)
            records[name] = {"sha256": digest(data), "bytes": len(data)}
        archive.writestr(MANIFEST, json.dumps({
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "algorithm": "SHA-256", "files": records,
            "scope": "Scientific source, frozen evidence, reviewer documentation and reference reanalysis; manifest excludes itself. No model rerun or correctness adjudication.",
        }, indent=2).encode())
    return verify_archive(path)


def package(output):
    from scripts import prepare_revision_evaluation as preparation

    output = output.resolve()
    if output.parent != ROOT / "artifacts" or output.suffix != ".zip":
        raise ValueError("Write the review ZIP directly under artifacts/")
    report = json.loads((DEFAULT_OUTPUT / "report.json").read_text(encoding="utf-8"))
    if not report["passed"] or not all(report["checks"].values()):
        raise ValueError("Run the offline reproduction successfully before packaging")
    preparation.verify(preparation.DEFAULT_OUTPUT)
    previous = json.loads((DEFAULT_OUTPUT / "manifest.json").read_text(encoding="utf-8"))["files"]
    current = inventory()
    for name, record in previous.items():
        if name.startswith(("data/", "results/", "logs/", "paper/")) and current.get(name) != record:
            raise ValueError(f"Verified evidence changed: {name}")
    files = {name: (ROOT / name, record) for name, record in current.items()
             if (not name.startswith("docs/") or name in REVIEW_DOCS)
             and (not name.startswith("paper/") or name in REVIEW_PAPER)}
    for name in (*PROTOCOL_FILES, ".gitattributes", ".gitignore", "LICENSE", "NOTICE.md", "CITATION.cff",
                 "pytest.ini", "requirements-test.txt"):
        source = ROOT / name
        if name in PROTOCOL_FILES or source.is_file():
            data = source.read_bytes()
            files[name] = (source, {"sha256": digest(data), "bytes": len(data)})
    for source in sorted(DEFAULT_OUTPUT.rglob("*")):
        if source.is_file():
            name = "review/reference_reproduction/" + source.relative_to(DEFAULT_OUTPUT).as_posix()
            data = source.read_bytes()
            files[name] = (source, {"sha256": digest(data), "bytes": len(data)})
    readme = """# OperatorBench review archive

Extract this ZIP and enter its `OperatorBench` folder. The archive includes
the frozen data, results and raw logs needed for offline reanalysis. A
source-only Git clone omits the raw logs and is insufficient for these checks.

Start with `docs/ARTIFACT-README.md`. Install `requirements-artifact.txt` in
an isolated environment, then run `python -m scripts.reproduce_artifact`.
No API key or model calls are needed. Outputs go to `audits/reproduction/`.
For the completed revision study, also run
`python -m scripts.run_revision_evaluation --aggregate-only`.
The required frozen preparation plan and its checksum are included under
`artifacts/revision-evaluation-20261008/`.

`artifact-manifest.json` checksums every payload file, excluding itself.
The external ZIP `.sha256` file checksums the whole archive, including that
manifest. `review/reference_reproduction/` preserves the successful local
reanalysis before packaging. Its earlier inventory describes that local
checkout; use `artifact-manifest.json` for the exact archive contents.

The 178 malformed historical log lines and incomplete provenance remain
disclosed evidence gaps. Numerical reproducibility is not answer correctness.
The manuscript source is `paper/reproducibility.tex`. This packaging command
does not compile or inspect a PDF. No files are published by this command.
"""
    output.parent.mkdir(parents=True, exist_ok=True)
    manifest = write_archive(output, files, {"ARCHIVE-README.md": readme.encode()})
    checksum = digest(output.read_bytes())
    output.with_suffix(".zip.sha256").write_text(f"{checksum}  {output.name}\n", encoding="utf-8")
    print(json.dumps({"archive": str(output), "sha256": checksum,
                      "payload_files": len(manifest["files"]), "archive_bytes": output.stat().st_size,
                      "all_payload_hashes_verified": True}, indent=2))
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/operatorbench-review-20261008.zip")
    parser.add_argument("--verify", type=Path)
    args = parser.parse_args()
    if args.verify:
        manifest = verify_archive(args.verify)
        print(f"Verified {len(manifest['files'])} payload files")
    else:
        package(args.output)


if __name__ == "__main__":
    main()
