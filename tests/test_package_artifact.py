import hashlib
import json
import zipfile

import pytest

from scripts.package_artifact import PREFIX, MANIFEST, verify_archive, write_archive


def test_archive_round_trip_keeps_nested_bytes_and_generated_readme(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"raw\0evidence\r\n")
    expected = {"sha256": hashlib.sha256(source.read_bytes()).hexdigest(), "bytes": source.stat().st_size}
    archive = tmp_path / "review.zip"
    manifest = write_archive(archive, {"logs/nested/raw.jsonl": (source, expected)}, {"ARCHIVE-README.md": b"review"})
    assert len(manifest["files"]) == 2
    assert verify_archive(archive) == manifest
    with zipfile.ZipFile(archive) as bundle:
        assert bundle.read(PREFIX + "logs/nested/raw.jsonl") == source.read_bytes()


def test_packaging_refuses_changed_source(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"changed")
    with pytest.raises(ValueError, match="Source changed"):
        write_archive(tmp_path / "review.zip", {"data/source": (source, {"sha256": "old", "bytes": 3})}, {})


def test_verification_rejects_replaced_payload(tmp_path):
    archive = tmp_path / "review.zip"
    manifest = {"files": {"data/source": {"bytes": 3, "sha256": hashlib.sha256(b"old").hexdigest()}}}
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr(PREFIX + "data/source", b"new")
        bundle.writestr(MANIFEST, json.dumps(manifest))
    with pytest.raises(ValueError, match="checksum mismatch"):
        verify_archive(archive)


def test_package_includes_frozen_plan_and_excludes_internal_paper_notes(tmp_path, monkeypatch):
    from scripts import package_artifact as packaging
    from scripts import prepare_revision_evaluation as preparation

    reproduction = tmp_path / "audits/reproduction"
    reproduction.mkdir(parents=True)
    (reproduction / "report.json").write_text(json.dumps({"passed": True, "checks": {"reproduced": True}}))
    sources = (*packaging.PROTOCOL_FILES, "paper/reproducibility.tex",
               "paper/VLDB-REVIEW-20261008.md", "paper/PAPER-PLAN.md",
               "docs/ARTIFACT-README.md", "docs/VENUE-ASSESSMENT-20261009.md")
    records = {}
    for name in sources:
        source = tmp_path / name
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_bytes(name.encode())
        records[name] = {"sha256": packaging.digest(source.read_bytes()), "bytes": source.stat().st_size}
    inventory = {name: record for name, record in records.items() if not name.startswith("artifacts/")}
    (reproduction / "manifest.json").write_text(json.dumps({"files": inventory}))
    monkeypatch.setattr(packaging, "ROOT", tmp_path)
    monkeypatch.setattr(packaging, "DEFAULT_OUTPUT", reproduction)
    monkeypatch.setattr(packaging, "inventory", lambda: inventory)
    verified = []
    monkeypatch.setattr(preparation, "verify", lambda path: verified.append(path))

    archive = packaging.package(tmp_path / "artifacts/review.zip")
    files = packaging.verify_archive(archive)["files"]
    assert verified == [preparation.DEFAULT_OUTPUT]
    assert set(packaging.PROTOCOL_FILES) <= set(files)
    assert "paper/reproducibility.tex" in files
    assert "paper/VLDB-REVIEW-20261008.md" not in files
    assert "paper/PAPER-PLAN.md" not in files
    assert "docs/VENUE-ASSESSMENT-20261009.md" not in files
