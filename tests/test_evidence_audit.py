import json
from pathlib import Path

import pytest

from scripts import audit_frozen_evidence as audit
from scripts.recompute_evidence_audit import differences, failed


def test_duplicate_csv_ids_are_rejected():
    with pytest.raises(ValueError, match="duplicate"):
        audit.indexed([{"row_id": "1"}, {"row_id": "1"}])


def test_csv_audit_preserves_nuls_and_embedded_newlines(tmp_path):
    source = tmp_path / "source.csv"
    content = 'row_id,text\r\n1,"before\0after\r\nnext"\r\n'
    source.write_bytes(content.encode())
    assert audit.read_csv(source) == [{"row_id": "1", "text": "before\0after\r\nnext"}]
    assert source.read_bytes() == content.encode()


def test_log_audit_keeps_archived_duplicates_separate(tmp_path, monkeypatch):
    logs = tmp_path / "logs"
    archive = logs / "_pre_dedup"
    archive.mkdir(parents=True)
    record = {"run_id": "run", "operator": "sem_map", "row_id": 1, "raw_output": "ML"}
    text = json.dumps(record) + "\n"
    (logs / "run.jsonl").write_text(text + "broken\n")
    (archive / "run.jsonl").write_text(text * 2)
    monkeypatch.setattr(audit, "ROOT", tmp_path)
    reports, operators = audit.audit_logs()
    assert len(operators[("run", "sem_map")]) == 1
    assert sum(len(r["invalid_lines"]) for r in reports) == 1
    assert next(r for r in reports if r["archived"])["duplicate_operator_keys"] == 1
    assert (logs / "run.jsonl").read_text() == text + "broken\n"


def test_numeric_comparison_detects_changed_counts():
    changes = differences({"count": 4, "note": "old"}, {"count": 5, "note": "new"})
    assert changes == [{"field": ".count", "old": 4, "new": 5}]


def test_numeric_comparison_detects_missing_fields_and_boolean_type_changes():
    assert differences({"count": 4}, {}) == [{"field": ".count", "old": 4, "missing": True}]
    assert differences(True, 1)
    assert not differences({"note": "old"}, {"note": "new", "disclosure": 5})


def test_missing_output_and_changed_results_fail_reproduction():
    assert failed([{"exit_status": 0, "comparisons": []}])
    assert failed([{"exit_status": 0, "comparisons": [{"numeric_or_boolean_changes": [{"missing_frozen_output": True}]}]}])
    assert not failed([{"exit_status": 0, "comparisons": [{"numeric_or_boolean_changes": []}]}])
