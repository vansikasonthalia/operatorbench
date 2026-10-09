import json
from pathlib import Path
import subprocess
import sys

import pytest

from harness.admission import build_ledger
from harness.revision import check_revision
from harness.revision_assertions import check_records
from scripts import prepare_revision_evaluation as preparation


@pytest.mark.parametrize("before,after,contract,missing", [
    ({0: "ML", 1: "theory"}, {0: "ML"}, {"admission_relation": "subset", "preserve_common_labels": True}, []),
    ({0: "ML", 1: "theory"}, {0: "ML"}, {"admission_relation": "equal"}, []),
    ({0: "ML"}, {0: "ML", 1: "ML"}, {"admission_relation": "subset"}, []),
    ({0: "ML"}, {1: "ML"}, {"admission_relation": "superset"}, []),
    ({0: "ML", 1: "theory"}, {0: "theory", 1: "ML"},
     {"admission_relation": "equal", "forbid_new_common_disagreements": True}, []),
    ({0: "ML"}, {0: "theory"}, {"admission_relation": "equal", "preserve_common_labels": True}, [4]),
    ({0: "ML"}, {0: "ML"}, {"admission_relation": "equal"}, [4]),
    ({0: "ML"}, {0: "ML"}, {"admission_relation": "equal", "require_complete_input": False}, [4]),
    ({0: "ML"}, {1: "ML"}, {"admission_relation": "unrestricted", "preserve_common_labels": True}, []),
    ({0: "ML"}, {1: "ML"}, {"admission_relation": "unrestricted", "forbid_new_common_disagreements": True}, []),
    ({}, {}, {"admission_relation": "equal"}, []),
    ({0: "invalid"}, {0: "ML"}, {"admission_relation": "equal"}, []),
])
def test_direct_baseline_agrees_on_status_and_exact_exception_rows(before, after, contract, missing):
    def records(labels):
        return {row: {"label": label, "raw": label} for row, label in labels.items()}
    before, after = records(before), records(after)
    valid = {"ML", "theory"}
    predicate = lambda label: label != "ML"
    ledger = build_ledger(before, after, range(4), label_column="label", raw_column="raw",
                          valid_labels=valid, disagreement=predicate)
    audited = check_revision(ledger, **contract, unassessed_rows=missing)
    direct = check_records(before, after, range(4), label_column="label",
                           raw_valid=lambda row: row["raw"] in valid, saved_valid=lambda label: label in valid,
                           disagreement=predicate, **contract, unassessed_rows=missing)
    assert direct["status"] == audited["status"]
    assert set(direct["checks"]) == set(audited["checks"])
    for name, check in direct["checks"].items():
        reference = audited["checks"][name]
        assert check["status"] == reference["status"]
        if name == "output_validity":
            expected = sorted({row for side in reference["invalid_outputs"].values()
                               for layer in side.values() for row in layer["row_ids"]})
        else:
            expected = reference["unassessed_rows"] if name == "complete_input" else reference["row_ids"]
        assert check["row_ids"] == expected


def test_direct_baseline_checks_raw_validity_independently_of_saved_labels():
    before = {0: {"label": "ML", "raw": "The label is ML"}}
    after = {0: {"label": "ML", "raw": "ML"}}
    direct = check_records(before, after, [0], label_column="label", admission_relation="equal",
                           raw_valid=lambda row: row["raw"] == "ML", saved_valid=lambda label: label == "ML",
                           disagreement=lambda label: label != "ML")
    assert direct["status"] == "violated"
    assert direct["checks"]["output_validity"]["row_ids"] == [0]


def test_direct_baseline_import_is_independent_and_has_no_model_clients():
    code = "import sys; import harness.revision_assertions; assert not {'harness.admission', 'harness.revision', 'lotus', 'litellm'} & sys.modules.keys()"
    subprocess.run([sys.executable, "-c", code], cwd=preparation.ROOT, check=True)


@pytest.fixture
def protocol_workspace(tmp_path, monkeypatch):
    monkeypatch.setattr(preparation, "ROOT", tmp_path)
    data = tmp_path / "data.csv"
    data.write_text("row_id,abstract_text\n" + "".join(f"{row},text {row}\n" for row in range(120)))
    monkeypatch.setattr(preparation.control, "DATA", data)
    for name in preparation.SOURCES:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(name)
    return tmp_path / "artifacts/protocol"


def test_plan_freezes_rows_schedule_and_sources_without_overwriting(protocol_workspace):
    output = protocol_workspace
    plan = preparation.freeze(output)
    assert len(plan["row_ids"]) == len(set(plan["row_ids"])) == 100
    assert len(plan["jobs"]) == 280
    assert plan["estimate"]["row_calls"] == 2800
    assert plan["execution"]["implemented"] is False
    for rep in range(1, 5):
        for cell in preparation.QUESTIONS:
            selected = [row for job in plan["jobs"] if job["replicate"] == rep and job["cell"] == cell
                        for row in job["row_ids"]]
            assert sorted(selected) == plan["row_ids"]
    assert preparation.verify(output) == plan
    original = (output / "plan.json").read_bytes()
    with pytest.raises(FileExistsError):
        preparation.freeze(output)
    assert (output / "plan.json").read_bytes() == original


def test_plan_verification_rejects_modified_protocol(protocol_workspace):
    output = protocol_workspace
    plan = preparation.freeze(output)
    plan["model"] = "different-model"
    (output / "plan.json").write_text(json.dumps(plan))
    with pytest.raises(ValueError, match="plan bytes"):
        preparation.verify(output)


@pytest.mark.parametrize("target", ["dataset", "source"])
def test_plan_verification_rejects_changed_inputs_or_baseline(protocol_workspace, target):
    output = protocol_workspace
    preparation.freeze(output)
    path = preparation.control.DATA if target == "dataset" else preparation.ROOT / "harness/revision_assertions.py"
    path.write_text("changed")
    with pytest.raises(ValueError, match="changed"):
        preparation.verify(output)
