import subprocess
import sys

import pytest

from scripts import reproduce_artifact as artifact


def test_statistics_import_does_not_load_model_clients():
    code = """
import sys
import harness
from harness.metrics import wilson_ci
from harness.normalize import normalize_label
assert harness.normalize_label is normalize_label
assert 'lotus' not in sys.modules
assert 'litellm' not in sys.modules
assert 'harness.runner' not in sys.modules
assert 'PipelineRunner' in harness.__all__
try:
    harness.unknown_export
except AttributeError:
    pass
else:
    raise AssertionError('Unknown export was accepted')
"""
    subprocess.run([sys.executable, "-c", code], cwd=artifact.ROOT, check=True)


@pytest.mark.parametrize("relative", ["results/recomputed", "audits", "audits/2026-10-08/recomputed"])
def test_output_cannot_overwrite_evidence(relative):
    with pytest.raises(ValueError, match="separate output"):
        artifact.run(artifact.ROOT / relative)


def test_admission_ledger_counts_exclusions_separately_from_label_changes(tmp_path, monkeypatch):
    results = tmp_path / "results"
    results.mkdir()
    data = tmp_path / "data/abstracts"
    data.mkdir(parents=True)
    (data / "abstracts_v1.csv").write_text("row_id\n1\n2\n3\n4\n5\n")
    header = "row_id,mapped_topic,_raw_mapped_topic\n"
    for rep in (1, 2):
        (results / f"rq3_abstracts_rep{rep}_map.csv").write_text(header + "1,theory,theory\n2,ML,ML\n3,other,other\n")
        (results / f"f7_ablation_abstracts_rep{rep}_map.csv").write_text(header + "2,theory,theory\n4,ML,ML\n")
    monkeypatch.setattr(artifact, "ROOT", tmp_path)
    reports = artifact.admission_ledger()
    legacy = {"replicate": 1, "common_rows": 1, "excluded_rows": 2, "new_rows": 1,
              "saved_label_changes": 1, "raw_label_changes": 1, "excluded_disagreements": 2}
    assert {key: reports[0][key] for key in legacy} == legacy
    assert reports[0]["sets"]["never_admitted"] == [5]
    assert reports[0]["terms"]["common_rows_new_disagreements"] == 1
    assert reports[0]["identity"]["observed_count_change"] == -1
