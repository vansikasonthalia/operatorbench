import hashlib
import json

import pandas as pd
import pytest

from harness.artifacts import atomic_write, experiment_identity, load_completed_pipeline


@pytest.mark.parametrize("change", ["rejected_id", "text", "prompt", "model", "seed"])
def test_resume_rejects_changed_experiment(tmp_path, change):
    frame = pd.DataFrame({"text": ["one", "two"]}, index=[1, 2])
    config = {"prompt": "old", "model": "a", "seed": 1}
    identity = experiment_identity(frame, config)
    hashes = {}
    for kind in ["filter", "map"]:
        path = tmp_path / f"run_{kind}.csv"
        pd.DataFrame({"label": ["ML"]}, index=pd.Index([1], name="row_id")).to_csv(path)
        hashes[kind] = hashlib.sha256(path.read_bytes()).hexdigest()
    (tmp_path / "run_meta.json").write_text(json.dumps({
        "total_rows": 2, "filter_positive_count": 1, "completion_status": "complete",
        "artifact_sha256": hashes, "experiment_identity": identity,
    }))
    assert len(load_completed_pipeline(tmp_path, "run", frame.index, "label", expected_identity=identity)) == 1
    if change == "rejected_id":
        frame.index = [1, 999]
    elif change == "text":
        frame.loc[2, "text"] = "changed"
    else:
        config[change] = 2 if change == "seed" else "changed"
    with pytest.raises(ValueError, match="identity mismatch"):
        load_completed_pipeline(tmp_path, "run", frame.index, "label",
                                expected_identity=experiment_identity(frame, config))


def test_failed_atomic_write_preserves_previous_file(tmp_path, monkeypatch):
    import harness.artifacts as artifacts
    path = tmp_path / "result.csv"
    path.write_text("previous")
    def fail(*args):
        raise OSError("interrupted")
    monkeypatch.setattr(artifacts.os, "replace", fail)
    with pytest.raises(OSError, match="interrupted"):
        atomic_write(path, "replacement")
    assert path.read_text() == "previous"
    assert list(tmp_path.iterdir()) == [path]
