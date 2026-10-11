from unittest.mock import Mock

import pandas as pd
import pytest

pytestmark = pytest.mark.framework


def test_rq1_resumes_only_verified_complete_operator(tmp_path, monkeypatch):
    from scripts import run_rq1 as rq
    monkeypatch.setattr(rq, "RESULTS_DIR", tmp_path)
    monkeypatch.setattr(rq, "write_rq1_metadata", lambda *a, **kw: None)
    frame = pd.DataFrame({"review_text": ["a", "b"]}, index=pd.Index([1, 2], name="row_id"))
    mapped = frame.assign(mapped_sentiment="negative", _raw_mapped_sentiment="negative")
    runner = Mock(backend="hosted")
    runner.experiment_identity.return_value = {"input": "same", "config": "same"}
    runner.run_filter_only.return_value = frame.iloc[:1]
    runner.run_map_only.side_effect = [RuntimeError("interrupted"), mapped]
    with pytest.raises(RuntimeError, match="interrupted"):
        rq.run_replicate(runner, frame, "reviews", "test", 1)
    rq.run_replicate(runner, frame, "reviews", "test", 1)
    assert runner.run_filter_only.call_count == 1
    assert runner.run_map_only.call_count == 2
    rq.run_replicate(runner, frame, "reviews", "test", 1)
    assert runner.run_map_only.call_count == 2
    path = tmp_path / "test_reviews_rep1_filter.csv"
    path.write_text("row_id,filter_passed\n1,True\n")
    with pytest.raises(ValueError, match="hash mismatch"):
        rq.run_replicate(runner, frame, "reviews", "test", 1)


def test_rq1_rejects_legacy_partial_csv_before_paid_call(tmp_path, monkeypatch):
    from scripts import run_rq1 as rq
    monkeypatch.setattr(rq, "RESULTS_DIR", tmp_path)
    (tmp_path / "test_reviews_rep1_filter.csv").write_text("row_id,filter_passed\n1,True\n")
    runner = Mock()
    runner.experiment_identity.return_value = {"input": "current"}
    with pytest.raises(ValueError, match="Missing execution manifest"):
        rq.run_replicate(runner, pd.DataFrame(index=[1, 2]), "reviews", "test", 1)
    runner.run_filter_only.assert_not_called()
    runner.run_map_only.assert_not_called()


@pytest.mark.parametrize("second_labels", [[], ["positive"]])
def test_rq3_empty_admissions_are_undefined_and_printable(tmp_path, monkeypatch, capsys, second_labels):
    from scripts import run_rq3 as rq
    monkeypatch.setattr(rq, "ROOT", tmp_path)
    (tmp_path / "results").mkdir()
    for rep, labels in enumerate([[], second_labels], 1):
        pd.DataFrame({"mapped_sentiment": labels}, index=pd.Index(range(len(labels)), name="row_id")).to_csv(
            tmp_path / "results" / f"rq3_reviews_rep{rep}_map.csv")
    result = rq.aggregate_domain("reviews", 2, 500)
    assert result["per_replicate"][0]["rate"] is None
    assert result["shared_row_cohort"]["ever_contradicted"]["rate"] is None
    assert result["pooled"]["rate"] == (1.0 if second_labels else None)
    rq.print_domain_summary(result)
    assert "not evaluable" in capsys.readouterr().out


def test_empty_f7_cannot_be_classified_as_success(tmp_path, monkeypatch, capsys):
    from scripts import run_f7_ablation as f7
    monkeypatch.setattr(f7, "ROOT", tmp_path)
    (tmp_path / "results").mkdir()
    baseline = pd.DataFrame({"mapped_topic": ["other"]}, index=pd.Index([1], name="row_id"))
    for kind in ["filter", "map"]:
        baseline.to_csv(tmp_path / "results" / f"rq3_abstracts_rep1_{kind}.csv")
    baseline.iloc[:0].to_csv(tmp_path / "results" / "f7_ablation_abstracts_rep1_map.csv")
    result = f7.analyse(1, pd.DataFrame(index=[1]))
    assert result["contradiction_ablated"]["rate"] is None
    assert result["contradiction_delta_pp"] is None
    assert "not evaluable" in result["verdict"]
    f7.report([result])
    assert "not evaluable" in capsys.readouterr().out
