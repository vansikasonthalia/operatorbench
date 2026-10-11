import json
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

from harness.metrics import consistency_across_runs, context_sensitivity_drift, raw_schema_compliance, disagreement_breakdown
from harness.artifacts import load_completed_pipeline
from harness.intercept import LLMInterceptor
from scripts.audit_call_logs import audit
from scripts import f7_precision_paired


def test_no_overlap_is_undefined():
    a = pd.DataFrame({"label": ["ML"]}, index=[1])
    b = pd.DataFrame({"label": ["ML"]}, index=[2])
    result = consistency_across_runs([a, b], "label")
    assert result["stability_rate"] is None
    assert result["ci_upper"] is None
    assert result["n_excluded_rows"] == 2
    assert context_sensitivity_drift(a, b, "label")["drift_rate"] is None


def test_duplicate_ids_rejected():
    a = pd.DataFrame({"row_id": [1, 1], "label": ["ML", "other"]})
    with pytest.raises(ValueError, match="Duplicate"):
        consistency_across_runs([a, a], "label")
    with pytest.raises(ValueError, match="Duplicate"):
        context_sensitivity_drift(a, a, "label")


def test_raw_compliance_keeps_verbose_and_missing_failures():
    a = pd.DataFrame({"raw": [" ML ", "The answer is ML", None, "not ML"]})
    assert raw_schema_compliance(a, "raw", {"ML"})["violation_rows"] == [1, 2, 3]


@pytest.mark.parametrize("use_column", [False, True])
def test_raw_compliance_reports_row_ids(use_column):
    frame = pd.DataFrame({"raw": ["ML", "verbose ML", None]},
                         index=[70, 30, 90])
    if use_column:
        frame["row_id"] = [12, 45, 23]
    expected = [45, 23] if use_column else [30, 90]
    assert raw_schema_compliance(frame, "raw", {"ML"})["violation_rows"] == expected


def test_invalid_output_is_not_semantic_disagreement():
    frame = pd.DataFrame({"label": ["ML", "other", "garbage", None]})
    result = disagreement_breakdown(frame, "label", {"ML", "other"},
                                    lambda passed, label: label != "ML")
    assert result == {"n_total": 4, "agreement": 1,
                      "valid_label_disagreement": 1, "invalid_output": 2}


def test_resume_rejects_partial_and_mismatched_files(tmp_path):
    mapped = pd.DataFrame({"label": ["ML"]}, index=pd.Index([1], name="row_id"))
    mapped.to_csv(tmp_path / "run_map.csv")
    with pytest.raises(ValueError, match="Incomplete"):
        load_completed_pipeline(tmp_path, "run", [1, 2], "label")
    mapped.to_csv(tmp_path / "run_filter.csv")
    (tmp_path / "run_meta.json").write_text(json.dumps({"total_rows": 2, "filter_positive_count": 2}))
    with pytest.raises(ValueError, match="counts"):
        load_completed_pipeline(tmp_path, "run", [1, 2], "label")
    (tmp_path / "run_meta.json").write_text(json.dumps({"total_rows": 2, "filter_positive_count": 1}))
    with pytest.raises(ValueError, match="Historical artifact"):
        load_completed_pipeline(tmp_path, "run", [1, 2], "label")
    with pytest.warns(UserWarning, match="structural checks only"):
        assert len(load_completed_pipeline(tmp_path, "run", [1, 2], "label", allow_legacy=True)) == 1


@pytest.mark.parametrize("nested", [False, True])
def test_batch_logs_pair_prompts_once(tmp_path, monkeypatch, nested):
    def completion(model, messages):
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=messages[0]["content"]))])
    fake = SimpleNamespace(completion=completion)
    def batch(model, messages):
        call = fake.completion if nested else completion
        return [call(model, prompt) for prompt in messages]
    fake.batch_completion = batch
    monkeypatch.setitem(sys.modules, "litellm", fake)
    interceptor = LLMInterceptor(tmp_path)
    interceptor.install()
    try:
        fake.batch_completion("model", [[{"role": "user", "content": "a"}], [{"role": "user", "content": "b"}]])
    finally:
        interceptor.uninstall()
    assert len(interceptor.calls) == 2
    for record in interceptor.calls:
        assert record["response_text"] == record["messages"][0]["content"]
    assert not audit(tmp_path / "llm_calls.jsonl")["invalid_lines"]


def test_log_audit_reports_corruption_without_rewriting(tmp_path):
    path = tmp_path / "calls.jsonl"
    original = '{"messages": []}\nbroken\n'
    path.write_text(original)
    result = audit(path)
    assert result["valid_records"] == 1
    assert result["invalid_lines"][0]["line"] == 2
    assert path.read_text() == original


@pytest.mark.parametrize("keep_ml, direction", [(True, "increased"), (False, "decreased")])
def test_analysis_conclusion_follows_changed_data(tmp_path, monkeypatch, keep_ml, direction):
    labels = pd.DataFrame({"topic_label_v2": ["machine_learning"] * 20 + ["other"] * 20},
                          index=pd.Index(range(40), name="row_id"))
    labels_path = tmp_path / "labels.csv"
    labels.to_csv(labels_path)
    base_ids = list(range(10)) + list(range(20, 30))
    retained = list(range(10)) if keep_ml else list(range(20, 30))
    for name, ids in (("rq3", base_ids), ("f7_ablation", retained)):
        pd.DataFrame(index=pd.Index(ids, name="row_id")).to_csv(tmp_path / f"{name}_abstracts_rep1_map.csv")
    monkeypatch.setattr(f7_precision_paired, "RESULTS", tmp_path)
    monkeypatch.setattr(f7_precision_paired, "LABELS", labels_path)
    result = f7_precision_paired.analyse(1)
    assert result["A_proxy_agreement_paired"]["verdict"].endswith(direction)
    expected = "proxy-negative" if keep_ml else "proxy-positive"
    assert result["B_selectivity_of_removal"]["verdict"].startswith(expected)


@pytest.mark.framework
def test_saved_run_checksums_detect_partial_overwrite(tmp_path, monkeypatch):
    from harness.runner import PipelineRunner
    from harness import provenance
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(provenance, "run_provenance", lambda **kwargs: {})
    runner = PipelineRunner.__new__(PipelineRunner)
    runner.batch_size = 10
    runner.batch_delay_seconds = 0
    runner.max_text_chars = 4000
    runner.backend = "hosted"
    runner.seed = None
    runner.cache = SimpleNamespace(enabled=False)
    frame = pd.DataFrame({"label": ["ML"]}, index=pd.Index([1], name="row_id"))
    runner._save_run_results({"run_id": "run", "filter_results": frame,
                             "map_results": frame, "total_rows": 2,
                             "filter_positive_count": 1,
                             "filter_predicate": "filter", "map_instruction": "map",
                             "experiment_identity": {"test": "identity"}})
    root = tmp_path / "results"
    assert len(load_completed_pipeline(root, "run", [1, 2], "label", expected_identity={"test": "identity"})) == 1
    with (root / "run_map.csv").open("a") as stream:
        stream.write("2,other\n")
    with pytest.raises(ValueError, match="hash mismatch"):
        load_completed_pipeline(root, "run", [1, 2], "label")
