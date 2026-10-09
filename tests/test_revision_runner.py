import json
from types import SimpleNamespace

import pandas as pd
import pytest

from scripts import prepare_revision_evaluation as preparation
from scripts import run_matched_control as control
from scripts import run_revision_evaluation as runner


def small_plan():
    return {"model": "gpt-4o-mini-2024-07-18", "temperature_requested": 0.0,
            "max_output_tokens": 16, "block_size": 3, "batch_delay_seconds": 0,
            "replicates": 1, "row_ids": [7, 3, 9], "questions": preparation.QUESTIONS,
            "instructions": {cell: f"{question} {control.FORMAT} {{abstract_text}}"
                             for cell, question in preparation.QUESTIONS.items()},
            "jobs": [{"job": index, "replicate": 1, "cell": cell, "row_ids": [7, 3, 9]}
                     for index, cell in enumerate(preparation.QUESTIONS)],
            "revisions": {name: {"before": ["reference_filter", "reference_map"], **spec,
                                 "require_valid_outputs": True, "require_complete_input": True}
                          for name, spec in preparation.REVISIONS.items()},
            "analysis": {"scope": "Constructed transport test; no provider calls"}}


@pytest.fixture
def mocked_run(monkeypatch):
    import litellm
    import lotus.models
    plan = small_plan()
    frame = pd.DataFrame({"abstract_text": ["paper seven\0with NUL", "paper three", "paper nine"]}, index=plan["row_ids"])
    manifest, prompts = runner.preflight(plan, frame)
    calls = []

    def completion(*args, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="Answer: False"))])

    def evaluate(block, cell, plan):
        for row in reversed(block.index):
            litellm.completion(model=plan["model"], messages=prompts[cell][str(row)])
        return [{"row_id": int(row), "answer": False, "raw_output": "Answer: False",
                 "operator_output": "False"} for row in block.index]

    monkeypatch.setenv("OPENAI_API_KEY", "offline-test")
    monkeypatch.setattr(litellm, "completion", completion)
    monkeypatch.setattr(lotus.models, "LM", lambda **kwargs: SimpleNamespace(kwargs=kwargs))
    monkeypatch.setattr(runner, "evaluate_cell", evaluate)
    return plan, frame, manifest, prompts, calls, evaluate


@pytest.mark.framework
def test_native_preflight_uses_all_seven_cells_without_provider_calls():
    plan = small_plan()
    frame = pd.DataFrame({"abstract_text": ["seven\0line\n", "three", "nine"]}, index=plan["row_ids"])
    manifest, prompts = runner.preflight(plan, frame)
    assert set(prompts) == set(preparation.QUESTIONS)
    assert manifest["native_prompts_sha256"] == control.sha256(control.canonical(prompts))
    assert manifest["requested_lm_kwargs"]["model"] == plan["model"]
    assert prompts["reference_filter"] == prompts["repeat_filter"]
    assert prompts["reference_map"] == prompts["repeat_map"]
    assert prompts["reference_filter"] != prompts["reference_map"]
    assert "\0" in prompts["reference_filter"]["7"][1]["content"]


@pytest.mark.framework
def test_execution_joins_out_of_order_requests_and_resumes_without_repeating(mocked_run, tmp_path):
    plan, frame, manifest, prompts, calls, _ = mocked_run
    runner.execute(tmp_path, plan, frame, manifest, prompts)
    assert len(calls) == 21
    first = runner.read_completed(tmp_path, plan, plan["jobs"][0], manifest, prompts)
    assert first["records"][0]["request_join"]["request_line"] == 3
    runner.execute(tmp_path, plan, frame, manifest, prompts)
    assert len(calls) == 21
    report = runner.analyse(tmp_path, plan, manifest, prompts)
    assert len(report["replicates"][0]["revisions"]) == 4
    assert all(value["baseline_agrees"] for value in report["replicates"][0]["revisions"].values())
    assert not (tmp_path / ".run.lock").exists()


@pytest.mark.framework
def test_failed_attempt_is_retained_and_not_marked_complete(mocked_run, tmp_path, monkeypatch):
    import litellm
    plan, frame, manifest, prompts, calls, evaluate = mocked_run
    original_completion = litellm.completion

    def fail(block, cell, plan):
        litellm.completion(model=plan["model"], messages=prompts[cell][str(block.index[0])])
        raise RuntimeError("constructed failure")

    monkeypatch.setattr(runner, "evaluate_cell", fail)
    with pytest.raises(RuntimeError, match="constructed"):
        runner.execute(tmp_path, plan, frame, manifest, prompts)
    assert not (tmp_path / "job_0000.json").exists()
    assert not (tmp_path / ".run.lock").exists()
    assert litellm.completion is original_completion
    state = json.loads((tmp_path / "attempt_0000_001.json").read_text())
    assert state["status"] == "failed"
    assert state["usage"]["total_calls"] == 1
    monkeypatch.setattr(runner, "evaluate_cell", evaluate)
    runner.execute(tmp_path, plan, frame, manifest, prompts)
    assert len(calls) == 22
    assert runner.read_completed(tmp_path, plan, plan["jobs"][0], manifest, prompts)["attempt"] == 2
    assert (tmp_path / "llm_calls_job_0000_001.jsonl").exists()


@pytest.mark.framework
def test_changed_request_log_or_binding_is_rejected(mocked_run, tmp_path):
    plan, frame, manifest, prompts, calls, _ = mocked_run
    runner.execute(tmp_path, plan, frame, manifest, prompts)
    changed = {**manifest, "runner_source_sha256": "changed"}
    with pytest.raises(ValueError, match="binding differs"):
        runner.execute(tmp_path, plan, frame, changed, prompts)
    assert len(calls) == 21
    (tmp_path / "llm_calls_job_0000_001.jsonl").write_text('{}\n')
    with pytest.raises(ValueError, match="archive changed"):
        runner.read_completed(tmp_path, plan, plan["jobs"][0], manifest, prompts)


@pytest.mark.framework
def test_incomplete_run_cannot_be_aggregated(mocked_run, tmp_path):
    plan, _, manifest, prompts, _, _ = mocked_run
    with pytest.raises(ValueError, match="incomplete"):
        runner.analyse(tmp_path, plan, manifest, prompts)


@pytest.mark.framework
def test_rehashed_request_still_requires_the_bound_native_prompt(mocked_run, tmp_path):
    plan, frame, manifest, prompts, _, _ = mocked_run
    runner.execute(tmp_path, plan, frame, manifest, prompts)
    checkpoint = tmp_path / "job_0000.json"
    saved = json.loads(checkpoint.read_text())
    request_file = tmp_path / saved["request_file"]
    requests = [json.loads(line) for line in request_file.read_text().splitlines()]
    requests[0]["messages"][0]["content"] += " changed"
    request_file.write_text("".join(json.dumps(call) + "\n" for call in requests), encoding="utf-8")
    saved["request_sha256"] = control.sha256(request_file.read_bytes())
    control.write_json(checkpoint, saved)
    with pytest.raises(ValueError, match="bound native prompt"):
        runner.read_completed(tmp_path, plan, plan["jobs"][0], manifest, prompts)


@pytest.mark.framework
def test_existing_lock_prevents_execution_without_removing_it(mocked_run, tmp_path):
    plan, frame, manifest, prompts, calls, _ = mocked_run
    lock = tmp_path / ".run.lock"
    lock.write_text("existing process")
    with pytest.raises(FileExistsError):
        runner.execute(tmp_path, plan, frame, manifest, prompts)
    assert calls == []
    assert lock.read_text() == "existing process"


@pytest.mark.framework
def test_duplicate_native_row_prompts_are_rejected():
    plan = small_plan()
    frame = pd.DataFrame({"abstract_text": ["same", "same", "different"]}, index=plan["row_ids"])
    with pytest.raises(ValueError, match="Duplicate row prompts"):
        runner.preflight(plan, frame)


def records(values):
    return {row: {"answer": value, "raw_output": "Answer: False." if value is None else f"Answer: {value}"}
            for row, value in enumerate(values)}


def test_unused_map_invalidity_does_not_exclude_rejected_rows_or_unrelated_cells():
    cells = {"reference_filter": records([False, True]), "reference_map": records([None, True]),
             "paraphrase_filter": records([False, True]), "unrelated_map": records([None, None])}
    revision = small_plan()["revisions"]["filter_paraphrase"]
    report = runner.revision_report(cells, revision, [0, 1])
    assert report["unassessed_rows"] == []
    assert report["invalid_responses_by_cell"]["reference_map"] == [0]
    assert "unrelated_map" not in report["required_cells"]
    assert report["ledger_verdict"]["status"] == "satisfied"
    assert report["views"]["full_ledger"]["n_input"] == 2


def test_required_invalidity_and_known_membership_exceptions_survive_partial_cohort():
    cells = {"reference_filter": records([False, True]), "reference_map": records([True, True]),
             "narrow_filter": records([True, True])}
    cells["reference_map"][0] = {"answer": None, "raw_output": "invalid"}
    revision = small_plan()["revisions"]["filter_narrowing"]
    report = runner.revision_report(cells, revision, [0, 1])
    assert report["unassessed_rows"] == [0]
    assert report["ledger_verdict"]["checks"]["complete_input"]["status"] == "not_evaluable"
    assert report["ledger_verdict"]["checks"]["required_response_format"]["row_ids"] == [0]
    assert report["ledger_verdict"]["checks"]["known_filter_membership"]["row_ids"] == [0]
    assert report["ledger_verdict"]["status"] == "violated"
    assert report["baseline_agrees"]


def test_dry_run_writes_no_run_and_execute_needs_explicit_flags(tmp_path, monkeypatch, capsys):
    plan = small_plan()
    plan.update(estimate={}, model_availability="not checked")
    folder = runner.DEFAULT_RUN.parent / "constructed-dry-run"
    monkeypatch.setattr(preparation, "verify", lambda path: plan)
    monkeypatch.setattr(runner, "load_frame", lambda plan: pd.DataFrame())
    monkeypatch.setattr(runner, "preflight", lambda plan, frame: ({"protocol_sha256": "test"}, {}))

    def forbidden(*args):
        pytest.fail("Dry run attempted execution")

    monkeypatch.setattr(runner, "execute", forbidden)
    runner.main(["--dry-run", "--output-dir", str(folder)])
    assert not folder.exists()
    assert "no provider calls" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        runner.main(["--execute"])
