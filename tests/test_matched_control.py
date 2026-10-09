from collections import Counter
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from scripts import run_matched_control as control


def test_schedule_covers_each_cell_and_interleaves_same_rows():
    jobs = control.schedule(list(range(23)), 4, 5, 42)
    assert jobs == control.schedule(list(range(23)), 4, 5, 42)
    assert jobs != control.schedule(list(range(23)), 4, 5, 43)
    for rep in range(1, 5):
        for cell in control.CELLS:
            rows = [row for job in jobs if job["replicate"] == rep and job["cell"] == cell
                    for row in job["row_ids"]]
            assert sorted(rows) == list(range(23))
    positions = {cell: Counter() for cell in control.CELLS}
    for start in range(0, len(jobs), 4):
        group = jobs[start:start + 4]
        assert len({job["cell"] for job in group}) == 4
        assert all(job["row_ids"] == group[0]["row_ids"] for job in group)
        for position, job in enumerate(group):
            positions[job["cell"]][position] += 1
    assert all(len(counts) == 4 and len(set(counts.values())) == 1 for counts in positions.values())


@pytest.mark.parametrize("raw, expected", [
    ("Answer: True", True), (" answer: false \n", False),
    ("True", None), ("Answer: True or False", None),
    ("Answer: True because it trains a model", None), (None, None),
])
def test_strict_answers_keep_format_errors_out_of_decision_counts(raw, expected):
    assert control.parse_answer(raw) is expected


def test_comparison_excludes_invalid_and_preserves_direction():
    result = control.compare({1: True, 2: False, 3: None},
                             {1: False, 2: True, 3: True})
    assert result["n_valid_pairs"] == 2
    assert result["n_excluded_invalid"] == 1
    assert result["left_true_only"] == result["right_true_only"] == 1
    assert result["right_minus_left_pp"] == 0
    assert result["mcnemar_exact_p"] == 1
    assert control.compare({1: None}, {1: True})["difference_rate"] is None


def save_fake_jobs(folder, plan):
    for job in plan["jobs"]:
        # Stable filter/map difference on one row; another row always violates format.
        records = []
        for row in job["row_ids"]:
            answer = None if row == 2 else row == 0 and job["cell"].endswith("filter")
            raw = "verbose" if answer is None else f"Answer: {answer}"
            records.append({"row_id": row, "answer": answer, "raw_output": raw})
        requests = folder / f"requests_{job['job']}.jsonl"
        requests.write_text('{}\n', encoding="utf-8")
        control.write_json(folder / f"job_{job['job']:04d}.json", {
            "plan_sha256": control.sha256(control.canonical(plan)), "job": job,
            "records": records, "records_sha256": control.sha256(control.canonical(records)),
            "request_file": requests.name, "request_sha256": control.sha256(requests.read_bytes()),
        })


def fake_plan():
    return {"row_ids": [0, 1, 2], "replicates": 2,
            "jobs": control.schedule([0, 1, 2], 2, 2, 42), "analysis": {}}


def test_aggregation_rejects_incomplete_or_changed_checkpoints(tmp_path):
    plan = fake_plan()
    with pytest.raises(ValueError, match="incomplete"):
        control.analyse(tmp_path, plan)
    save_fake_jobs(tmp_path, plan)
    result = control.analyse(tmp_path, plan)
    assert result["replicates"][0]["contrasts"]["operator_about"]["left_true_only"] == 1
    assert result["repeatability"]["about_filter"]["n_valid_all_replicates"] == 2
    assert result["repeatability"]["about_filter"]["flip_rate"] == 0
    (tmp_path / "requests_0.jsonl").write_text('changed\n', encoding="utf-8")
    with pytest.raises(ValueError, match="archive changed"):
        control.analyse(tmp_path, plan)


def test_aggregation_checks_result_identity_and_invalid_answers(tmp_path):
    plan = fake_plan()
    save_fake_jobs(tmp_path, plan)
    path = tmp_path / "job_0000.json"
    saved = json.loads(path.read_text(encoding="utf-8"))
    saved["records"][0]["answer"] = not saved["records"][0]["answer"]
    control.write_json(path, saved)
    with pytest.raises(ValueError, match="checkpoint changed"):
        control.read_completed(tmp_path, plan, plan["jobs"][0])
    saved["records_sha256"] = control.sha256(control.canonical(saved["records"]))
    control.write_json(path, saved)
    with pytest.raises(ValueError, match="raw response"):
        control.read_completed(tmp_path, plan, plan["jobs"][0])


def test_holm_adjustment_is_monotonic_and_bounded():
    assert control.holm({"a": 0.01, "b": 0.04, "c": 0.8}) == {"a": 0.03, "b": 0.08, "c": 0.8}


def test_invalid_contrasts_remain_in_planned_testing_family(tmp_path, monkeypatch):
    plan = fake_plan()
    save_fake_jobs(tmp_path, plan)
    observed = []
    actual_holm = control.holm
    def capture(values):
        observed.append(values)
        return actual_holm(values)
    monkeypatch.setattr(control, "holm", capture)
    monkeypatch.setattr(control, "compare", lambda left, right: {
        "n_valid_pairs": 0, "mcnemar_exact_p": None,
    })
    result = control.analyse(tmp_path, plan)
    assert len(observed[0]) == 8
    assert all(value == 1 for value in observed[0].values())
    assert all(contrast["holm_p"] is None for report in result["replicates"]
               for contrast in report["contrasts"].values())


def test_dry_run_does_not_execute_or_write(tmp_path, monkeypatch, capsys):
    def forbidden(*args, **kwargs):
        pytest.fail("Dry run attempted paid execution")
    monkeypatch.setattr(control, "execute", forbidden)
    folder = tmp_path / "experiment"
    control.main(["--dry-run", "--limit", "3", "--replicates", "2",
                  "--output-dir", str(folder)])
    assert not folder.exists()
    assert '"row_calls": 24' in capsys.readouterr().out
    with pytest.raises(SystemExit):
        control.main(["--execute", "--limit", "3"])


def test_native_operators_share_question_and_preserve_rejected_raw_outputs(monkeypatch, tmp_path):
    import lotus
    from lotus.models import LM
    from lotus.types import LMOutput

    monkeypatch.chdir(tmp_path)
    prompts = []
    def offline_call(self, messages, **kwargs):
        prompts.append(messages)
        return LMOutput(outputs=["Answer: True", "Answer: False", "invalid"])
    monkeypatch.setattr(LM, "__call__", offline_call)
    frame = pd.DataFrame({"abstract_text": ["a learning paper", "a systems paper", "a theory paper"]},
                         index=[7, 3, 9])
    lm = LM(model="gpt-4o-mini-2024-07-18", max_tokens=16)
    with lotus.settings.context(lm=lm, enable_cache=False):
        results = {cell: control.evaluate_cell(frame, cell) for cell in control.CELLS}
    for records in results.values():
        assert [record["row_id"] for record in records] == [7, 3, 9]
        assert [record["answer"] for record in records] == [True, False, None]
        assert [record["raw_output"] for record in records] == ["Answer: True", "Answer: False", "invalid"]
    assert control.instruction("about_filter") == control.instruction("about_map")
    assert control.instruction("primary_filter") == control.instruction("primary_map")
    assert len(prompts) == 4
    for cell, messages in zip(control.CELLS, prompts):
        question = control.QUESTIONS[cell.rsplit("_", 1)[0]]
        assert all(question in str(message) and control.FORMAT in str(message) for message in messages)


def test_execution_archives_and_resumes_without_repeating_completed_calls(tmp_path, monkeypatch):
    import litellm
    import lotus.models

    plan = fake_plan()
    plan.update({"model": "gpt-4o-mini-2024-07-18", "temperature_requested": 0.0,
                 "max_output_tokens": 16, "block_size": 2,
                 "max_text_chars": 4000, "batch_delay_seconds": 0})
    frame = pd.DataFrame({"abstract_text": ["paper a", "paper b", "paper c"]})
    calls = []
    def completion(*args, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="Answer: False"))])
    def evaluate(block, cell):
        for row in block.index:
            litellm.completion(model=plan["model"], messages=[{"role": "user", "content": str(row)}])
        return [{"row_id": int(row), "raw_output": "Answer: False", "answer": False,
                 "operator_output": "False"} for row in block.index]
    monkeypatch.setenv("OPENAI_API_KEY", "offline-test")
    monkeypatch.setattr(litellm, "completion", completion)
    monkeypatch.setattr(lotus.models, "LM", lambda **kwargs: SimpleNamespace(kwargs=kwargs))
    monkeypatch.setattr(control, "evaluate_cell", evaluate)
    monkeypatch.setattr(control.time, "sleep", lambda _: None)
    folder = tmp_path / "experiment"
    control.execute(folder, plan, frame)
    assert len(calls) == 24
    assert all(control.read_completed(folder, plan, job) is not None for job in plan["jobs"])
    control.execute(folder, plan, frame)
    assert len(calls) == 24
    assert all(report["contrasts"]["operator_about"]["difference_rate"] == 0
               for report in control.analyse(folder, plan)["replicates"])
    changed = {**plan, "temperature_requested": 0.5}
    with pytest.raises(ValueError, match="plan differs"):
        control.execute(folder, changed, frame)
