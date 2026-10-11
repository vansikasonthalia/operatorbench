from types import SimpleNamespace

import pandas as pd
import pytest

pytestmark = pytest.mark.framework


@pytest.mark.parametrize("cache_enabled", [False, True])
def test_runner_binds_model_seed_and_cache_and_restores_on_failure(tmp_path, monkeypatch, cache_enabled):
    from harness import runner as module

    monkeypatch.setattr(module, "LM", lambda model, **kw: SimpleNamespace(model=model, kwargs=kw))
    a = module.PipelineRunner(model="a", seed=1, cache_enabled=cache_enabled,
                              cache_path=tmp_path / "cache.db", log_dir=tmp_path / "a")
    b = module.PipelineRunner(model="b", seed=2, cache_enabled=False, log_dir=tmp_path / "b")
    previous = module.lotus.settings.lm, module.lotus.settings.enable_cache
    def observe():
        lm = module.lotus.settings.lm
        return lm.model, lm.kwargs["seed"], module.lotus.settings.enable_cache
    try:
        assert a._run_with_retry(observe, "a") == ("a", 1, cache_enabled)
        assert b._run_with_retry(observe, "b") == ("b", 2, False)
        assert a._run_with_retry(observe, "a") == ("a", 1, cache_enabled)
        with pytest.raises(ValueError, match="deliberate"):
            a._run_with_retry(lambda: (_ for _ in ()).throw(ValueError("deliberate")), "a")
        assert (module.lotus.settings.lm, module.lotus.settings.enable_cache) == previous
    finally:
        a.cache.close()
        b.cache.close()


def test_empty_map_skips_provider(monkeypatch):
    from harness.runner import PipelineRunner
    runner = PipelineRunner.__new__(PipelineRunner)
    frame = pd.DataFrame({"text": []}, index=pd.Index([], name="row_id"))
    result = runner._batched_map(frame, "classify", "label")
    assert result.empty and list(result.columns) == ["text", "label"]
