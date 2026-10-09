from .logger import CallLogger
from .cache import ResponseCache
from .normalize import normalize_label
from .metrics import (
    contradiction_rate,
    schema_violation_rate,
    consistency_across_runs,
    context_sensitivity_drift,
    wilson_ci,
    format_results_table,
)

__all__ = [
    "CallLogger", "ResponseCache", "PipelineRunner", "normalize_label",
    "LLMInterceptor",
    "contradiction_rate", "schema_violation_rate",
    "consistency_across_runs", "context_sensitivity_drift",
    "wilson_ci", "format_results_table",
]


def __getattr__(name):
    if name == "PipelineRunner":
        from .runner import PipelineRunner
        return PipelineRunner
    if name == "LLMInterceptor":
        from .intercept import LLMInterceptor
        return LLMInterceptor
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
