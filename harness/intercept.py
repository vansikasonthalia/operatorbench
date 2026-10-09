"""
OperatorBench -- LiteLLM Call Interceptor

Monkey-patches litellm.completion (and batch_completion) to capture the
exact prompts LOTUS constructs and the raw LLM responses.  This is
essential for reproducibility: reviewers need to see the real prompts,
not just the user-facing predicate strings.

Usage:
    from harness.intercept import LLMInterceptor

    interceptor = LLMInterceptor(log_dir="logs/llm_calls")
    interceptor.install()       # patches litellm globally
    # ... run LOTUS pipelines ...
    interceptor.uninstall()     # restores original functions

    # Inspect captured calls
    for call in interceptor.calls:
        print(call["messages"], call["response_text"])
"""

import json
import datetime
import hashlib
import threading
from pathlib import Path
from typing import Optional


class LLMInterceptor:
    """Transparent interceptor for litellm.completion calls."""

    def __init__(self, log_dir: str | Path = "logs/llm_calls", tag: str = ""):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.tag = tag  # e.g. run_id, to group calls
        self.calls: list[dict] = []
        self._installed = False
        self._orig_completion = None
        self._orig_batch = None
        self._lock = threading.RLock()
        self._active_batches = 0
        self._batch_seen = {}

    def install(self):
        """Patch litellm.completion to intercept every call."""
        if self._installed:
            return
        import litellm
        self._orig_completion = litellm.completion
        self._orig_batch = getattr(litellm, "batch_completion", None)

        interceptor = self  # closure reference

        def patched_completion(*args, **kwargs):
            result = interceptor._orig_completion(*args, **kwargs)
            interceptor._record(args, kwargs, result)
            with interceptor._lock:
                if interceptor._active_batches:
                    interceptor._batch_seen[id(result)] = result
            return result

        litellm.completion = patched_completion

        # LOTUS uses batch_completion internally
        if self._orig_batch is not None:
            orig_batch = self._orig_batch

            def patched_batch(*args, **kwargs):
                with interceptor._lock:
                    interceptor._active_batches += 1
                try:
                    results = orig_batch(*args, **kwargs)
                    model = kwargs.get("model", args[0] if args else "unknown")
                    prompts = kwargs.get("messages", args[1] if len(args) > 1 else [])
                    if isinstance(results, list):
                        if len(prompts) != len(results):
                            print("[intercept] Warning: batch prompt/response cardinality mismatch; batch pairing unavailable")
                            return results
                        for prompt, response in zip(prompts, results):
                            with interceptor._lock:
                                seen = id(response) in interceptor._batch_seen
                            if not seen:
                                interceptor._record_response(
                                    {"model": model, "messages": prompt}, response)
                    return results
                finally:
                    with interceptor._lock:
                        interceptor._active_batches -= 1
                        if not interceptor._active_batches:
                            interceptor._batch_seen.clear()

            litellm.batch_completion = patched_batch

        self._installed = True

    def uninstall(self):
        """Restore original litellm functions."""
        if not self._installed:
            return
        import litellm
        litellm.completion = self._orig_completion
        if self._orig_batch is not None:
            litellm.batch_completion = self._orig_batch
        self._installed = False

    def _record(self, args, kwargs, response):
        """Record a single completion call."""
        try:
            model = kwargs.get("model") or (args[0] if args else "unknown")
            messages = kwargs.get("messages") or (args[1] if len(args) > 1 else [])

            # Extract response text
            response_text = ""
            if hasattr(response, "choices") and response.choices:
                choice = response.choices[0]
                if hasattr(choice, "message") and hasattr(choice.message, "content"):
                    response_text = choice.message.content or ""

            # Build prompt string for hashing
            prompt_str = json.dumps(messages, default=str)
            prompt_hash = hashlib.sha256(prompt_str.encode()).hexdigest()[:16]

            record = {
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "tag": self.tag,
                "model": str(model),
                "messages": messages,
                "prompt_hash": prompt_hash,
                "response_text": response_text,
                "usage": _extract_usage(response),
            }
            self.calls.append(record)
            self._write(record)
        except Exception as e:
            # Never let logging break the pipeline
            print(f"  [intercept] Warning: failed to record call: {e}")

    def _record_response(self, kwargs, response):
        """Record from batch_completion (kwargs shared across batch)."""
        try:
            model = kwargs.get("model", "unknown")
            messages = kwargs.get("messages", [])

            response_text = ""
            if hasattr(response, "choices") and response.choices:
                choice = response.choices[0]
                if hasattr(choice, "message") and hasattr(choice.message, "content"):
                    response_text = choice.message.content or ""
            elif isinstance(response, Exception):
                response_text = f"ERROR: {response}"

            prompt_str = json.dumps(messages, default=str)
            prompt_hash = hashlib.sha256(prompt_str.encode()).hexdigest()[:16]

            record = {
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "tag": self.tag,
                "model": str(model),
                "messages": messages if isinstance(messages, list) else [],
                "prompt_hash": prompt_hash,
                "response_text": response_text,
                "usage": _extract_usage(response),
            }
            self.calls.append(record)
            self._write(record)
        except Exception as e:
            print(f"  [intercept] Warning: failed to record batch call: {e}")

    def _write(self, record: dict):
        """Append to JSONL file."""
        filename = f"llm_calls_{self.tag}.jsonl" if self.tag else "llm_calls.jsonl"
        log_file = self.log_dir / filename
        with self._lock:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")

    def summary(self) -> dict:
        """Quick summary of intercepted calls."""
        total = len(self.calls)
        total_prompt_tokens = sum(c.get("usage", {}).get("prompt_tokens", 0) for c in self.calls)
        total_completion_tokens = sum(c.get("usage", {}).get("completion_tokens", 0) for c in self.calls)
        return {
            "total_calls": total,
            "total_prompt_tokens": total_prompt_tokens,
            "total_completion_tokens": total_completion_tokens,
            "unique_prompts": len(set(c.get("prompt_hash", "") for c in self.calls)),
        }

    def get_prompt_templates(self) -> list[str]:
        """Extract unique prompt structures (for paper appendix)."""
        seen = set()
        templates = []
        for call in self.calls:
            msgs = call.get("messages", [])
            # Use system + first user message structure as template key
            key_parts = []
            for m in msgs:
                role = m.get("role", "")
                content = str(m.get("content", ""))[:100]
                key_parts.append(f"{role}: {content}")
            key = " | ".join(key_parts)
            if key not in seen:
                seen.add(key)
                templates.append(json.dumps(msgs, indent=2, default=str))
        return templates


def _extract_usage(response) -> dict:
    """Safely extract token usage from a litellm response."""
    try:
        if hasattr(response, "usage") and response.usage:
            return {
                "prompt_tokens": getattr(response.usage, "prompt_tokens", 0),
                "completion_tokens": getattr(response.usage, "completion_tokens", 0),
                "total_tokens": getattr(response.usage, "total_tokens", 0),
            }
    except Exception:
        pass
    return {}
