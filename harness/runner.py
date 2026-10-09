"""
OperatorBench — Pipeline Runner

Wraps LOTUS sem_filter and sem_map calls with:
  - Call logging (every LLM invocation)
  - Response caching (development mode)
  - Run ID tracking
  - Gleaning/retry OFF (operator-level isolation)

Usage:
    runner = PipelineRunner(
        model="gpt-4o-mini",
        backend="hosted",
        seed=42,  # configure the seed when the LOTUS LM is created
        cache_enabled=True,  # False for real experimental runs
    )
    results = runner.run_filter_map(
        df=reviews_df,
        domain="reviews",
        filter_predicate="Is this review negative?",
        map_instruction="Classify sentiment: positive / negative / neutral",
        map_output_col="mapped_sentiment",
        run_id="rq3_reviews_seed42_run1",
    )
"""

import hashlib
import json
import os
import re
import tempfile
import time
import uuid
import pandas as pd
from pathlib import Path
from typing import Optional

from .logger import CallLogger
from .cache import ResponseCache

# Max rows per LOTUS call to stay under TPM rate limits
# arXiv abstracts are long (~300 tokens each), so keep batches small
BATCH_SIZE = 10
BATCH_DELAY_SECONDS = 12  # pause between batches (TPM window is per minute)
MAX_RETRIES = 3           # retry on rate-limit errors

# Transient failures worth retrying. Rate limits were the original case, but an
# overnight run on 10 Aug 2026 died on a bare "Connection error" -- a dropped
# network link, retried zero times because the check was 429-only, losing ~8h.
# Anything not matched here still raises immediately: a retry loop must never
# swallow a real bug.
_TRANSIENT_SUBSTRINGS = (
    "429", "rate limit", "ratelimit", "rate_limit",
    "connection error", "connection reset", "connection aborted",
    "timeout", "timed out", "temporarily unavailable",
    "502", "503", "504", "bad gateway", "service unavailable",
)
_TRANSIENT_TYPE_NAMES = (
    "RateLimitError", "APIConnectionError", "APITimeoutError",
    "Timeout", "ServiceUnavailableError", "InternalServerError",
    "ConnectionError", "ReadTimeout",
)


def _is_transient(exc: Exception) -> bool:
    """True if the failure is worth retrying rather than surfacing."""
    if type(exc).__name__ in _TRANSIENT_TYPE_NAMES:
        return True
    msg = str(exc).lower()
    return any(s in msg for s in _TRANSIENT_SUBSTRINGS)


# ---------------------------------------------------------------------------
# Label normalisation
# ---------------------------------------------------------------------------
#
# The implementation lives in harness/normalize.py, which imports nothing but
# `re`. It was moved out of this module because this module imports LOTUS at
# import time, and the determinism tests spawn one interpreter per hash seed --
# paying for a full LOTUS import to normalise a single string made the suite
# take 4.5 minutes. Re-exported here so every existing import path still works.
from harness.normalize import normalize_label  # noqa: F401  (re-export)

# LOTUS imports — will fail if lotus-ai not installed
try:
    import lotus
    from lotus.models import LM
    LOTUS_AVAILABLE = True
except ImportError:
    LOTUS_AVAILABLE = False
    print("WARNING: lotus-ai not installed. Runner will not work.")


class PipelineRunner:
    """Runs sem_filter -> sem_map pipelines with full instrumentation."""

    def __init__(
        self,
        model: str = "gpt-4o-mini",
        backend: str = "hosted",
        seed: Optional[int] = None,
        log_dir: str | Path = "logs",
        cache_path: str | Path = "cache.db",
        cache_enabled: bool = True,
        max_text_chars: int = 4000,
        batch_size: int | None = None,
        batch_delay_seconds: float | None = None,
        patient: bool = False,
    ):
        self.model = model
        self.backend = backend
        self.seed = seed
        self.max_text_chars = max_text_chars  # truncate long texts before sending to LOTUS
        # Batch tuning is per-instance so the local backend can drop the
        # rate-limit delay entirely (Ollama has no TPM ceiling). Defaults keep
        # the hosted behaviour byte-identical to Week 1.
        self.batch_size = BATCH_SIZE if batch_size is None else batch_size
        self.batch_delay_seconds = (
            BATCH_DELAY_SECONDS if batch_delay_seconds is None else batch_delay_seconds
        )
        # Patient mode: survive a daily-cap reset instead of dying in 90 seconds.
        self.patient = patient
        self.retry_attempts = 40 if patient else MAX_RETRIES
        self.retry_max_wait = 900  # 15 min ceiling on exponential backoff
        self.logger = CallLogger(log_dir=log_dir)
        self.cache = ResponseCache(cache_path=cache_path, enabled=cache_enabled)

        if LOTUS_AVAILABLE:
            # Configure LOTUS with LiteLLM backend.
            #
            # Verified in lotus 1.2.4: LM.__init__ stores **kwargs into
            # self.kwargs (lm.py:161) and forwards them to
            # litellm.batch_completion (lm.py:297), so `seed` reaches the
            # provider. NOTE: batch_completion is called with drop_params=True,
            # which means LiteLLM will SILENTLY DISCARD `seed` if the provider
            # adapter does not support it. Passing it is therefore necessary but
            # not sufficient -- determinism must be verified empirically before
            # it is claimed. See docs/local_backend_setup.md section 4.
            lm_kwargs: dict = {}
            if self.seed is not None:
                lm_kwargs["seed"] = self.seed
            self.lm = LM(model=model, **lm_kwargs)
            lotus.settings.configure(lm=self.lm)
            # Kept for run metadata. We record what was actually SENT rather than
            # what the project believes the defaults to be -- temperature in
            # particular is never set by us, so it is the pinned LOTUS source's
            # declared default. Do not start passing
            # temperature here to make the metadata tidier: that would change the
            # experimental conditions and invalidate comparison with the frozen
            # runs (D-W2-03).
            self._lm_kwargs_sent = dict(lm_kwargs)
        else:
            self._lm_kwargs_sent = {}

    def run_filter_map(
        self,
        df: pd.DataFrame,
        domain: str,
        filter_predicate: str,
        map_instruction: str,
        map_output_col: str = "mapped_label",
        valid_labels: set[str] | None = None,
        seed: Optional[int] = None,
        run_id: Optional[str] = None,
    ) -> dict:
        """
        Execute filter -> map pipeline with operator-level logging.

        Returns dict with:
          - filter_results: DataFrame after sem_filter
          - map_results: DataFrame after sem_map (labels normalised)
          - filter_positive_count: int
          - total_count: int
          - run_id: str
        """
        if not LOTUS_AVAILABLE:
            raise RuntimeError("lotus-ai not installed")

        run_id = run_id or f"{domain}_{uuid.uuid4().hex[:8]}"
        _seed = self._effective_seed(seed)

        print(f"[{run_id}] Starting filter->map pipeline")
        print(f"  Model: {self.model}, Backend: {self.backend}, Seed: {_seed}")
        print(f"  Domain: {domain}, Rows: {len(df)}")

        # Truncate long texts to avoid context window errors
        df = self._truncate_texts(df)

        # Step 1: sem_filter (batched to avoid TPM rate limits)
        print(f"  Running sem_filter: '{filter_predicate}'...")
        filtered_df = self._batched_filter(df, filter_predicate)
        filter_positive_count = len(filtered_df)
        print(f"  Filter passed: {filter_positive_count}/{len(df)} rows")

        # Log filter decisions (positive rows)
        text_col = self._guess_text_col(df)
        for row_id in filtered_df.index:
            self.logger.log(
                run_id=run_id,
                domain=domain,
                operator="sem_filter",
                row_id=int(row_id),
                model=self.model,
                seed=_seed,
                prompt_text=filter_predicate,
                raw_output="True",
                backend=self.backend,
                extras={"input_text_preview": str(df.loc[row_id, text_col])[:200]}
                if text_col else None,
            )
        # Log filter-negative rows too
        rejected_ids = df.index.difference(filtered_df.index)
        for row_id in rejected_ids:
            self.logger.log(
                run_id=run_id,
                domain=domain,
                operator="sem_filter",
                row_id=int(row_id),
                model=self.model,
                seed=_seed,
                prompt_text=filter_predicate,
                raw_output="False",
                backend=self.backend,
                extras={"input_text_preview": str(df.loc[row_id, text_col])[:200]}
                if text_col else None,
            )

        # Step 2: sem_map on filter-positive rows only (batched)
        print(f"  Running sem_map: '{map_instruction}'...")
        mapped_df = self._batched_map(filtered_df, map_instruction, map_output_col)
        print(f"  Map complete: {len(mapped_df)} rows with '{map_output_col}'")

        # Normalise map labels and log each row
        raw_col = f"_raw_{map_output_col}"
        mapped_df[raw_col] = mapped_df[map_output_col]
        mapped_df[map_output_col] = mapped_df[map_output_col].apply(
            lambda v: normalize_label(str(v), valid_labels)
        )

        for row_id, row in mapped_df.iterrows():
            self.logger.log(
                run_id=run_id,
                domain=domain,
                operator="sem_map",
                row_id=int(row_id),
                model=self.model,
                seed=_seed,
                prompt_text=map_instruction,
                raw_output=str(row[raw_col]),
                backend=self.backend,
                extras={"normalised_label": str(row[map_output_col])},
            )

        results = {
            "run_id": run_id,
            "domain": domain,
            "model": self.model,
            "backend": self.backend,
            "seed": _seed,
            "filter_predicate": filter_predicate,
            "map_instruction": map_instruction,
            "total_rows": len(df),
            "filter_positive_count": filter_positive_count,
            "filter_results": filtered_df,
            "map_results": mapped_df,
        }

        # Save intermediate results
        self._save_run_results(results)

        return results

    def _observed_temperature(self):
        """
        The temperature actually in effect, or None if we cannot observe it.

        This project never sets temperature. The pinned LOTUS reproduction
        source declares a default of 0.0; this method reads a value only when it
        is observable on the configured LM object. That still does not establish
        what the hosted provider applied to a historical call.
        """
        if "temperature" in getattr(self, "_lm_kwargs_sent", {}):
            return self._lm_kwargs_sent["temperature"]
        lm = getattr(self, "lm", None)
        for attr in ("temperature",):
            if hasattr(lm, attr):
                return getattr(lm, attr)
        kwargs = getattr(lm, "kwargs", None)
        if isinstance(kwargs, dict) and "temperature" in kwargs:
            return kwargs["temperature"]
        return None

    def _effective_seed(self, requested_seed):
        """Return the runner's configured seed; reject metadata-only overrides."""
        if requested_seed is not None and requested_seed != self.seed:
            raise ValueError(
                "Seed is fixed when PipelineRunner is constructed because LOTUS "
                "configures its LM then. Construct a new runner with the desired "
                "seed; a per-run seed override would only change logged metadata."
            )
        return self.seed

    def run_filter_only(
        self,
        df: pd.DataFrame,
        domain: str,
        filter_predicate: str,
        seed: Optional[int] = None,
        run_id: Optional[str] = None,
    ) -> pd.DataFrame:
        """Run sem_filter alone (for RQ1/RQ2 isolation), with logging."""
        if not LOTUS_AVAILABLE:
            raise RuntimeError("lotus-ai not installed")

        run_id = run_id or f"{domain}_filter_{uuid.uuid4().hex[:8]}"
        _seed = self._effective_seed(seed)
        df = self._truncate_texts(df)
        print(f"[{run_id}] sem_filter: '{filter_predicate}' on {len(df)} rows")
        result = self._batched_filter(df, filter_predicate)
        print(f"  Passed: {len(result)}/{len(df)}")

        # Log every row
        for row_id in df.index:
            passed = row_id in result.index
            self.logger.log(
                run_id=run_id, domain=domain, operator="sem_filter",
                row_id=int(row_id), model=self.model, seed=_seed,
                prompt_text=filter_predicate,
                raw_output=str(passed), backend=self.backend,
            )
        return result

    def run_map_only(
        self,
        df: pd.DataFrame,
        domain: str,
        map_instruction: str,
        map_output_col: str = "mapped_label",
        valid_labels: set[str] | None = None,
        seed: Optional[int] = None,
        run_id: Optional[str] = None,
    ) -> pd.DataFrame:
        """Run sem_map alone (for RQ1/RQ2 isolation), with logging + normalisation."""
        if not LOTUS_AVAILABLE:
            raise RuntimeError("lotus-ai not installed")

        run_id = run_id or f"{domain}_map_{uuid.uuid4().hex[:8]}"
        _seed = self._effective_seed(seed)
        df = self._truncate_texts(df)
        print(f"[{run_id}] sem_map: '{map_instruction}' on {len(df)} rows")
        result = self._batched_map(df, map_instruction, map_output_col)
        print(f"  Complete: {len(result)} rows")

        # Normalise and log
        raw_col = f"_raw_{map_output_col}"
        result[raw_col] = result[map_output_col]
        result[map_output_col] = result[map_output_col].apply(
            lambda v: normalize_label(str(v), valid_labels)
        )
        for row_id, row in result.iterrows():
            self.logger.log(
                run_id=run_id, domain=domain, operator="sem_map",
                row_id=int(row_id), model=self.model, seed=_seed,
                prompt_text=map_instruction,
                raw_output=str(row[raw_col]), backend=self.backend,
                extras={"normalised_label": str(row[map_output_col])},
            )
        return result

    @staticmethod
    def _guess_text_col(df: pd.DataFrame) -> str | None:
        """Return the first text-like column name, or None."""
        for c in ("review_text", "abstract_text", "text"):
            if c in df.columns:
                return c
        return None

    def _truncate_texts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Truncate long text columns to max_text_chars.

        The ccdv/arxiv-classification dataset contains full papers (avg 51k chars),
        not just abstracts. Truncating to ~4000 chars captures the abstract section
        and is sufficient for semantic operator testing.
        """
        text_col = self._guess_text_col(df)
        if text_col is None or self.max_text_chars <= 0:
            return df
        max_len = df[text_col].str.len().max()
        if max_len <= self.max_text_chars:
            return df
        df = df.copy()
        n_truncated = (df[text_col].str.len() > self.max_text_chars).sum()
        df[text_col] = df[text_col].str[:self.max_text_chars]
        print(f"    Truncated {n_truncated} texts to {self.max_text_chars} chars")
        return df

    def _run_with_retry(self, fn, label: str):
        """
        Run fn() with retry on rate-limit errors.

        Default (patient=False): 3 attempts, 30s/60s/90s -- unchanged from Week 1.

        Patient mode exists because per-minute backoff cannot bridge a *daily*
        account limit. When the daily cap is hit, every call 429s for hours, the
        3-attempt ceiling is exhausted in 90 seconds, and an overnight run dies
        immediately having done nothing. Patient mode instead backs off
        exponentially to a 15-minute ceiling and keeps trying for hours, so a
        run launched before the reset simply sleeps through it and resumes.

        Non-rate-limit errors always raise immediately -- patience must never
        turn a real bug into an eight-hour hang.
        """
        attempts = self.retry_attempts
        for attempt in range(1, attempts + 1):
            try:
                return fn()
            except Exception as e:
                if not _is_transient(e) or attempt == attempts:
                    raise
                if self.patient:
                    # 30s, 60s, 2m, 4m, 8m, then capped at 15m
                    wait = min(30 * (2 ** (attempt - 1)), self.retry_max_wait)
                else:
                    wait = 30 * attempt  # 30s, 60s, 90s
                mins = wait / 60
                pretty = f"{wait:.0f}s" if wait < 120 else f"{mins:.0f}m"
                reason = str(e).strip().replace("\n", " ")[:70]
                print(f"    Transient failure on {label} ({reason}), waiting "
                      f"{pretty} (attempt {attempt}/{attempts})...", flush=True)
                time.sleep(wait)

    def _batched_filter(self, df: pd.DataFrame, predicate: str) -> pd.DataFrame:
        """Run sem_filter in chunks with retry on rate limits."""
        bs, delay = self.batch_size, self.batch_delay_seconds
        if len(df) <= bs:
            return self._run_with_retry(
                lambda: df.sem_filter(predicate), "filter"
            )

        chunks = [df.iloc[i:i + bs] for i in range(0, len(df), bs)]
        filtered_parts = []
        for idx, chunk in enumerate(chunks):
            print(f"    Batch {idx + 1}/{len(chunks)} ({len(chunk)} rows)...")
            result = self._run_with_retry(
                lambda c=chunk: c.sem_filter(predicate),
                f"filter batch {idx + 1}",
            )
            filtered_parts.append(result)
            if idx < len(chunks) - 1 and delay > 0:
                time.sleep(delay)
        return pd.concat(filtered_parts) if filtered_parts else pd.DataFrame()

    def _batched_map(self, df: pd.DataFrame, instruction: str, output_col: str) -> pd.DataFrame:
        """Run sem_map in chunks with retry on rate limits."""
        bs, delay = self.batch_size, self.batch_delay_seconds
        if len(df) <= bs:
            return self._run_with_retry(
                lambda: df.sem_map(instruction, suffix=output_col), "map"
            )

        chunks = [df.iloc[i:i + bs] for i in range(0, len(df), bs)]
        mapped_parts = []
        for idx, chunk in enumerate(chunks):
            print(f"    Batch {idx + 1}/{len(chunks)} ({len(chunk)} rows)...")
            result = self._run_with_retry(
                lambda c=chunk: c.sem_map(instruction, suffix=output_col),
                f"map batch {idx + 1}",
            )
            mapped_parts.append(result)
            if idx < len(chunks) - 1 and delay > 0:
                time.sleep(delay)
        return pd.concat(mapped_parts) if mapped_parts else pd.DataFrame()

    def _save_run_results(self, results: dict):
        """Save run results to results/ as CSV."""
        results_dir = Path("results")
        results_dir.mkdir(exist_ok=True)

        run_id = results["run_id"]

        def publish(path, content):
            fd, temporary = tempfile.mkstemp(dir=results_dir, suffix=".tmp")
            try:
                with os.fdopen(fd, "w", encoding="utf-8", newline="") as stream:
                    stream.write(content)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, path)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)

        if not results["filter_results"].index.is_unique or not results["map_results"].index.is_unique:
            raise ValueError("Duplicate row IDs in pipeline results")
        if set(results["filter_results"].index) != set(results["map_results"].index):
            raise ValueError("Filter/map row mismatch")

        csv_hashes = {}
        for kind in ("filter", "map"):
            content = results[f"{kind}_results"].to_csv(index=True, index_label="row_id")
            path = results_dir / f"{run_id}_{kind}.csv"
            publish(path, content)
            csv_hashes[kind] = hashlib.sha256(path.read_bytes()).hexdigest()

        # Publish metadata last so its hashes bind the completed CSVs.
        meta = {k: v for k, v in results.items()
                if k not in ("filter_results", "map_results")}
        meta["batch_size"] = self.batch_size
        meta["batch_delay_seconds"] = self.batch_delay_seconds
        meta["artifact_sha256"] = csv_hashes
        meta["completion_status"] = "complete"
        actual_prompts = {key: results[key] for key in ("filter_predicate", "map_instruction")}
        meta["actual_prompt_sha256"] = hashlib.sha256(
            json.dumps(actual_prompts, sort_keys=True).encode("utf-8")
        ).hexdigest()
        try:
            from harness.provenance import run_provenance
            meta["provenance"] = run_provenance(
                temperature=self._observed_temperature(),
                max_text_chars=self.max_text_chars,
                backend=self.backend,
                seed=self.seed,
                cache_enabled=getattr(self.cache, "enabled", None),
            )
        except Exception as exc:        # never let bookkeeping kill a paid run
            meta["provenance"] = {"error": f"provenance capture failed: {exc}"}
        publish(results_dir / f"{run_id}_meta.json", json.dumps(meta, indent=2, default=str))
        print(f"  Results saved to results/{run_id}_*.csv")
