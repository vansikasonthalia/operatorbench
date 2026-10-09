"""
OperatorBench — LLM Call Logger

Every LLM call that passes through the harness gets logged with:
  - model name
  - seed (if applicable)
  - timestamp (ISO 8601, UTC)
  - prompt hash (SHA-256, first 16 chars)
  - raw output (full text)
  - run_id, domain, operator, row_id for traceability

Logs are append-only JSONL files in logs/.
"""

import hashlib
import json
import datetime
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class CallRecord:
    """One logged LLM call."""
    run_id: str
    domain: str          # "reviews" or "abstracts"
    operator: str        # "sem_filter" or "sem_map"
    row_id: int
    model: str
    seed: Optional[int]
    timestamp: str       # ISO 8601 UTC
    prompt_hash: str     # SHA-256 prefix
    prompt_text: str     # full prompt (for debugging)
    raw_output: str      # full model response
    backend: str         # "local" or "hosted"
    extras: Optional[dict] = None


class CallLogger:
    """Append-only JSONL logger for LLM calls."""

    def __init__(self, log_dir: str | Path = "logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._records: list[CallRecord] = []

    def log(
        self,
        run_id: str,
        domain: str,
        operator: str,
        row_id: int,
        model: str,
        seed: Optional[int],
        prompt_text: str,
        raw_output: str,
        backend: str = "hosted",
        extras: Optional[dict] = None,
    ) -> CallRecord:
        prompt_hash = hashlib.sha256(prompt_text.encode()).hexdigest()[:16]
        record = CallRecord(
            run_id=run_id,
            domain=domain,
            operator=operator,
            row_id=row_id,
            model=model,
            seed=seed,
            timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            prompt_hash=prompt_hash,
            prompt_text=prompt_text,
            raw_output=raw_output,
            backend=backend,
            extras=extras,
        )
        self._records.append(record)
        self._append_to_file(record)
        return record

    def _append_to_file(self, record: CallRecord):
        log_file = self.log_dir / f"{record.run_id}.jsonl"
        with open(log_file, "a") as f:
            f.write(json.dumps(asdict(record), default=str) + "\n")

    @property
    def records(self) -> list[CallRecord]:
        return list(self._records)

    def summary(self) -> dict:
        return {
            "total_calls": len(self._records),
            "by_operator": _count_by(self._records, "operator"),
            "by_domain": _count_by(self._records, "domain"),
            "by_model": _count_by(self._records, "model"),
        }


def _count_by(records: list[CallRecord], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for r in records:
        key = getattr(r, field)
        counts[key] = counts.get(key, 0) + 1
    return counts


def load_run_log(log_dir: str | Path, run_id: str) -> list[dict]:
    """Load a completed run's log for analysis."""
    log_file = Path(log_dir) / f"{run_id}.jsonl"
    records = []
    with open(log_file) as f:
        for line in f:
            records.append(json.loads(line))
    return records
