"""
OperatorBench — Response Cache

Caches LLM responses keyed on (model, seed, prompt_hash).
Avoids redundant API calls during development and reruns.
Disabled during actual experimental runs (set enabled=False).

Storage: SQLite for simplicity and portability.
"""

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Optional


class ResponseCache:
    """SQLite-backed LLM response cache."""

    def __init__(self, cache_path: str | Path = "cache.db", enabled: bool = True):
        self.enabled = enabled
        self.cache_path = Path(cache_path)
        self._conn: Optional[sqlite3.Connection] = None
        if enabled:
            self._init_db()

    def _init_db(self):
        self._conn = sqlite3.connect(str(self.cache_path))
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS response_cache (
                cache_key TEXT PRIMARY KEY,
                model TEXT NOT NULL,
                seed INTEGER,
                prompt_hash TEXT NOT NULL,
                prompt_text TEXT NOT NULL,
                response TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self._conn.commit()

    @staticmethod
    def make_key(model: str, seed: Optional[int], prompt_text: str) -> str:
        prompt_hash = hashlib.sha256(prompt_text.encode()).hexdigest()[:16]
        seed_str = str(seed) if seed is not None else "none"
        return f"{model}::{seed_str}::{prompt_hash}"

    def get(self, model: str, seed: Optional[int], prompt_text: str) -> Optional[str]:
        """Return cached response or None."""
        if not self.enabled or self._conn is None:
            return None
        key = self.make_key(model, seed, prompt_text)
        row = self._conn.execute(
            "SELECT response FROM response_cache WHERE cache_key = ?", (key,)
        ).fetchone()
        return row[0] if row else None

    def put(
        self,
        model: str,
        seed: Optional[int],
        prompt_text: str,
        response: str,
    ):
        """Store a response in cache."""
        if not self.enabled or self._conn is None:
            return
        key = self.make_key(model, seed, prompt_text)
        prompt_hash = hashlib.sha256(prompt_text.encode()).hexdigest()[:16]
        self._conn.execute(
            """INSERT OR REPLACE INTO response_cache
               (cache_key, model, seed, prompt_hash, prompt_text, response)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (key, model, seed, prompt_hash, prompt_text, response),
        )
        self._conn.commit()

    def stats(self) -> dict:
        if not self.enabled or self._conn is None:
            return {"enabled": False}
        row = self._conn.execute("SELECT COUNT(*) FROM response_cache").fetchone()
        return {"enabled": True, "entries": row[0]}

    def close(self):
        if self._conn:
            self._conn.close()
