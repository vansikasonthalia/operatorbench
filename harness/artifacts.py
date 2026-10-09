"""Validate saved pipeline artifacts without modifying historical files."""
import json
import hashlib
from pathlib import Path

import pandas as pd


def load_completed_pipeline(results_dir, run_id, expected_ids, map_col):
    root = Path(results_dir)
    paths = {kind: root / f"{run_id}_{kind}.{ext}"
             for kind, ext in (("filter", "csv"), ("map", "csv"), ("meta", "json"))}
    if not all(path.exists() for path in paths.values()):
        raise ValueError(f"Incomplete artifacts for {run_id}")
    filtered = pd.read_csv(paths["filter"], index_col="row_id")
    mapped = pd.read_csv(paths["map"], index_col="row_id")
    meta = json.loads(paths["meta"].read_text(encoding="utf-8"))
    for kind, expected_hash in meta.get("artifact_sha256", {}).items():
        if kind not in ("filter", "map") or hashlib.sha256(paths[kind].read_bytes()).hexdigest() != expected_hash:
            raise ValueError(f"Artifact hash mismatch in {run_id}")
    if not filtered.index.is_unique or not mapped.index.is_unique:
        raise ValueError(f"Duplicate row IDs in {run_id}")
    if set(filtered.index) != set(mapped.index):
        raise ValueError(f"Filter/map row mismatch in {run_id}")
    if not set(mapped.index) <= set(expected_ids) or map_col not in mapped:
        raise ValueError(f"Unexpected rows or missing map column in {run_id}")
    # Historical manifests used several names. They must still establish
    # both input cardinality and admitted cardinality to be reusable.
    total = meta.get("total_rows", meta.get("n_rows", meta.get("n_eligible_rows")))
    admitted = meta.get("filter_positive_count")
    if total != len(expected_ids) or admitted != len(filtered):
        raise ValueError(f"Missing or inconsistent completion counts in {run_id}")
    return mapped
