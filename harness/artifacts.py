"""Validate saved pipeline artifacts without modifying historical files."""
import json
import hashlib
import os
import tempfile
import warnings
from pathlib import Path

import pandas as pd


def atomic_write(path, content):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def experiment_identity(frame, configuration):
    if not frame.index.is_unique:
        raise ValueError("Duplicate input row IDs")
    return {
        "input_sha256": hashlib.sha256(frame.to_json(orient="split", double_precision=15).encode()).hexdigest(),
        "configuration_sha256": hashlib.sha256(json.dumps(configuration, sort_keys=True).encode()).hexdigest(),
    }


def validate_identity(meta, expected, *, allow_legacy=False):
    identity = meta.get("experiment_identity")
    if identity is None:
        if not allow_legacy:
            raise ValueError("Historical artifact lacks experiment identity; use explicit legacy inspection or a new run ID")
        warnings.warn("Historical artifact: structural checks only; input/configuration identity unverified", UserWarning)
        return "historical_structural_only"
    if expected is None or identity != expected:
        raise ValueError("Experiment identity mismatch or missing expected identity")
    if meta.get("completion_status") != "complete":
        raise ValueError("Incomplete artifact metadata")
    return "identity_verified"


def load_completed_pipeline(results_dir, run_id, expected_ids, map_col, *,
                            expected_identity=None, allow_legacy=False):
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
    status = validate_identity(meta, expected_identity, allow_legacy=allow_legacy)
    if status == "identity_verified" and set(meta.get("artifact_sha256", {})) != {"filter", "map"}:
        raise ValueError("Missing artifact hashes")
    mapped.attrs["validation_status"] = status
    return mapped
