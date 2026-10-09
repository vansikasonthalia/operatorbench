"""Read-only audit of frozen results/logs; writes only a separate audit directory.

Checks structural consistency and selected numerical claims. This cannot prove
that absent historical request parameters were applied, or validate proxy labels.
"""
import argparse
import collections
import csv
import hashlib
import importlib.util
import io
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
csv.field_size_limit(10_000_000)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_csv(path, source=False):
    with path.open(encoding="utf-8-sig", newline="") as stream:
        text = stream.read()
    # Python 3.10's CSV parser rejects NULs present in the frozen text fields.
    marker = "\ufdd0"
    if marker in text:
        raise ValueError(f"CSV contains the reserved NUL placeholder: {path}")
    rows = csv.DictReader(io.StringIO(text.replace("\0", marker), newline=""),
                          escapechar="\\" if source else None)
    return [{key: value.replace(marker, "\0") if isinstance(value, str) else value
             for key, value in row.items()} for row in rows]


def indexed(rows):
    ids = [int(r["row_id"]) for r in rows]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate row IDs")
    return dict(zip(ids, rows))


def load_json(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def disagreement(domain, label):
    return label.strip().lower() != "ml" if domain == "abstracts" else label.strip().lower() == "positive"


def audit_logs():
    reports, operators = [], collections.defaultdict(list)
    for path in sorted((ROOT / "logs").rglob("*.jsonl")):
        archived = "_pre_dedup" in path.parts
        invalid, nested, hashes_bad, valid = [], 0, 0, 0
        keys = collections.Counter()
        with path.open(encoding="utf-8", errors="strict") as stream:
            for number, line in enumerate(stream, 1):
                try:
                    record = json.loads(line)
                    if not isinstance(record, dict):
                        raise ValueError("not a JSON object")
                    valid += 1
                    if "messages" in record:
                        msgs = record["messages"]
                        if not isinstance(msgs, list) or any(not isinstance(m, dict) for m in msgs):
                            nested += 1
                        expected = hashlib.sha256(json.dumps(msgs, default=str).encode()).hexdigest()[:16]
                        hashes_bad += record.get("prompt_hash") != expected
                    elif "run_id" in record:
                        key = (record["run_id"], record.get("operator"), record.get("row_id"))
                        keys[key] += 1
                        if not archived:
                            operators[(record["run_id"], record.get("operator"))].append(record)
                except (ValueError, TypeError) as exc:
                    invalid.append({"line": number, "reason": str(exc)})
        reports.append({"path": path.relative_to(ROOT).as_posix(), "archived": archived,
                        "sha256": digest(path), "valid_records": valid,
                        "invalid_lines": invalid, "nonindividual_message_records": nested,
                        "prompt_hash_mismatches": hashes_bad,
                        "duplicate_operator_keys": sum(n - 1 for n in keys.values())})
    return reports, operators


def audit_run(map_path, operators, normalize):
    stem = map_path.name.removesuffix("_map.csv")
    rq1 = map_path.parent.name == "rq1"
    run_id = "rq1_" + stem if rq1 else stem
    domain = "abstracts" if "abstracts" in stem else "reviews"
    col = "mapped_topic" if domain == "abstracts" else "mapped_sentiment"
    valid = {"ML", "systems", "theory", "other"} if domain == "abstracts" else {"positive", "negative", "neutral"}
    wrapper = stem.startswith("wrapper_")
    if wrapper:
        col, valid = "about_ml", {"True", "False"}
    raw_col = "_raw_" + col
    issues, gaps = [], []
    meta_path = ROOT / "results" / f"{run_id}_meta.json"
    filter_path = map_path.with_name(stem + "_filter.csv")
    rows = indexed(read_csv(map_path))
    meta = load_json(meta_path) if meta_path.exists() else {}
    if not meta:
        gaps.append("metadata missing")
    if not meta.get("provenance"):
        gaps.append("execution-time environment/request provenance unavailable")
    if "posthoc" in meta.get("capture_status", ""):
        gaps.append("metadata is explicitly post-hoc")
    if not meta.get("artifact_sha256"):
        gaps.append("no execution-time artifact checksum binding")
    universe = set(indexed(read_csv(ROOT / f"data/{domain}/{domain}_v1.csv", source=True)))
    if "rq4_" in stem:
        inj = indexed(read_csv(ROOT / "data/abstracts/abstracts_v1_injection.csv"))
        universe = {rid for rid, row in inj.items() if row["has_submission_month"].lower() == "true"}
    total = meta.get("total_rows", meta.get("n_rows", meta.get("n_eligible_rows")))
    if "n50" in stem:
        universe = set(sorted(universe)[:50])
    elif "smoke" in stem and total:
        universe = set(sorted(universe)[:total])
    if not set(rows) <= universe:
        issues.append("map has row IDs outside the expected input universe")
    if total is not None and total != len(universe):
        issues.append("metadata input count disagrees with expected universe")
    filtered = None
    if filter_path.exists():
        filtered = indexed(read_csv(filter_path))
        if rq1:
            if set(filtered) != universe or set(rows) != universe:
                issues.append("isolated RQ1 frame does not cover its expected universe")
            if any(r.get("filter_passed") not in ("True", "False") for r in filtered.values()):
                issues.append("filter_passed contains nonboolean values")
            admitted = {rid for rid, r in filtered.items() if r.get("filter_passed") == "True"}
        else:
            admitted = set(filtered)
            if admitted != set(rows):
                issues.append("filter/map row IDs differ")
        expected = meta.get("filter_positive_count", meta.get("filter_passed"))
        if expected is not None and expected != len(admitted):
            issues.append("metadata admission count differs from CSV")
        if not set(filtered) <= universe:
            issues.append("filter IDs outside expected universe")
    elif "wrapper_" not in stem:
        issues.append("filter CSV missing")
    if wrapper and set(rows) != universe:
        issues.append("wrapper control does not cover its expected universe")
    if rows and col not in next(iter(rows.values())):
        # Wrapper map-only control has a deliberately different output schema.
        if "wrapper_" in stem:
            gaps.append("map-only wrapper control; evaluated separately")
            return {"run_id": run_id, "domain": domain, "map_path": str(map_path.relative_to(ROOT)),
                    "n_input_expected": len(universe), "n_map": len(rows), "issues": issues, "gaps": gaps}
        raise ValueError(f"missing {col}")
    raw_missing = raw_invalid = normalized_invalid = normalization_mismatch = 0
    for row in rows.values():
        raw = row.get(raw_col)
        raw_missing += raw is None
        normalized_invalid += row[col].strip().lower() not in {v.lower() for v in valid}
        if raw is not None:
            raw_invalid += raw.strip().lower() not in {v.lower() for v in valid}
            normalization_mismatch += normalize(raw, valid) != row[col]
    if raw_missing:
        gaps.append(f"raw map output absent on {raw_missing} rows")
    if normalization_mismatch:
        issues.append(f"{normalization_mismatch} stored labels differ from current normalization")
    log_summary = {}
    for op in ("sem_filter", "sem_map"):
        if wrapper and op == "sem_filter":
            continue
        log_id = run_id + ("_filter" if op == "sem_filter" else "_map") if rq1 else run_id
        records = operators.get((log_id, op), [])
        want = universe if op == "sem_filter" else set(rows)
        counts = collections.Counter(r["row_id"] for r in records)
        missing, extra = sorted(want - set(counts)), sorted(set(counts) - want)
        duplicates = sum(n - 1 for n in counts.values())
        mismatches = []
        for record in records:
            rid = record["row_id"]
            if op == "sem_map" and rid in rows and raw_col in rows[rid]:
                expected_output = rows[rid][raw_col]
            elif op == "sem_filter" and filtered is not None:
                expected_output = str(rid in admitted)
            else:
                continue
            if str(record.get("raw_output")) != expected_output:
                mismatches.append(rid)
        log_summary[op] = {"records": len(records), "missing_rows": missing, "extra_rows": extra,
                           "duplicate_records": duplicates, "output_mismatch_rows": sorted(set(mismatches))}
        if missing or extra or duplicates or mismatches:
            gaps.append(f"{op} operator log coverage/output mismatch")
    cohort = "headline" if (rq1 and "n50" not in stem) or stem.startswith(("rq3_abstracts_rep", "rq3_reviews_rep", "rq4_", "f7_ablation_")) else "control_or_exploratory"
    return {"run_id": run_id, "domain": domain, "cohort": cohort,
            "map_path": map_path.relative_to(ROOT).as_posix(), "n_input_expected": len(universe),
            "n_map": len(rows), "n_admitted": len(admitted) if filtered is not None else None,
            "operational_disagreements": None if wrapper else sum(disagreement(domain, r[col]) for r in rows.values()),
            "raw_invalid": raw_invalid, "raw_missing": raw_missing,
            "normalized_invalid": normalized_invalid, "normalization_mismatches": normalization_mismatch,
            "issues": issues, "gaps": gaps, "operator_logs": log_summary,
            "status": "structural_failure" if issues else "consistent_artifacts_with_provenance_gaps" if gaps else "consistent_artifacts"}


def verify_claims():
    claims = []
    def check(name, observed, expected, source):
        claims.append({"claim": name, "observed": observed, "reported": expected,
                       "match": observed == expected, "source": source})
    rq1 = load_json(ROOT / "results/rq1_hosted_summary.json")
    rq3 = load_json(ROOT / "results/rq3_summary.json")
    for domain in ("abstracts", "reviews"):
        col = "mapped_topic" if domain == "abstracts" else "mapped_sentiment"
        for op, label in (("filter", "filter_passed"), ("map", col)):
            runs = [indexed(read_csv(ROOT / f"results/rq1/hosted_{domain}_rep{r}_{op}.csv")) for r in range(1, 6)]
            common = set.intersection(*(set(r) for r in runs))
            flips = sum(len({run[rid][label] for run in runs}) > 1 for rid in common)
            check(f"RQ1 {domain} {op} flips", flips, rq1[domain][f"{op}_consistency"]["n_flipped"],
                  f"results/rq1/hosted_{domain}_rep1..5_{op}.csv")
        for r in range(1, 4):
            path = ROOT / f"results/rq3_{domain}_rep{r}_map.csv"
            rows = indexed(read_csv(path))
            k = sum(disagreement(domain, row[col]) for row in rows.values())
            expected = next(x for x in rq3[domain]["per_replicate"] if x["replicate"] == r)
            check(f"RQ3 {domain} rep{r} disagreements/admitted", [k, len(rows)],
                  [expected["n_contradictions"], expected["n_filter_positive"]], path.relative_to(ROOT).as_posix())
    f7 = load_json(ROOT / "results/f7_ablation_summary.json")
    for ref in f7["replicates"]:
        r = ref["replicate"]
        path = ROOT / f"results/f7_ablation_abstracts_rep{r}_map.csv"
        rows = indexed(read_csv(path))
        k = sum(disagreement("abstracts", row["mapped_topic"]) for row in rows.values())
        check(f"F12 rep{r} disagreements/admitted", [k, len(rows)],
              [ref["contradiction_ablated"]["k"], ref["contradiction_ablated"]["n"]], path.relative_to(ROOT).as_posix())
    rq4 = load_json(ROOT / "results/rq4_paired.json")
    for ref in rq4["replicates"]:
        r = ref["replicate"]
        bm = indexed(read_csv(ROOT / f"results/rq3_abstracts_rep{r}_map.csv"))
        im = indexed(read_csv(ROOT / f"results/rq4_abstracts_rep{r}_map.csv"))
        paired = set(bm) & set(im)
        bc = {i for i in paired if disagreement("abstracts", bm[i]["mapped_topic"])}
        ic = {i for i in paired if disagreement("abstracts", im[i]["mapped_topic"])}
        b, c = len(bc - ic), len(ic - bc)
        check(f"RQ4 rep{r} paired n/b/c", [len(paired), b, c],
              [ref["PAIRED_set_size"], ref["mcnemar"]["b_baseline_only"], ref["mcnemar"]["c_injected_only"]],
              f"results/rq4_abstracts_rep{r}_map.csv")
    return claims


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "audits/2026-10-07")
    args = parser.parse_args()
    output = args.output.resolve()
    if output == ROOT or any(output.is_relative_to(ROOT / folder) for folder in ("data", "results", "logs", "paper")):
        raise ValueError("Audit output must be separate from frozen results/logs")
    frozen = sorted(p for folder in ("data", "results", "logs", "paper")
                    for p in (ROOT / folder).rglob("*") if p.is_file())
    before = {p.relative_to(ROOT).as_posix(): digest(p) for p in frozen}
    spec = importlib.util.spec_from_file_location("audit_normalize", ROOT / "harness/normalize.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    logs, operators = audit_logs()
    runs = []
    for path in sorted((ROOT / "results").rglob("*_map.csv")):
        try:
            runs.append(audit_run(path, operators, module.normalize_label))
        except (ValueError, KeyError) as exc:
            runs.append({"run_id": path.stem, "status": "audit_error", "issues": [str(exc)], "gaps": []})
    claims = verify_claims()
    manifest = load_json(ROOT / "logs_manifest.json")["files"]
    have = {k: v for k, v in before.items() if k.startswith("logs/")}
    manifest_check = {"missing": sorted(set(manifest) - set(have)),
                      "extra": sorted(set(have) - set(manifest)),
                      "changed": [k for k in sorted(set(have) & set(manifest)) if have[k] != manifest[k]["sha256"]]}
    unchanged = all(digest(ROOT / rel) == checksum for rel, checksum in before.items())
    report = {"audit_date": "2026-10-07", "scope": "all saved map runs; all JSONL logs including archived backups; selected RQ1/RQ3/F12/RQ4 headline counts",
              "limitations": ["operator logs record parsed filter decisions, not raw provider filter responses",
                               "interceptor logs lack row/run joins sufficient to certify per-run request coverage",
                               "structural consistency is not independent scientific validation",
                               "audit checks current bytes; absent historical checksums/provenance remain absent"],
              "frozen_files_unchanged": unchanged, "manifest_check": manifest_check,
              "runs": runs, "logs": logs, "claims": claims, "input_sha256": before}
    output.mkdir(parents=True, exist_ok=True)
    (output / "evidence_audit.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    lines = ["# Frozen evidence audit — 7 October 2026", "",
             f"Audited {len(runs)} saved map runs and {len(logs)} JSONL logs. Frozen source files unchanged: **{unchanged}**.", "",
             "## Headline numerical checks", "", "| Claim | Recomputed | Reported | Match |", "|---|---|---|---|"]
    lines += [f"| {c['claim']} | {c['observed']} | {c['reported']} | {c['match']} |" for c in claims]
    lines += ["", "## Run evidence ledger", "", "| Run | Map rows | Raw invalid | Normalized invalid | Structural issues | Evidence gaps |", "|---|---:|---:|---:|---|---|"]
    lines += [f"| {r['run_id']} | {r.get('n_map', '?')} | {r.get('raw_invalid', '?')} | {r.get('normalized_invalid', '?')} | {'; '.join(r['issues']) or 'none found'} | {'; '.join(r['gaps']) or 'none found'} |" for r in runs]
    lines += ["", "## Log integrity", "", "| Log | Archived | Valid records | Invalid lines | Duplicate operator records |", "|---|---|---:|---:|---:|"]
    lines += [f"| {l['path']} | {l['archived']} | {l['valid_records']} | {len(l['invalid_lines'])} | {l['duplicate_operator_keys']} |" for l in logs]
    lines += ["", "## Archive manifest", "", json.dumps(manifest_check), "", "## Limits", ""]
    lines += ["- " + limit for limit in report["limitations"]]
    (output / "EVIDENCE-LEDGER.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"runs": len(runs), "logs": len(logs), "claim_checks": len(claims),
                      "claim_mismatches": sum(not c["match"] for c in claims),
                      "structural_failures": [r["run_id"] for r in runs if r["issues"]],
                      "invalid_log_lines": sum(len(l["invalid_lines"]) for l in logs),
                      "frozen_files_unchanged": unchanged, "output": str(output)}, indent=2))


if __name__ == "__main__":
    main()
