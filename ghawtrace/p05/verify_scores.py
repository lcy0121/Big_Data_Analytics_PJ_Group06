"""Recompute selected stored Spark scores directly from the immutable P02 Parquet."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
from statistics import median

import pyarrow.compute as pc
import pyarrow.dataset as ds

from ghawtrace.feature_pipeline import sha256_file

KEY = ["repo_id", "workflow_path"]
INPUT = ["run_id", *KEY, "created_at_utc", "updated_at_utc", "status", "conclusion",
         "elapsed_seconds", "run_attempts"]
CHECK = ["score_at_utc", "history_start_utc", "history_end_utc", "n_history", "n_elapsed",
         "n_attempts", "median_seconds", "mad_seconds", "tail_count", "elapsed_score",
         "retry_score", "score", "score_status", "reason"]


def reference(target, workflow):
    """Filter the raw group afresh per target; independent of the Spark streaming window."""
    end = target["updated_at_utc"]
    valid_end = end is not None and end >= target["created_at_utc"] and target["status"] == "completed"
    out = dict.fromkeys(CHECK)
    out.update(score_at_utc=end if valid_end else None, score_status="unscored")
    if target["status"] != "completed":
        out["reason"] = "not_completed"
    elif target["conclusion"] == "skipped":
        out["reason"] = "skipped"
    elif not valid_end:
        out["reason"] = "invalid_update_time"
    elif not target["conclusion"]:
        out["reason"] = "missing_conclusion"
    else:
        visible = [r for r in workflow if r["status"] == "completed" and r["conclusion"]
                   and r["conclusion"] != "skipped" and r["updated_at_utc"] is not None
                   and r["created_at_utc"] <= r["updated_at_utc"] < end]
        prior = sorted(visible, key=lambda r: (r["updated_at_utc"], r["run_id"]))[-64:]
        times = [r["elapsed_seconds"] for r in prior if r["elapsed_seconds"] is not None
                 and r["elapsed_seconds"] >= 0]
        attempts = [r["run_attempts"] for r in prior if (r["run_attempts"] or 0) > 0]
        out.update(n_history=len(prior), n_elapsed=len(times), n_attempts=len(attempts))
        if prior:
            out.update(history_start_utc=prior[0]["updated_at_utc"], history_end_utc=prior[-1]["updated_at_utc"])
        value = target["elapsed_seconds"]
        has_elapsed = value is not None and value >= 0
        has_attempts = (target["run_attempts"] or 0) > 0
        if len(times) >= 5:
            m = out["median_seconds"] = median(times)
            mad = out["mad_seconds"] = median(abs(t - m) for t in times)
            if has_elapsed:
                z = abs(value - m) / max(1.0, 1.4826 * mad)
                out["elapsed_score"] = z / (1.0 + z)
        if has_attempts and len(attempts) >= 5:
            tail = out["tail_count"] = sum(a >= target["run_attempts"] for a in attempts)
            out["retry_score"] = 1 - (1 + tail) / (1 + len(attempts))
        duration, retry = out["elapsed_score"], out["retry_score"]
        scores = [s for s in (duration, retry) if s is not None]
        if scores:
            out["score"] = max(scores)
            out["score_status"] = "partial" if len(scores) == 1 else "scored"
            if out["score"] == 0:
                out["reason"] = "within_baseline"
            elif duration is not None and retry is not None and duration == retry:
                out["reason"] = "elapsed_and_retries"
            elif out["score"] == retry:
                out["reason"] = "unusual_retries"
            else:
                out["reason"] = "longer_elapsed" if value > m else "shorter_elapsed"
        else:
            out["reason"] = "missing_metrics" if not (has_elapsed or has_attempts) else "insufficient_history"
    return out


def assert_equal(field, expected, actual, run_id):
    equal = actual == expected
    if isinstance(expected, float) and actual is not None:
        equal = math.isclose(expected, actual, rel_tol=1e-12, abs_tol=1e-12)
    if not equal:
        raise ValueError(f"Run {run_id} {field}: expected {expected!r}, got {actual!r}")


def read_batch(batch, mode):
    manifest = json.loads((batch / "manifest.json").read_text())
    if (manifest["status"] != "success" or manifest["mode"] != mode or manifest["version"] != 1
            or manifest["rows"] != manifest["unique_runs"] or manifest["key_differences"] != 0):
        raise ValueError(f"Invalid {mode} score manifest")
    contract = dict(history_size=64, min_support=5, mad_factor=1.4826, scale_floor_seconds=1.0,
                    combine="max_available", timezone="UTC", time_type="timestamp")
    if any(manifest.get(k) != value for k, value in contract.items()):
        raise ValueError(f"Unexpected {mode} score configuration")
    if sum(r["count"] for r in manifest["breakdown"]) != manifest["rows"]:
        raise ValueError(f"Incomplete {mode} status breakdown")
    return manifest, ds.dataset(batch / "run_scores", format="parquet")


def examples(scores):
    # Deterministic minimum ID per case, independent of Spark partition order.
    col = ds.field
    criteria = {
        "normal": (col("score_status") == "scored") & (col("score") == 0) & (col("mad_seconds") > 0),
        "duration_spike": (col("reason") == "longer_elapsed") & (col("elapsed_score") >= 0.9)
                          & (col("mad_seconds") > 0),
        "zero_mad": (col("mad_seconds") == 0) & (col("elapsed_score") > 0),
        "rare_retry": (col("reason") == "unusual_retries") & (col("retry_score") >= 0.8),
    }
    result = {}
    for name, condition in criteria.items():
        value = pc.min(scores.to_table(columns=["run_id"], filter=condition)["run_id"]).as_py()
        if value is None and name != "rare_retry":
            raise ValueError(f"Missing real {name} case")
        if value is not None:
            result[name] = value
    return result


def compare_full_outputs(first, second):
    """Compare by run ID, reading only two value columns at a time to bound memory."""
    if first.schema != second.schema:
        raise ValueError("Full reruns produced different schemas")
    columns = [name for name in first.schema.names if name not in ("run_id", "worker")]
    def ordered(source, subset):
        table = source.to_table(columns=subset)
        return table.take(pc.sort_indices(table["run_id"]))

    for offset in range(0, len(columns), 2):
        subset = ["run_id", *columns[offset:offset + 2]]
        if not ordered(first, subset).equals(ordered(second, subset)):
            raise ValueError(f"Full reruns differ in {subset}")
    return 1 + len(columns)


def verify(clean_batch, sample_manifest, sample_batch, full_batch, repeat_batch):
    sample, small = read_batch(sample_batch, "sample")
    full, scores = read_batch(full_batch, "full")
    repeat, repeat_scores = read_batch(repeat_batch, "full")
    if full["application_id"] == repeat["application_id"]:
        raise ValueError("Expected two independent Spark runs")
    clean = clean_batch / "runs_clean"
    hashes = {p.name: sha256_file(p) for p in sorted(clean.glob("*.parquet"))}
    digest = hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()
    marker = json.loads((clean_batch / "_SUCCESS").read_text())
    for manifest in (sample, full, repeat):
        if (manifest["clean_inventory_sha256"] != digest or manifest["p02_batch"] != marker["batch_token"]
                or manifest["input_shards"] != len(hashes) or manifest["scorer_sha256"] != full["scorer_sha256"]):
            raise ValueError("P02 inputs differ")
    if not hashes or marker["reconciled_equals_input"] is not True or marker["batch_token"] != clean_batch.name:
        raise ValueError("Invalid fixed P02 input")
    if full["rows"] != marker["retained_rows"] or repeat["rows"] != full["rows"]:
        raise ValueError("Full output counts differ from P02")
    def breakdown(manifest):
        groups = Counter()
        for item in manifest["breakdown"]:
            groups[item["score_status"], item["reason"]] += item["count"]
        return groups
    if breakdown(full) != breakdown(repeat):
        raise ValueError("Full reruns produced different status/reason counts")

    ids = set(json.loads(sample_manifest.read_text())["run_ids"])
    sample_rows = {r["run_id"]: r for r in small.to_table().to_pylist()}
    if (set(sample_rows) != ids or sample["rows"] != len(ids)
            or sample["sample_manifest_sha256"] != sha256_file(sample_manifest)):
        raise ValueError("Sample run IDs or manifest differ")
    compared_columns = compare_full_outputs(scores, repeat_scores)
    cases = examples(scores)
    chosen = ids.union(cases.values())
    id_filter = ds.field("run_id").isin(sorted(chosen))
    full_rows = {r["run_id"]: r for r in scores.to_table(filter=id_filter).to_pylist()}
    repeated = {r["run_id"]: r for r in repeat_scores.to_table(filter=id_filter).to_pylist()}
    source = ds.dataset(clean, format="parquet")
    targets = source.to_table(columns=INPUT, filter=id_filter).to_pylist()
    if set(full_rows) != chosen or set(repeated) != chosen or {r["run_id"] for r in targets} != chosen:
        raise ValueError("Missing independent check targets")
    groups = defaultdict(list)
    for target in targets:
        groups[target["repo_id"], target["workflow_path"]].append(target)
    for (repo, path), selected in groups.items():
        same_workflow = (ds.field("repo_id") == repo) & (ds.field("workflow_path") == path)
        workflow = source.to_table(columns=INPUT, filter=same_workflow).to_pylist()
        for target in selected:
            run_id = target["run_id"]
            wanted = reference(target, workflow)
            wanted.update({k: target[k] for k in [*KEY, "created_at_utc", "run_id"]})
            wanted["elapsed_seconds"] = target["elapsed_seconds"] if (target["elapsed_seconds"] or 0) >= 0 else None
            wanted["run_attempts"] = target["run_attempts"] if (target["run_attempts"] or 0) >= 1 else None
            copies = [full_rows[run_id], repeated[run_id]]
            if run_id in sample_rows:
                copies.append(sample_rows[run_id])
            for row in copies:
                for field, value in wanted.items():
                    assert_equal(field, value, row[field], run_id)
    def summary(manifest):
        fields = ("application_id", "spark_version", "master", "rows", "unique_runs", "key_differences",
                  "input_shards", "partitions", "breakdown", "mode", "time_type", "timezone")
        return {f: manifest[f] for f in fields}
    return dict(status="success", p02_batch=marker["batch_token"], clean_inventory_sha256=digest,
                scorer_sha256=full["scorer_sha256"],
                sample_manifest_sha256=sample["sample_manifest_sha256"], version=full["version"],
                history_size=64, min_support=5, mad_factor=1.4826, scale_floor_seconds=1.0,
                combine="max_available", sample=summary(sample), full=summary(full), repeat=summary(repeat),
                independent_targets=len(chosen), sample_checked=len(ids), mismatches=0, tolerance=1e-12,
                repeat_compared_rows=full["rows"], repeat_compared_columns=compared_columns,
                cases={name: {k: v for k, v in full_rows[run_id].items() if k != "worker"}
                       for name, run_id in cases.items()})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("clean-batch", "sample-manifest", "sample-batch", "full-batch", "repeat-batch"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    result = verify(args.clean_batch, args.sample_manifest, args.sample_batch, args.full_batch, args.repeat_batch)
    print(json.dumps(result, indent=2, sort_keys=True,
                     default=datetime.isoformat))


if __name__ == "__main__":
    main()
