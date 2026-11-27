"""Compare each Spark workflow and field against the P03 Parquet reference."""
import argparse
from collections import Counter, defaultdict
from datetime import date
import gzip
import hashlib
import json
from math import isclose, isfinite
from pathlib import Path
import re
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from ghawtrace.feature_pipeline import sha256_file, write_json
from ghawtrace.features import _profile_schema


def identical(actual, expected):
    if isinstance(expected, float):
        return type(actual) in (int, float) and isfinite(actual) and isclose(
            actual, expected, rel_tol=1e-12, abs_tol=1e-12)
    return type(actual) is type(expected) and actual == expected


def verify(reference, spark_output, marker_file, checksum_file):
    reference_manifest = json.loads((reference / "manifest.json").read_text())
    marker = json.loads(marker_file.read_text())
    checks = reference_manifest["checks"]
    if (reference_manifest["status"] != "success" or len(checks) != 11
            or not all(c["ok"] is True for c in checks.values())
            or marker["reconciled_equals_input"] is not True
            or not (spark_output / "_SUCCESS").is_file()):
        raise ValueError("Missing successful clean, reference, or Spark output")
    if marker["total_input_rows"] != marker["retained_rows"] + marker["quarantined_rows"]:
        raise ValueError("P02 counts do not reconcile")
    exported = {}
    for line in checksum_file.read_text().splitlines():
        digest, name = line.split(maxsplit=1)
        if name in exported or not re.fullmatch("[0-9a-f]{64}", digest):
            raise ValueError("Invalid exported shard checksum list")
        exported[name] = digest
    expected_inputs = {r["path"]: r["sha256"] for r in reference_manifest["inputs"]}
    if not exported or exported != expected_inputs:
        raise ValueError("Reference input differs from the fixed P02 export")
    table = pq.read_table(reference / "workflow_profile.parquet")
    if table.schema != _profile_schema():
        raise ValueError("Unexpected workflow profile schema")
    # The Spark transport encodes timestamps as UTC microseconds, dates as ISO strings.
    for name in ("first_seen_utc", "last_seen_utc"):
        table = table.set_column(table.schema.get_field_index(name), name, table[name].cast(pa.int64()))
    expected = {}
    for row in table.to_pylist():
        key = (row["repo_id"], row["workflow_path"])
        if None in key or key in expected:
            raise ValueError("Null or duplicate reference key")
        for name in ("first_week_utc", "last_week_utc"):
            row[name] = date.isoformat(row[name])
        expected[key] = row
    hosts, partitions = Counter(), defaultdict(set)
    applications = set()
    run_count = 0
    columns = set(table.column_names)
    parts = sorted(spark_output.glob("part-*.gz"))
    if not parts or set(spark_output.glob("part-*")) != set(parts):
        raise ValueError("Expected gzip Spark partition outputs")
    for part in parts:
        partition = int(part.stem.removeprefix("part-"))
        with gzip.open(part, "rt", encoding="utf-8") as rows:
            for line in rows:
                item = json.loads(line)
                if (item["batch_token"] != marker["batch_token"] or item["partition"] != partition
                        or not item["worker"] or not item["application_id"].startswith("app-")):
                    raise ValueError("Invalid Spark execution provenance")
                actual = item["profile"]
                key = actual["repo_id"], actual["workflow_path"]
                wanted = expected.pop(key, None)
                if wanted is None or set(actual) != columns:
                    raise ValueError(f"Unexpected or duplicate workflow or mismatched columns: {key}")
                for name, value in wanted.items():
                    if not identical(actual[name], value):
                        raise ValueError(f"Mismatch for {key}, {name}: {actual[name]!r} vs {value!r}")
                run_count += actual["run_count"]
                hosts[item["worker"]] += 1
                partitions[item["worker"]].add(partition)
                applications.add(item["application_id"])
    if (expected or len(hosts) < 2 or len(applications) != 1 or run_count != marker["retained_rows"]
            or table.num_rows != checks["profile_workflows"]["input_workflows"]):
        raise ValueError("Incomplete workflow, run, or distributed execution evidence")
    return {
        "status": "success", "application_id": applications.pop(), "p02_batch": marker["batch_token"],
        "p03_batch": reference_manifest["batch_id"], "input_shards": len(exported),
        "clean_inventory_sha256": hashlib.sha256(json.dumps(exported, sort_keys=True).encode()).hexdigest(),
        "runs": run_count, "workflows": table.num_rows, "profile_columns": len(columns),
        "compared_cells": table.num_rows * len(columns), "mismatches": 0,
        "spark_part_files": len(parts), "workers": {
            host: {"workflows": n, "partitions": sorted(partitions[host])} for host, n in sorted(hosts.items())
        },
        "reference_checks": checks, "outputs": {name: {**stats, "sha256": sha256_file(reference / name)}
            for name, stats in reference_manifest["outputs"].items()},
        "numeric_tolerance": {"relative": 1e-12, "absolute": 1e-12},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("reference", "spark-output", "p02-marker", "clean-sha256", "report"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    report = args.report.resolve()
    if (report in (args.p02_marker.resolve(), args.clean_sha256.resolve())
            or any(report.is_relative_to(p.resolve()) for p in (args.reference, args.spark_output))):
        parser.error("Report must be outside the inputs")
    result = verify(args.reference, args.spark_output, args.p02_marker, args.clean_sha256)
    report.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=report.parent) as tmp:
        path = Path(tmp) / "report.json"
        write_json(path, result)
        path.replace(report)
    print(json.dumps({k: result[k] for k in ("application_id", "workflows", "compared_cells", "mismatches")}))


if __name__ == "__main__":
    main()
