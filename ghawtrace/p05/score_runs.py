# -*- coding: utf-8 -*-
"""P05-T01. Score each run against earlier completions of the same workflow."""
from __future__ import division, print_function

import argparse
from collections import deque
import hashlib
from itertools import groupby
import json
import socket
import uuid

HISTORY_SIZE = 64
MIN_SUPPORT = 5
MAD_FACTOR = 1.4826
SCALE_FLOOR_SECONDS = 1.0
VERSION = 1

# Timestamps cross the Python/JVM boundary as canonical UTC strings. Spark
# restores TimestampType before writing Parquet, including fractional seconds.
FIELDS = (
    ("run_id", "long"), ("repo_id", "long"), ("workflow_path", "string"),
    ("created_at_utc", "time"), ("score_at_utc", "time"),
    ("elapsed_seconds", "double"), ("run_attempts", "long"),
    ("history_start_utc", "time"), ("history_end_utc", "time"),
    ("n_history", "integer"), ("n_elapsed", "integer"), ("n_attempts", "integer"),
    ("median_seconds", "double"), ("mad_seconds", "double"), ("tail_count", "integer"),
    ("elapsed_score", "double"), ("retry_score", "double"), ("score", "double"),
    ("score_status", "string"), ("reason", "string"), ("worker", "string"),
)


def median(values):
    ordered = sorted(values)
    mid = len(ordered) // 2
    return (ordered[mid] + ordered[(len(ordered) - 1) // 2]) / 2.0


def history_summary(history):
    durations = [v for _, v, _ in history if v is not None]
    attempts = [a for _, _, a in history if a is not None]
    center = mad = None
    if len(durations) >= MIN_SUPPORT:
        center = median(durations)
        mad = median([abs(v - center) for v in durations])
    bounds = (history[0][0], history[-1][0]) if history else (None, None)
    return bounds + (len(history), len(durations), attempts, center, mad)


def component_scores(elapsed, attempts, baseline):
    start, end, size, n_elapsed, prior_attempts, center, mad = baseline
    duration_score = retry_score = tail = None
    n_attempts = len(prior_attempts)
    if elapsed is not None and center is not None:
        deviation = abs(elapsed - center)
        scale = max(SCALE_FLOOR_SECONDS, MAD_FACTOR * mad)
        duration_score = deviation / (scale + deviation)
    if attempts is not None and n_attempts >= MIN_SUPPORT:
        tail = sum(a >= attempts for a in prior_attempts)
        retry_score = (n_attempts - tail) / (n_attempts + 1)
    available = [s for s in (duration_score, retry_score) if s is not None]
    total = max(available) if available else None
    if total is None:
        reason = "missing_metrics" if elapsed is None and attempts is None else "insufficient_history"
    elif total == 0:
        reason = "within_baseline"
    elif retry_score == total and duration_score == total:
        reason = "elapsed_and_retries"
    elif retry_score == total:
        reason = "unusual_retries"
    else:
        reason = "longer_elapsed" if elapsed > center else "shorter_elapsed"
    state = ("unscored", "partial", "scored")[len(available)]
    return (start, end, size, n_elapsed, n_attempts, center, mad, tail,
            duration_score, retry_score, total, state, reason)


def exclusion(status, conclusion, target_time):
    if status != "completed":
        return "not_completed"
    if conclusion == "skipped":
        return "skipped"
    if not target_time:
        return "invalid_update_time"
    if not conclusion:
        return "missing_conclusion"
    return None


def score_partition(rows):
    """Input order: workflow, final update time, run_id. Keep bounded tie buffers."""
    host = socket.gethostname()
    for (repo, path), workflow in groupby(rows, key=lambda row: (row[0], row[1])):
        history = deque(maxlen=HISTORY_SIZE)
        for updated, tied in groupby(workflow, key=lambda row: row[2]):
            pending = deque(maxlen=HISTORY_SIZE)
            baseline = None
            for _, _, _, run_id, created, status, conclusion, elapsed, attempts in tied:
                target_time = updated if status == "completed" and updated and updated >= created else None
                elapsed = elapsed if elapsed is not None and elapsed >= 0 else None
                attempts = attempts if attempts is not None and attempts >= 1 else None
                reason = exclusion(status, conclusion, target_time)
                if reason:
                    metrics = (None,) * 11 + ("unscored", reason)
                else:
                    # Simultaneous completions share a baseline and enter it together afterwards.
                    if baseline is None:
                        baseline = history_summary(history)
                    metrics = component_scores(elapsed, attempts, baseline)
                    pending.append((updated, elapsed, attempts))
                identifiers = (run_id, repo, path, created, target_time, elapsed, attempts)
                yield identifiers + metrics + (host,)
            history.extend(pending)


def input_evidence(sc, batch):
    jvm = sc._jvm
    path = jvm.org.apache.hadoop.fs.Path(batch)
    fs = path.getFileSystem(sc._jsc.hadoopConfiguration())
    marker_stream = fs.open(jvm.org.apache.hadoop.fs.Path(path, "_SUCCESS"))
    try:
        marker = json.loads(jvm.org.apache.commons.io.IOUtils.toString(marker_stream, "UTF-8"))
    finally:
        marker_stream.close()
    if (marker["batch_token"] != path.getName() or marker["reconciled_equals_input"] is not True
            or marker["retained_rows"] + marker["quarantined_rows"] != marker["total_input_rows"]):
        raise ValueError("Invalid P02 batch marker")
    hashes = {}
    clean = jvm.org.apache.hadoop.fs.Path(path, "runs_clean")
    for file_status in fs.listStatus(clean):
        item = file_status.getPath()
        if item.getName().startswith(("_", ".")):
            continue
        if not file_status.isFile() or not item.getName().endswith(".parquet"):
            raise ValueError("Unexpected P02 clean input: " + item.toString())
        stream = fs.open(item)
        try:
            hashes[item.getName()] = jvm.org.apache.commons.codec.digest.DigestUtils.sha256Hex(stream)
        finally:
            stream.close()
    if not hashes:
        raise ValueError("Empty P02 batch")
    return marker, hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest(), len(hashes)


def run(spark, args):
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    batch = args.p02_batch.rstrip("/")
    path_type = sc._jvm.org.apache.hadoop.fs.Path
    conf = sc._jsc.hadoopConfiguration()
    source_path, target_path = path_type(batch), path_type(args.output_root)
    source_path = source_path.getFileSystem(conf).makeQualified(source_path)
    parent = target_path.getFileSystem(conf).makeQualified(target_path)
    while parent is not None:
        if parent.equals(source_path):
            raise ValueError("Output root must be outside the immutable P02 batch")
        parent = parent.getParent()
    marker, digest, parts = input_evidence(sc, batch)
    data = spark.read.parquet(batch + "/runs_clean")
    typed = dict(data.dtypes)
    if any(typed.get(col) != "timestamp" for col in ("created_at_utc", "updated_at_utc")):
        raise ValueError("P02 input requires typed UTC timestamps")
    if sc._jvm.java.util.TimeZone.getDefault().getID() != "UTC":
        raise ValueError("Set driver and executor JVM timezones to UTC")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    expected = data.select("run_id")
    expected_count = marker["retained_rows"]
    sample_digest = None
    if args.sample_manifest:
        with open(args.sample_manifest, "rb") as source:
            raw = source.read()
        sample_digest = hashlib.sha256(raw).hexdigest()
        ids = json.loads(raw)["run_ids"]
        if not ids or len(set(ids)) != len(ids):
            raise ValueError("Expected nonempty unique sample run IDs")
        sample = data.where(F.col("run_id").isin(ids))
        # Preserve the complete histories of anchor workflows as well as cohorts.
        data = data.join(F.broadcast(sample.select("repo_id", "workflow_path").distinct()),
                         ["repo_id", "workflow_path"])
        expected, expected_count = sample.select("run_id"), len(ids)
    rows = data.selectExpr("repo_id", "workflow_path", "cast(updated_at_utc as string) as updated",
                           "run_id", "cast(created_at_utc as string) as created", "status", "conclusion",
                           "cast(elapsed_seconds as double)", "cast(run_attempts as bigint)")
    ordered = rows.repartition(args.partitions, "repo_id", "workflow_path").sortWithinPartitions(
        "repo_id", "workflow_path", "updated", "run_id")
    types = dict(long=T.LongType(), string=T.StringType(), time=T.StringType(),
                 integer=T.IntegerType(), double=T.DoubleType())
    schema = T.StructType([T.StructField(name, types[kind], True) for name, kind in FIELDS])
    scores = spark.createDataFrame(ordered.rdd.mapPartitions(score_partition), schema)
    if args.sample_manifest:
        scores = scores.where(F.col("run_id").isin(ids))
    for name, kind in FIELDS:
        if kind == "time":
            scores = scores.withColumn(name, F.col(name).cast("timestamp"))
    dest = args.output_root.rstrip("/") + "/p05-" + sc.applicationId + "-" + uuid.uuid4().hex[:12]
    scores.write.mode("error").parquet(dest + "/run_scores")
    stored = spark.read.parquet(dest + "/run_scores")
    counts = stored.agg(F.count("*"), F.countDistinct("run_id")).first()
    if (tuple(counts) != (expected_count, expected_count)
            or expected.subtract(stored.select("run_id")).take(1)
            or stored.select("run_id").subtract(expected).take(1)):
        raise ValueError("Stored score keys or row counts differ from the selected P02 runs")
    if any(dict(stored.dtypes)[n] != "timestamp" for n, k in FIELDS if k == "time"):
        raise ValueError("Stored time types differ")
    breakdown = stored.groupBy("score_status", "reason", "worker").count().collect()
    with open(__file__, "rb") as script:
        code_digest = hashlib.sha256(script.read()).hexdigest()
    report = dict(status="success", version=VERSION, scorer_sha256=code_digest, mode="sample" if args.sample_manifest else "full",
                  application_id=sc.applicationId, master=sc.master, spark_version=spark.version,
                  p02_batch=marker["batch_token"], clean_inventory_sha256=digest, input_shards=parts,
                  sample_manifest_sha256=sample_digest, rows=counts[0], unique_runs=counts[1],
                  input_uri=batch, output_uri=dest, time_type="timestamp", timezone="UTC",
                  key_differences=0, partitions=args.partitions, history_size=HISTORY_SIZE,
                  min_support=MIN_SUPPORT, mad_factor=MAD_FACTOR, scale_floor_seconds=SCALE_FLOOR_SECONDS,
                  combine="max_available", breakdown=sorted([r.asDict() for r in breakdown],
                      key=lambda r: (r["worker"], r["score_status"], r["reason"])))
    path = sc._jvm.org.apache.hadoop.fs.Path(dest)
    fs = path.getFileSystem(sc._jsc.hadoopConfiguration())
    temp = sc._jvm.org.apache.hadoop.fs.Path(path, ".manifest-writing")
    stream = fs.create(temp, False)
    try:
        stream.write(bytearray(json.dumps(report, sort_keys=True, indent=2).encode("utf-8")))
    finally:
        stream.close()
    if not fs.rename(temp, sc._jvm.org.apache.hadoop.fs.Path(path, "manifest.json")):
        raise IOError("Failed to publish scored batch manifest")
    print("P05_SUCCESS", dest)


def main():
    from pyspark.sql import SparkSession
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("p02_batch")
    parser.add_argument("output_root")
    parser.add_argument("--sample-manifest", help="driver-local P01 sample_manifest.json")
    parser.add_argument("--partitions", type=int, default=24)
    args = parser.parse_args()
    if args.partitions < 1:
        parser.error("--partitions must be positive")
    spark = SparkSession.builder.appName("GHAWTrace P05 run scores").getOrCreate()
    try:
        run(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
