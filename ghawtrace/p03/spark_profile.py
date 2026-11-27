# -*- coding: utf-8 -*-
"""Recompute the P03 full workflow profile on classroom Spark 2 / Python 2.7."""
from __future__ import division, print_function

from collections import Counter
from datetime import date, timedelta
import json
import math
import socket
import sys


EVENTS = ("issue_comment", "schedule", "pull_request_target", "pull_request",
          "issues", "check_run", "workflow_run", "pull_request_review_comment",
          "workflow_dispatch", "push", "deployment_status", "check_suite",
          "pull_request_review", "discussion", "discussion_comment", "release",
          "repository_dispatch", "create")
CONCLUSIONS = ("success", "failure", "skipped", "cancelled", "action_required", "startup_failure")
DAY = 86400000000


def percentile(sorted_values, percent):
    if not sorted_values:
        return None
    index, remainder = divmod((len(sorted_values) - 1) * percent, 100)
    value = sorted_values[index]
    if remainder:
        value += (sorted_values[index + 1] - value) * remainder / 100
    return value / 1000000


def entropy(counts, size):
    return -sum((n / size) * math.log(n / size, 2) for n in counts if n)


def monday(day):
    return (date(1970, 1, 1) + timedelta(days=day - (day + 3) % 7)).isoformat()


def profile(key, rows):
    times, elapsed, attempts = [], [], []
    workflow_ids = set()
    events, outcomes, days, hours = Counter(), Counter(), Counter(), Counter()
    open_count = negative = 0
    for workflow_id, created, updated, event, status, conclusion, attempt in rows:
        if created is None:
            raise ValueError("missing creation time")
        times.append(created)
        days[created // DAY] += 1
        hours[(created // 3600000000) % 24] += 1
        if workflow_id is not None and workflow_id >= 0:
            workflow_ids.add(workflow_id)
        events["unknown" if event is None else event] += 1
        outcomes["unknown" if conclusion is None else conclusion] += 1
        if status == "completed":
            if updated is not None:
                if updated >= created:
                    elapsed.append(updated - created)
                else:
                    negative += 1
            if attempt is not None and attempt >= 1:
                attempts.append(attempt)
        elif status not in (None, "unknown"):
            open_count += 1
    times.sort()
    elapsed.sort()
    gaps = sorted(times[i] - times[i - 1] for i in range(1, len(times)))
    size = len(times)
    span = max(days) - min(days) + 1
    decisive = outcomes["failure"] + outcomes["success"]
    retried = sum(n > 1 for n in attempts)
    peak = min(hours, key=lambda h: (-hours[h], h))
    top = min(events, key=lambda event: (-events[event], event))
    result = dict(repo_id=key[0], workflow_path=key[1], workflow_id=min(workflow_ids) if workflow_ids else None,
                  workflow_id_variants=len(workflow_ids), workflow_id_conflict=len(workflow_ids) > 1,
                  run_count=size, first_seen_utc=times[0], last_seen_utc=times[-1],
                  first_week_utc=monday(min(days)), last_week_utc=monday(max(days)),
                  observation_span_days=span, active_day_count=len(days), active_day_rate=len(days) / span,
                  mean_runs_per_active_day=size / len(days), max_daily_runs=max(days.values()),
                  unknown_conclusion_count=outcomes["unknown"],
                  other_conclusion_count=sum(n for c, n in outcomes.items() if c not in CONCLUSIONS + ("unknown",)),
                  skipped_rate=outcomes["skipped"] / size,
                  failure_rate=outcomes["failure"] / decisive if decisive else None,
                  failure_rate_estimable=bool(decisive), open_run_count=open_count,
                  attempts_known_count=len(attempts), total_attempts=sum(attempts), retried_run_count=retried,
                  mean_attempts=sum(attempts) / len(attempts) if attempts else None,
                  retry_rate=retried / len(attempts) if attempts else None, retry_rate_estimable=bool(attempts),
                  max_attempts=max(attempts) if attempts else None,
                  elapsed_missing_count=size-len(elapsed), elapsed_known_count=len(elapsed),
                  elapsed_negative_count=negative, elapsed_zero_count=elapsed.count(0),
                  elapsed_p99_seconds=percentile(elapsed, 99), interval_count=len(gaps),
                  interval_min_seconds=gaps[0] / 1000000 if gaps else None,
                  peak_hour_utc=peak, peak_hour_share=hours[peak] / size,
                  hour_entropy_bits=entropy(hours.values(), size), night_share_utc=sum(hours[h] for h in range(6)) / size,
                  event_type_count=len(events), top_event=top, top_event_share=events[top] / size,
                  event_entropy_bits=entropy(events.values(), size), schedule_share=events["schedule"] / size,
                  dispatch_share=(events["workflow_dispatch"] + events["repository_dispatch"]) / size)
    for prefix, values in (("elapsed", elapsed), ("interval", gaps)):
        result[prefix + "_estimable"] = bool(values)
        result[prefix + "_mean_seconds"] = sum(values) / (1000000 * len(values)) if values else None
        result[prefix + "_max_seconds"] = values[-1] / 1000000 if values else None
        result[prefix + "_median_seconds"] = percentile(values, 50)
        result[prefix + "_p90_seconds"] = percentile(values, 90)
    for c in CONCLUSIONS:
        result[c + "_count"] = outcomes[c]
    for hour in range(24):
        result["hour_%02d_utc_share" % hour] = hours[hour] / size
    for event in EVENTS + ("unknown",):
        result["event_%s_share" % event] = events[event] / size
    result["event_other_share"] = sum(n for e, n in events.items() if e not in EVENTS + ("unknown",)) / size
    result["webhook_share"] = max(0., 1 - result["schedule_share"] - result["dispatch_share"] - result["event_unknown_share"])
    return result


def emit_partition(index, groups, application_id, batch_token):
    host = socket.gethostname()
    for key, values in groups:
        yield json.dumps(dict(worker=host, partition=index, application_id=application_id,
                              batch_token=batch_token, profile=profile(key, values)), allow_nan=False)


def main():
    from pyspark.sql import SparkSession
    if len(sys.argv) != 3:
        raise SystemExit("usage: spark_profile.py P02_SUCCESSFUL_BATCH_DIR FRESH_HDFS_OUTPUT")
    batch, target = sys.argv[1].rstrip("/"), sys.argv[2]
    spark = SparkSession.builder.appName("GHAWTrace P03 workflow profiles").getOrCreate()
    sc = spark.sparkContext
    try:
        path = sc._jvm.org.apache.hadoop.fs.Path(batch + "/_SUCCESS")
        stream = path.getFileSystem(sc._jsc.hadoopConfiguration()).open(path)
        try:
            marker = json.loads(sc._jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8"))
        finally:
            stream.close()
        token = batch.rsplit("/", 1)[-1]
        if marker["batch_token"] != token or marker["reconciled_equals_input"] is not True:
            raise ValueError("P02 success marker does not match the requested immutable batch")
        data = spark.read.parquet(batch + "/runs_clean")
        def micros(column):
            # Spark 2 renders timestamps in the UTC JVM zone. Keep the fraction
            # separate from epoch seconds so no floating-point cast drops a microsecond.
            stamp = "cast(%s as string)" % column
            return ("unix_timestamp(substring({0},1,19), 'yyyy-MM-dd HH:mm:ss') * 1000000 + "
                    "cast(rpad(substring({0},21,6),6,'0') as bigint)").format(stamp)

        rows = data.selectExpr("repo_id", "workflow_path", "workflow_id", micros("created_at_utc"),
                               micros("updated_at_utc"), "event", "status", "conclusion", "run_attempts")
        keyed = rows.rdd.map(lambda r: ((r[0], r[1]), tuple(r[2:])))
        groups = keyed.groupByKey(int(spark.conf.get("spark.sql.shuffle.partitions")))
        app = sc.applicationId
        groups.mapPartitionsWithIndex(lambda i, g: emit_partition(i, g, app, token)).saveAsTextFile(
            target, "org.apache.hadoop.io.compress.GzipCodec")
        print("P03_SPARK_SUCCESS", app, token)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
