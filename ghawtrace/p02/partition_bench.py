# -*- coding: utf-8 -*-
from __future__ import division, print_function

import hashlib
import json
import sys
import time

try:
    import urllib2
except ImportError:
    import urllib.request as urllib2

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def fetch_spark_rest(ui_url, app_id, endpoint):
    url = "%s/api/v1/applications/%s/%s" % (ui_url.rstrip("/"), app_id, endpoint)
    try:
        resp = urllib2.urlopen(url, timeout=10)
        raw_text = resp.read()
        if isinstance(raw_text, bytes):
            raw_text = raw_text.decode("utf-8")
        return json.loads(raw_text)
    except Exception as exc:
        return {"error": str(exc), "url": url}


def _null_totals():
    return {
        "stage_count": None,
        "total_tasks": None,
        "executor_run_time_ms": None,
        "input_bytes": None,
        "input_records": None,
        "output_bytes": None,
        "output_records": None,
        "shuffle_read_bytes": None,
        "shuffle_read_records": None,
        "shuffle_write_bytes": None,
        "shuffle_write_records": None,
        "memory_bytes_spilled": None,
        "disk_bytes_spilled": None,
    }


def _zero_totals():
    return {
        "stage_count": 0,
        "total_tasks": 0,
        "executor_run_time_ms": 0,
        "input_bytes": 0,
        "input_records": 0,
        "output_bytes": 0,
        "output_records": 0,
        "shuffle_read_bytes": 0,
        "shuffle_read_records": 0,
        "shuffle_write_bytes": 0,
        "shuffle_write_records": 0,
        "memory_bytes_spilled": 0,
        "disk_bytes_spilled": 0,
    }

def evaluate_t05_evidence(stages_raw, executors_raw):
    """
    纯函数：根据两个 Spark REST 返回判定 T05 证据是否足够可验收。
    返回 (evidence_complete, breakdown, errors)。
    可验收条件（全部满足）：
      1. 两个接口都正常返回 list（不是 error dict）；
      2. 至少 1 个 status == "COMPLETE" 的 stage；
      3. 这些完成 stage 的 numCompleteTasks 累计 > 0；
      4. 至少 1 个非 driver 的 executor 有 completedTasks > 0。
    """
    stages_ok = isinstance(stages_raw, list)
    executors_ok = isinstance(executors_raw, list)

    errors = []
    if not stages_ok:
        errors.append({
            "endpoint": "stages",
            "detail": stages_raw if isinstance(stages_raw, dict)
                      else {"error": "Unexpected stages payload type"},
        })
    if not executors_ok:
        errors.append({
            "endpoint": "executors",
            "detail": executors_raw if isinstance(executors_raw, dict)
                      else {"error": "Unexpected executors payload type"},
        })

    completed_stages = []
    total_completed_tasks = 0
    if stages_ok:
        completed_stages = [s for s in stages_raw if s.get("status") == "COMPLETE"]
        for s in completed_stages:
            try:
                total_completed_tasks += int(s.get("numCompleteTasks") or 0)
            except (TypeError, ValueError):
                pass

    workers_with_tasks = 0
    if executors_ok:
        for ex in executors_raw:
            if ex.get("id") == "driver":
                continue
            try:
                if int(ex.get("completedTasks") or 0) > 0:
                    workers_with_tasks += 1
            except (TypeError, ValueError):
                pass

    has_completed_stage = len(completed_stages) > 0
    has_tasks = total_completed_tasks > 0
    has_worker_with_tasks = workers_with_tasks > 0

    if stages_ok and not has_completed_stage:
        errors.append({"endpoint": "stages", "detail": "no stage with status=COMPLETE"})
    if stages_ok and has_completed_stage and not has_tasks:
        errors.append({"endpoint": "stages", "detail": "completed stages have zero tasks"})
    if executors_ok and not has_worker_with_tasks:
        errors.append({"endpoint": "executors",
                       "detail": "no non-driver executor with completedTasks > 0"})

    evidence_complete = bool(
        stages_ok and executors_ok
        and has_completed_stage and has_tasks and has_worker_with_tasks
    )

    breakdown = {
        "stages_available": stages_ok,
        "executors_available": executors_ok,
        "completed_stage_count": len(completed_stages),
        "total_completed_tasks": total_completed_tasks,
        "workers_with_completed_tasks": workers_with_tasks,
    }
    return evidence_complete, breakdown, errors


def main():
    if len(sys.argv) < 5:
        print("Usage: partition_bench.py <label> <input_clean_parquet> <output_agg_parquet> <report_json>")
        sys.exit(1)

    label = sys.argv[1]
    input_path = sys.argv[2]
    output_path = sys.argv[3]
    report_path = sys.argv[4]

    spark = (
        SparkSession.builder
        .appName("P02-T05-Partition-Bench-%s" % label)
        .getOrCreate()
    )
    sc = spark.sparkContext
    app_id = sc.applicationId
    print("=== APPLICATION ID ===", app_id)

    shuffle_parts = spark.conf.get("spark.sql.shuffle.partitions")
    exec_cores = sc._conf.get("spark.executor.cores", "default")
    exec_mem = sc._conf.get("spark.executor.memory", "default")

    # 1. 计时核心聚合作业（读取 -> 分组聚合 -> 写回 HDFS）
    t0 = time.time()

    df = spark.read.parquet(input_path)
    input_partitions = df.rdd.getNumPartitions()

    elapsed_col = (
        F.col("elapsed_seconds").cast("double")
        if "elapsed_seconds" in df.columns
        else F.col("duration_sec").cast("double")
    )

    agg_df = (
        df.groupBy("repo_id", "event", "conclusion")
        .agg(
            F.count("*").alias("run_cnt"),
            F.sum(elapsed_col).alias("sum_elapsed_seconds"),
            F.sum("run_attempts").alias("sum_run_attempts"),
            F.sum(F.when(F.col("log_file_count") > 0, 1).otherwise(0)).alias("nonempty_log_runs"),
            F.sum("log_file_count").alias("sum_log_files"),
        )
        .withColumn("sum_duration_sec", F.col("sum_elapsed_seconds").cast("long"))
    )
    output_partitions = agg_df.rdd.getNumPartitions()
    agg_df.write.mode("overwrite").parquet(output_path)

    job_wall_time_sec = round(time.time() - t0, 3)

    # 2. 在 spark.stop() 之前抓取 Spark Web UI REST API 指标（记录任务累计执行耗时 executorRunTime）
    opt_ui = sc._jsc.sc().uiWebUrl()
    ui_url = opt_ui.get() if opt_ui.isDefined() else "http://127.0.0.1:4040"
    stages_raw = fetch_spark_rest(ui_url, app_id, "stages")
    executors_raw = fetch_spark_rest(ui_url, app_id, "executors")

    stages_ok = isinstance(stages_raw, list)
    executors_ok = isinstance(executors_raw, list)

    stage_summaries = None
    if stages_ok:
        stage_summaries = []
        totals = _zero_totals()
        completed = [s for s in stages_raw if s.get("status") == "COMPLETE"]
        completed.sort(key=lambda s: s.get("stageId", 0))
        for s in completed:
            item = {
                "stage_id": s.get("stageId"),
                "name": s.get("name"),
                "num_tasks": int(s.get("numCompleteTasks", 0)),
                "task_cumulative_execution_time_ms": int(s.get("executorRunTime", 0)),
                "executor_run_time_ms": int(s.get("executorRunTime", 0)),
                "input_bytes": int(s.get("inputBytes", 0)),
                "output_bytes": int(s.get("outputBytes", 0)),
                "shuffle_read_bytes": int(s.get("shuffleReadBytes", 0)),
                "shuffle_read_records": int(s.get("shuffleReadRecords", 0)),
                "shuffle_write_bytes": int(s.get("shuffleWriteBytes", 0)),
                "shuffle_write_records": int(s.get("shuffleWriteRecords", 0)),
                "memory_bytes_spilled": int(s.get("memoryBytesSpilled", 0)),
                "disk_bytes_spilled": int(s.get("diskBytesSpilled", 0)),
            }
            stage_summaries.append(item)
            totals["stage_count"] += 1
            totals["total_tasks"] += item["num_tasks"]
            totals["executor_run_time_ms"] += item["executor_run_time_ms"]
            totals["input_bytes"] += item["input_bytes"]
            totals["input_records"] += int(s.get("inputRecords", 0))
            totals["output_bytes"] += item["output_bytes"]
            totals["output_records"] += int(s.get("outputRecords", 0))
            totals["shuffle_read_bytes"] += item["shuffle_read_bytes"]
            totals["shuffle_read_records"] += item["shuffle_read_records"]
            totals["shuffle_write_bytes"] += item["shuffle_write_bytes"]
            totals["shuffle_write_records"] += item["shuffle_write_records"]
            totals["memory_bytes_spilled"] += item["memory_bytes_spilled"]
            totals["disk_bytes_spilled"] += item["disk_bytes_spilled"]
    else:
        totals = _null_totals()

    worker_executors = None
    if executors_ok:
        worker_executors = []
        for ex in executors_raw:
            if ex.get("id") != "driver":
                worker_executors.append({
                    "id": ex.get("id"),
                    "host_port": ex.get("hostPort"),
                    "total_cores": ex.get("totalCores"),
                    "max_memory": ex.get("maxMemory"),
                    "completed_tasks": ex.get("completedTasks"),
                })

    evidence_complete, evidence_breakdown, telemetry_errors = evaluate_t05_evidence(
        stages_raw, executors_raw
    )

    # 3. 回读 HDFS 聚合结果，计算确定性校验和
    written_df = spark.read.parquet(output_path)
    ordered_rows = (
        written_df
        .orderBy("repo_id", "event", "conclusion")
        .collect()
    )

    h = hashlib.sha256()
    sum_run_cnt = 0
    sum_elapsed_seconds = 0.0
    sum_duration_sec = 0
    sum_nonempty_log_runs = 0
    for r in ordered_rows:
        r_elapsed = r["sum_elapsed_seconds"]
        r_dur = r["sum_duration_sec"]
        line = u"%s|%s|%s|%s|%s|%s|%s|%s\n" % (
            r["repo_id"], r["event"], r["conclusion"],
            r["run_cnt"], r_elapsed, r["sum_run_attempts"],
            r["nonempty_log_runs"], r["sum_log_files"],
        )
        h.update(line.encode("utf-8"))
        sum_run_cnt += int(r["run_cnt"])
        if r_elapsed is not None:
            sum_elapsed_seconds += float(r_elapsed)
        if r_dur is not None:
            sum_duration_sec += int(r_dur)
        sum_nonempty_log_runs += int(r["nonempty_log_runs"])

    report = {
        "label": label,
        "application_id": app_id,
        "input_path": input_path,
        "output_path": output_path,
        "evidence_complete": evidence_complete,
        "valid_for_t05_acceptance": evidence_complete,
        "evidence_breakdown": evidence_breakdown,
        "telemetry_status": {
            "stages_available": stages_ok,
            "executors_available": executors_ok,
            "errors": telemetry_errors,
        },
        "resources": {
            "executor_cores_conf": exec_cores,
            "executor_memory_conf": exec_mem,
            "worker_executors": worker_executors,
        },
        "partitions": {
            "spark_sql_shuffle_partitions": int(shuffle_parts),
            "input_partitions": int(input_partitions),
            "output_partitions": int(output_partitions),
        },
        "timing": {
            "job_wall_time_sec": job_wall_time_sec,
            "task_cumulative_execution_time_ms": totals["executor_run_time_ms"],
            "sum_executor_run_time_ms": totals["executor_run_time_ms"],
        },
        "output_verification": {
            "output_rows": len(ordered_rows),
            "sum_run_cnt": sum_run_cnt,
            "sum_elapsed_seconds": round(sum_elapsed_seconds, 6),
            "sum_duration_sec": sum_duration_sec,
            "sum_nonempty_log_runs": sum_nonempty_log_runs,
            "output_content_sha256": h.hexdigest(),
        },
        "stage_and_shuffle_totals": totals,
        "stages": stage_summaries,
    }

    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, sort_keys=True)

    print("=== P02-T05 BENCHMARK REPORT (%s) ===" % label)
    print(json.dumps({
        "label": label,
        "application_id": app_id,
        "evidence_complete": evidence_complete,
        "shuffle_partitions": int(shuffle_parts),
        "job_wall_time_sec": job_wall_time_sec,
        "output_rows": len(ordered_rows),
        "output_content_sha256": report["output_verification"]["output_content_sha256"],
        "stage_and_shuffle_totals": totals,
        "telemetry_errors": telemetry_errors,
    }, indent=2, sort_keys=True))

    spark.stop()


if __name__ == "__main__":
    main()