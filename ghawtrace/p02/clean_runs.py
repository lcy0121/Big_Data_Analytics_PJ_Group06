# -*- coding: utf-8 -*-
from __future__ import division, print_function

import json
import re
import sys
import uuid
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

P01_RUN_COLUMNS = [
    "run_id",
    "repo_id",
    "workflow_id",
    "workflow_path",
    "created_at",
    "updated_at",
    "event",
    "status",
    "conclusion",
    "run_attempts",
    "log_archive_count",
    "log_file_count",
    "empty_archive_count",
]

UTC_PATTERN = re.compile(
    r"^([0-9]{4})-(0[1-9]|1[0-2])-([0-9]{2})"
    r"[T ]([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])"
    r"(?:\.([0-9]{1,9}))?(Z|\+00:00)$"
)


def _days_before_year(year):
    y = year - 1
    return y * 365 + y // 4 - y // 100 + y // 400


def _is_leap(year):
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)


_MDAYS_COMMON = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
_MDAYS_LEAP = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


class InjectedFault(IOError):
    def __init__(self, stage_name):
        self.stage_name = stage_name
        super(InjectedFault, self).__init__("Injected fault at stage: %s" % stage_name)

def _maybe_inject_fault(stage_name):
    if os.environ.get("P02_FAULT_INJECT") == stage_name:
        raise InjectedFault(stage_name)


def parse_utc_timestamp_py(raw_val):
    """
    Parse ISO-8601 UTC timestamp string preserving microsecond precision.
    Returns tuple: (utc_micros_int_or_None, epoch_seconds_float_or_None)
    """
    if raw_val is None:
        return (None, None)
    s = str(raw_val).strip()
    if not s:
        return (None, None)

    m = UTC_PATTERN.match(s)
    if not m:
        return (None, None)

    year = int(m.group(1))
    month = int(m.group(2))
    day = int(m.group(3))
    hour = int(m.group(4))
    minute = int(m.group(5))
    second = int(m.group(6))
    frac_str = m.group(7) or ""

    mdays = _MDAYS_LEAP if _is_leap(year) else _MDAYS_COMMON
    if day < 1 or day > mdays[month - 1]:
        return (None, None)

    days = (_days_before_year(year) - _days_before_year(1970)) + sum(mdays[: month - 1]) + (day - 1)
    sec_epoch = ((days * 24 + hour) * 60 + minute) * 60 + second

    if frac_str:
        frac_padded = (frac_str + "000000000")[:9]
        nanos = int(frac_padded)
        micros = nanos // 1000
        frac_sec = nanos / 1e9
    else:
        micros = 0
        frac_sec = 0.0

    utc_micros = int(sec_epoch) * 1000000 + int(micros)
    return (utc_micros, float(sec_epoch) + frac_sec)


ts_struct_type = StructType([
    StructField("utc_micros", LongType(), nullable=True),
    StructField("epoch_sec", DoubleType(), nullable=True),
])
parse_utc_udf = F.udf(parse_utc_timestamp_py, ts_struct_type)


def _micros_to_timestamp_col(micros_col_name):
    mc = F.col(micros_col_name).cast("long")
    sec_part = F.floor(mc / F.lit(1000000)).cast("long")
    us_part = (mc - sec_part * F.lit(1000000)).cast("long")
    # 格式化为 YYYY-MM-DD HH:MM:SS.ffffff 后转为 TimestampType，保留完整微秒
    formatted_str = F.concat(
        F.from_unixtime(sec_part, "yyyy-MM-dd HH:mm:ss"),
        F.lit("."),
        F.format_string("%06d", us_part),
    )
    return F.when(mc.isNotNull(), formatted_str.cast(TimestampType())).otherwise(
        F.lit(None).cast(TimestampType())
    )


def _is_non_empty_str_col(col_name):
    c = F.col(col_name)
    return c.isNotNull() & (F.length(F.trim(c.cast(StringType()))) > 0)


def _is_positive_int_col(col_name):
    c = F.col(col_name).cast("long")
    return c.isNotNull() & (c > 0)


def _hadoop_fs(spark, path_str):
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    uri = jvm.java.net.URI(path_str)
    return jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf), jvm.org.apache.hadoop.fs.Path(path_str)


def _delete_if_exists(spark, path_str):
    fs, hpath = _hadoop_fs(spark, path_str)
    if fs.exists(hpath):
        fs.delete(hpath, True)


def _write_success_marker(spark, batch_dir, metadata_dict):
    fs, hpath = _hadoop_fs(spark, batch_dir.rstrip("/") + "/_SUCCESS")
    out_stream = fs.create(hpath, True)
    payload = json.dumps(metadata_dict, indent=2, sort_keys=True)
    if not isinstance(payload, bytes):
        payload = payload.encode("utf-8")
    out_stream.write(bytearray(payload))
    out_stream.close()


def main():
    if len(sys.argv) < 3:
        print("Usage: clean_runs.py <input_parquet_dir> <output_base_dir> [report_local_json]")
        sys.exit(1)

    input_path = sys.argv[1]
    output_base = sys.argv[2].rstrip("/")
    report_local_path = sys.argv[3] if len(sys.argv) >= 4 else "/tmp/p02_t03_report.json"

    spark = (
        SparkSession.builder
        .appName("P02-T03-Clean-Runs")
        .getOrCreate()
    )
    sc = spark.sparkContext
    print("=== APPLICATION ID ===", sc.applicationId)

    raw_df = spark.read.parquet(input_path)
    missing_cols = [c for c in P01_RUN_COLUMNS if c not in raw_df.columns]
    if missing_cols:
        raise ValueError("Input parquet missing P01 required columns: %s" % missing_cols)

    df = raw_df.select(*P01_RUN_COLUMNS)

    dedup_win = Window.partitionBy("run_id").orderBy(
        F.col("updated_at").desc(),
        F.col("run_attempts").desc(),
        F.col("created_at").asc(),
        F.col("repo_id").asc(),
        F.col("workflow_id").asc(),
        F.col("workflow_path").asc(),
        F.col("status").asc(),
        F.col("conclusion").asc(),
        F.col("event").asc(),
        F.col("log_archive_count").desc(),
        F.col("log_file_count").desc(),
        F.col("empty_archive_count").asc(),
    )

    df_parsed = (
        df
        .withColumn("_parsed_created", parse_utc_udf(F.col("created_at")))
        .withColumn("_parsed_updated", parse_utc_udf(F.col("updated_at")))
        .withColumn("_created_micros", F.col("_parsed_created.utc_micros").cast(LongType()))
        .withColumn("_updated_micros", F.col("_parsed_updated.utc_micros").cast(LongType()))
        .withColumn("created_at_utc", _micros_to_timestamp_col("_created_micros"))
        .withColumn("updated_at_utc", _micros_to_timestamp_col("_updated_micros"))
        .withColumn("_created_epoch", F.col("_parsed_created.epoch_sec").cast(DoubleType()))
        .withColumn("_updated_epoch", F.col("_parsed_updated.epoch_sec").cast(DoubleType()))
    )

    df_checked = (
        df_parsed
        .withColumn("_pk_invalid", ~_is_positive_int_col("run_id"))
        .withColumn("_repo_invalid", ~_is_positive_int_col("repo_id"))
        .withColumn("_workflow_path_invalid", ~_is_non_empty_str_col("workflow_path"))
        .withColumn("_created_utc_invalid", F.col("_created_epoch").isNull())
        .withColumn("_updated_raw_present", _is_non_empty_str_col("updated_at"))
        .withColumn("_updated_utc_invalid", F.col("_updated_raw_present") & F.col("_updated_epoch").isNull())
        .withColumn("_updated_missing_or_invalid", F.col("_updated_epoch").isNull())
        .withColumn(
            "_rn",
            F.when(F.col("_pk_invalid"), F.lit(1)).otherwise(F.row_number().over(dedup_win)),
        )
        .withColumn("_pk_dup", (~F.col("_pk_invalid")) & (F.col("_rn") > 1))
        .withColumn(
            "_raw_delta_sec",
            F.when(
                (~F.col("_created_utc_invalid")) & (~F.col("_updated_missing_or_invalid")),
                F.col("_updated_epoch") - F.col("_created_epoch"),
            ).otherwise(F.lit(None).cast(DoubleType())),
        )
        .withColumn(
            "_time_inversion",
            F.col("_raw_delta_sec").isNotNull() & (F.col("_raw_delta_sec") < F.lit(0.0)),
        )
        .withColumn(
            "elapsed_seconds",
            F.when(
                F.col("_raw_delta_sec").isNotNull() & (~F.col("_time_inversion")),
                F.col("_raw_delta_sec"),
            ).otherwise(F.lit(None).cast(DoubleType())),
        )
        .withColumn("duration_sec", F.col("elapsed_seconds").cast("long"))
        .withColumn("_status_null", ~_is_non_empty_str_col("status"))
        .withColumn("_conclusion_null", ~_is_non_empty_str_col("conclusion"))
    )

    df_tagged = df_checked.withColumn(
        "quarantine_reason",
        F.when(F.col("_pk_invalid"), F.lit("INVALID_PRIMARY_KEY"))
         .when(F.col("_pk_dup"), F.lit("DUPLICATE_PRIMARY_KEY"))
         .when(F.col("_repo_invalid"), F.lit("INVALID_REPO_ID"))
         .when(F.col("_workflow_path_invalid"), F.lit("INVALID_WORKFLOW_PATH"))
         .when(F.col("_created_utc_invalid"), F.lit("INVALID_CREATED_AT_UTC"))
         .otherwise(F.lit("VALID")),
    ).persist()

    summary_row = df_tagged.agg(
        F.count("*").alias("total_input_rows"),
        F.countDistinct("run_id").alias("distinct_run_ids"),
        F.sum(F.when(F.col("_pk_invalid"), 1).otherwise(0)).alias("ind_invalid_pk"),
        F.sum(F.when(F.col("_pk_dup"), 1).otherwise(0)).alias("ind_duplicate_pk"),
        F.sum(F.when(F.col("_repo_invalid"), 1).otherwise(0)).alias("ind_invalid_repo_id"),
        F.sum(F.when(F.col("_repo_invalid") | F.col("_workflow_path_invalid"), 1).otherwise(0)).alias("ind_invalid_workflow_key"),
        F.sum(F.when(F.col("_workflow_path_invalid"), 1).otherwise(0)).alias("ind_invalid_workflow_path"),
        F.sum(F.when(F.col("_created_utc_invalid"), 1).otherwise(0)).alias("ind_invalid_created_utc"),
        F.sum(F.when(F.col("_updated_missing_or_invalid"), 1).otherwise(0)).alias("missing_or_invalid_updated_at"),
        F.sum(F.when(F.col("_updated_utc_invalid"), 1).otherwise(0)).alias("malformed_updated_at_when_present"),
        F.sum(F.when(F.col("_time_inversion"), 1).otherwise(0)).alias("time_inversion_count"),
        F.sum(F.when(F.col("_status_null"), 1).otherwise(0)).alias("ind_null_status"),
        F.sum(F.when(F.col("_conclusion_null"), 1).otherwise(0)).alias("ind_null_conclusion"),
    ).collect()[0]

    reason_rows = (
        df_tagged.groupBy("quarantine_reason")
        .count()
        .orderBy("quarantine_reason")
        .collect()
    )
    reason_counts = dict((r["quarantine_reason"], int(r["count"])) for r in reason_rows)

    total_input = int(summary_row["total_input_rows"])
    distinct_runs = int(summary_row["distinct_run_ids"])
    retained_count = int(reason_counts.get("VALID", 0))
    quarantined_breakdown = dict((k, int(v)) for k, v in reason_counts.items() if k != "VALID")
    total_quarantined = int(sum(quarantined_breakdown.values()))
    reconciled = (retained_count + total_quarantined == total_input)

    unfinished_df = df_tagged.filter((F.col("quarantine_reason") == "VALID") & F.col("_conclusion_null"))
    unfinished_by_status_rows = (
        unfinished_df
        .groupBy(F.coalesce(F.col("status"), F.lit("<NULL>")).alias("status_val"))
        .count()
        .orderBy(F.desc("count"))
        .collect()
    )
    unfinished_by_status = dict((r["status_val"], int(r["count"])) for r in unfinished_by_status_rows)
    unfinished_workflows_count = int(
        unfinished_df.select("repo_id", "workflow_path").distinct().count()
    )

    output_cols = P01_RUN_COLUMNS + [
        "created_at_utc",
        "updated_at_utc",
        "elapsed_seconds",
        "duration_sec",
    ]
    clean_df = df_tagged.filter(F.col("quarantine_reason") == "VALID").select(*output_cols)
    quarantine_df = df_tagged.filter(F.col("quarantine_reason") != "VALID").select(
        *(output_cols + ["quarantine_reason"])
    )

    # 先写入独立批次暂存目录，回读核验通过后再发布到最终目录与 _SUCCESS 标记
    batch_token = "%s_%s" % (sc.applicationId, uuid.uuid4().hex[:8])
    batch_dir = "%s/batches/%s" % (output_base, batch_token)
    batch_clean_path = "%s/runs_clean" % batch_dir
    batch_quarantine_path = "%s/runs_quarantine" % batch_dir

    current_link_path = "%s/current" % output_base
    final_clean_path = "%s/runs_clean" % current_link_path
    final_quarantine_path = "%s/runs_quarantine" % current_link_path

    report_clean_path = "%s/runs_clean" % batch_dir
    report_quarantine_path = "%s/runs_quarantine" % batch_dir


    backup_current = None
    try:
        _delete_if_exists(spark, batch_dir)
        clean_df.write.mode("overwrite").parquet(batch_clean_path)
        _maybe_inject_fault("after_batch_clean_write")
        quarantine_df.write.mode("overwrite").parquet(batch_quarantine_path)

        verified_clean_df = spark.read.parquet(batch_clean_path)
        verified_quarantine_df = spark.read.parquet(batch_quarantine_path)

        clean_fp = verified_clean_df.agg(
            F.count("*").alias("written_clean_rows"),
            F.min("run_id").alias("min_run_id"),
            F.max("run_id").alias("max_run_id"),
            F.sum("elapsed_seconds").alias("sum_elapsed_seconds"),
            F.sum("duration_sec").alias("sum_duration_sec"),
            F.sum(F.when(F.col("elapsed_seconds").isNull(), 1).otherwise(0)).alias("null_elapsed_seconds_rows"),
        ).collect()[0]

        written_clean_rows = int(clean_fp["written_clean_rows"] or 0)
        written_quarantine_rows = int(verified_quarantine_df.count())

        if written_clean_rows != retained_count or written_quarantine_rows != total_quarantined:
            raise RuntimeError(
                "Batch readback count mismatch: clean=%d (expected %d), quarantine=%d (expected %d)"
                % (written_clean_rows, retained_count, written_quarantine_rows, total_quarantined)
            )

        publish_meta = {
            "application_id": sc.applicationId,
            "batch_token": batch_token,
            "batch_dir": batch_dir,
            "total_input_rows": total_input,
            "retained_rows": written_clean_rows,
            "quarantined_rows": written_quarantine_rows,
            "reconciled_equals_input": reconciled,
        }
        _write_success_marker(spark, batch_dir, publish_meta)
        _maybe_inject_fault("after_batch_success") 

        # 批次完整且校验通过后，再将不可变批次发布到 current 目录
        stage_current = "%s/.current_staging_%s" % (output_base, batch_token)
        _delete_if_exists(spark, stage_current)
        spark.read.parquet(batch_clean_path).write.mode("overwrite").parquet("%s/runs_clean" % stage_current)
        spark.read.parquet(batch_quarantine_path).write.mode("overwrite").parquet("%s/runs_quarantine" % stage_current)
        _write_success_marker(spark, stage_current, publish_meta)
        _maybe_inject_fault("after_stage_success")

        backup_current = "%s/.current_prev_%s" % (output_base, batch_token)
        fs_cur, h_cur = _hadoop_fs(spark, current_link_path)
        fs_stg, h_stg = _hadoop_fs(spark, stage_current)
        fs_bak, h_bak = _hadoop_fs(spark, backup_current)
        if fs_cur.exists(h_cur):
            fs_cur.rename(h_cur, h_bak)
        _maybe_inject_fault("after_rename_current_to_backup")
        if not fs_stg.rename(h_stg, h_cur):
            if fs_bak.exists(h_bak):
                fs_bak.rename(h_bak, h_cur)
            raise RuntimeError("Failed to promote staged batch %s to current" % batch_token)
        if fs_bak.exists(h_bak):
            fs_bak.delete(h_bak, True)
    except Exception:
        # 回滚：若 current 已 rename 到 backup 但 staging 未提升，恢复旧 current
        if backup_current is not None:
            try:
                fs_cur_rb, h_cur_rb = _hadoop_fs(spark, current_link_path)
                fs_bak_rb, h_bak_rb = _hadoop_fs(spark, backup_current)
                if (not fs_cur_rb.exists(h_cur_rb)) and fs_bak_rb.exists(h_bak_rb):
                    fs_bak_rb.rename(h_bak_rb, h_cur_rb)
            except Exception:
                pass
        df_tagged.unpersist()
        spark.stop()
        raise

    report = {
        "application_id": sc.applicationId,
        "batch_token": batch_token,
        "input_path": input_path,
        "clean_path": report_clean_path,
        "quarantine_path": report_quarantine_path,
        "current_clean_path": final_clean_path,
        "current_quarantine_path": final_quarantine_path,
        "total_input_rows": total_input,
        "distinct_run_ids": distinct_runs,
        "retained_rows": retained_count,
        "total_quarantined_rows": total_quarantined,
        "reconciled_equals_input": reconciled,
        "mutually_exclusive_quarantine_breakdown": quarantined_breakdown,
        "retained_unfinished_runs": {
            "total_rows": int(sum(unfinished_by_status.values())),
            "distinct_workflows": unfinished_workflows_count,
            "by_status": unfinished_by_status,
        },
        "quality_statistics": {
            "missing_or_invalid_updated_at": int(summary_row["missing_or_invalid_updated_at"] or 0),
            "malformed_updated_at_when_present": int(summary_row["malformed_updated_at_when_present"] or 0),
            "time_inversion_count": int(summary_row["time_inversion_count"] or 0),
            "null_elapsed_seconds_rows": int(clean_fp["null_elapsed_seconds_rows"] or 0),
        },
        "independent_rule_hit_counts": {
            "invalid_primary_key": int(summary_row["ind_invalid_pk"] or 0),
            "duplicate_primary_key": int(summary_row["ind_duplicate_pk"] or 0),
            "invalid_repo_id": int(summary_row["ind_invalid_repo_id"] or 0),
            "invalid_workflow_key": int(summary_row["ind_invalid_workflow_key"] or 0),
            "invalid_workflow_path": int(summary_row["ind_invalid_workflow_path"] or 0),
            "invalid_created_utc": int(summary_row["ind_invalid_created_utc"] or 0),
            "null_status": int(summary_row["ind_null_status"] or 0),
            "null_conclusion": int(summary_row["ind_null_conclusion"] or 0),
        },
        "clean_output_fingerprint": {
            "written_clean_rows": written_clean_rows,
            "written_quarantine_rows": written_quarantine_rows,
            "min_run_id": int(clean_fp["min_run_id"]) if clean_fp["min_run_id"] is not None else None,
            "max_run_id": int(clean_fp["max_run_id"]) if clean_fp["max_run_id"] is not None else None,
            "sum_elapsed_seconds": (
                float(clean_fp["sum_elapsed_seconds"])
                if clean_fp["sum_elapsed_seconds"] is not None
                else None
            ),
            "sum_duration_sec": (
                int(clean_fp["sum_duration_sec"])
                if clean_fp["sum_duration_sec"] is not None
                else None
            ),
        },
    }

    with open(report_local_path, "w") as f:
        json.dump(report, f, indent=2, sort_keys=True)

    print("=== P02-T03 RECONCILIATION REPORT ===")
    print(json.dumps(report, indent=2, sort_keys=True))

    df_tagged.unpersist()
    spark.stop()

    if not reconciled:
        raise RuntimeError("Reconciliation failed: retained + quarantined != total_input")


if __name__ == "__main__":
    main()