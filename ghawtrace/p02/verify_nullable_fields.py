# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import sys
import json
import glob
import tempfile
import shutil

try:
    import importlib.util
    def load_module(name, path):
        spec = importlib.util.spec_from_file_location(name, path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod
        spec.loader.exec_module(mod)
        return mod
except ImportError:
    import imp
    def load_module(name, path):
        return imp.load_source(name, path)

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CLEAN = os.path.join(HERE, "clean_runs.py")
clean_runs_path = os.environ.get("CLEAN_RUNS_PATH") or DEFAULT_CLEAN
if not os.path.isfile(clean_runs_path):
    print("ERROR: clean_runs.py not found next to this script")
    sys.exit(2)

print("[setup] loading clean_runs from:", clean_runs_path)
clean_mod = load_module("clean_runs", clean_runs_path)

from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .master("local[1]")
         .appName("verify-nullable-fields")
         .getOrCreate())

rows = [
    (1, 10, None, ".github/workflows/a.yml", "2026-01-01T00:00:00Z", "2026-01-01T00:00:01Z",
     "push", "completed", "success", 1, 0, 0, 0),
    (2, 10, 100, ".github/workflows/a.yml", "2026-01-01T00:01:00Z", "2026-01-01T00:01:02Z",
     "push", None, None, 1, 0, 0, 0),
    (3, 10, 100, None, "2026-01-01T00:02:00Z", "2026-01-01T00:02:01Z",
     "push", "completed", "success", 1, 0, 0, 0),
    (4, 10, 100, ".github/workflows/b.yml", "2026-01-01T00:03:00Z", "2026-01-01T00:03:01Z",
     "push", "completed", "failure", 1, 0, 0, 0),
]

in_df = spark.createDataFrame(rows, clean_mod.P01_RUN_COLUMNS)
tmp = tempfile.mkdtemp(prefix="verify_nullable_")
in_dir_local = os.path.join(tmp, "in_parquet")
out_dir_local = os.path.join(tmp, "out_clean")
rep_path_local = os.path.join(tmp, "report.json")

in_dir_spark = "file://" + in_dir_local
out_dir_spark = "file://" + out_dir_local

try:
    in_df.write.mode("overwrite").parquet(in_dir_spark)

    # 关键：临时禁用 spark.stop，防止 clean_mod.main() 停掉共享 SparkContext
    orig_stop = spark.stop
    spark.stop = lambda: None

    orig_argv = sys.argv
    try:
        sys.argv = ["clean_runs.py", in_dir_spark, out_dir_spark, rep_path_local]
        clean_mod.main()
    finally:
        sys.argv = orig_argv
        spark.stop = orig_stop

    with open(rep_path_local) as f:
        rep = json.load(f)

    # 打印实际目录结构，方便调试
    print("[debug] out_dir tree:")
    for root, dirs, files in os.walk(out_dir_local):
        rel = os.path.relpath(root, out_dir_local)
        print("  ", rel, "dirs=", dirs[:5], "files=", files[:5])

    def resolve_clean_dir(kind):
        cand_current = os.path.join(out_dir_local, "current", kind)
        if os.path.isdir(cand_current):
            return cand_current
        batches = sorted(glob.glob(os.path.join(out_dir_local, "batches", "*", kind)))
        if batches:
            return batches[-1]
        return None

    clean_dir = resolve_clean_dir("runs_clean")
    quarantine_dir = resolve_clean_dir("runs_quarantine")
    print("[debug] clean_dir:", clean_dir)
    print("[debug] quarantine_dir:", quarantine_dir)
    if not clean_dir or not quarantine_dir:
        print("ERROR: cannot locate output dirs")
        sys.exit(3)

    clean_df = spark.read.parquet("file://" + clean_dir)
    quarantine_df = spark.read.parquet("file://" + quarantine_dir)

    clean_ids = sorted([r["run_id"] for r in clean_df.collect()])
    q_rows = quarantine_df.collect()
    q_ids = sorted([r["run_id"] for r in q_rows])
    q_reasons = sorted(set([r["quarantine_reason"] for r in q_rows]))

    print("=" * 60)
    print("[report] retained_rows:", rep["retained_rows"])
    print("[report] total_quarantined_rows:", rep["total_quarantined_rows"])
    print("[report] quarantine_breakdown:", rep["mutually_exclusive_quarantine_breakdown"])
    print("[report] unfinished_by_status:", rep["retained_unfinished_runs"]["by_status"])
    print("[report] null_status_count:", rep["independent_rule_hit_counts"]["null_status"])
    print("[report] null_conclusion_count:", rep["independent_rule_hit_counts"]["null_conclusion"])
    print("[clean] run_ids:", clean_ids)
    print("[quarantine] run_ids:", q_ids)
    print("[quarantine] reasons:", q_reasons)

    ok = True
    checks = [
        ("workflow_id null retained", 1 in clean_ids),
        ("status null retained as activity", 2 in clean_ids),
        ("workflow_path null quarantined", 3 in q_ids),
        ("normal run retained", 4 in clean_ids),
        ("retained_rows == 3", rep["retained_rows"] == 3),
        ("quarantined_rows == 1", rep["total_quarantined_rows"] == 1),
        ("INVALID_WORKFLOW_PATH == 1",
         rep["mutually_exclusive_quarantine_breakdown"].get("INVALID_WORKFLOW_PATH") == 1),
        ("unfinished <NULL> == 1", rep["retained_unfinished_runs"]["by_status"].get("<NULL>") == 1),
        ("null_status_count == 1", rep["independent_rule_hit_counts"]["null_status"] == 1),
        ("null_conclusion_count == 1", rep["independent_rule_hit_counts"]["null_conclusion"] == 1),
    ]
    for name, passed in checks:
        print(("PASS" if passed else "FAIL"), "-", name)
        if not passed:
            ok = False
    print("=" * 60)
    print("RESULT:", "ALL PASS" if ok else "SOME FAILED")
    sys.exit(0 if ok else 1)
finally:
    shutil.rmtree(tmp, True)
    spark.stop()
