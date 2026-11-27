# -*- coding: utf-8 -*-
"""Node-only acceptance script: microsecond timestamp persistence (review item 3).

Writes two pairs of sub-second timestamps through clean_runs.py, then reads
back from the report-specified Parquet and asserts created_at_utc /
updated_at_utc microsecond fields and elapsed_seconds.

Requires PySpark 2.x. Run from repo root:

    spark-submit --master local[1] \
      --py-files ghawtrace/p02/clean_runs.py \
      ghawtrace/p02/verify_time_precision.py

Env:
    CLEAN_RUNS_PATH  override; default: ./clean_runs.py (same dir as this file)
"""
from __future__ import print_function
import json
import os
import shutil
import sys
import tempfile

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
    print("ERROR: clean_runs.py not found at:", clean_runs_path)
    print("Place clean_runs.py next to this script, or set CLEAN_RUNS_PATH.")
    sys.exit(2)

print("[setup] script dir     :", HERE)
print("[setup] clean_runs path :", clean_runs_path)
clean_mod = load_module("clean_runs", clean_runs_path)

from pyspark.sql import SparkSession

ROWS = [
    # 跨秒小数：00:00:00.900001 -> 00:00:01.100002, elapsed ~= 0.200001
    (1, 10, 100, ".github/workflows/time-ok.yml",
     "2026-01-01T00:00:00.900001Z", "2026-01-01T00:00:01.100002Z",
     "push", "completed", "success", 1, 0, 0, 0),
    # 同秒倒序：00:00:05.500000 -> 00:00:05.200000, elapsed = None
    (2, 10, 100, ".github/workflows/time-inv.yml",
     "2026-01-01T00:00:05.500000Z", "2026-01-01T00:00:05.200000Z",
     "push", "completed", "success", 1, 0, 0, 0),
]

def _us(dt):
    return None if dt is None else dt.microsecond


def main():
    spark = (SparkSession.builder
             .master("local[1]")
             .appName("verify-time-precision")
             .getOrCreate())

    tmp = tempfile.mkdtemp(prefix="verify_time_")
    in_dir = os.path.join(tmp, "in_parquet")
    out_dir = os.path.join(tmp, "out_clean")
    rep_path = os.path.join(tmp, "report.json")
    in_dir_uri = "file://" + in_dir
    out_dir_uri = "file://" + out_dir

    try:
        in_df = spark.createDataFrame(ROWS, clean_mod.P01_RUN_COLUMNS)
        in_df.write.mode("overwrite").parquet(in_dir_uri)

        orig_argv = sys.argv
        orig_stop = spark.stop
        spark.stop = lambda: None
        try:
            sys.argv = ["clean_runs.py", in_dir_uri, out_dir_uri, rep_path]
            clean_mod.main()
        finally:
            sys.argv = orig_argv
            spark.stop = orig_stop

        with open(rep_path) as f:
            rep = json.load(f)

        print("=" * 60)
        print("[time] report clean_path:", rep["clean_path"])

        clean_df = spark.read.parquet(rep["clean_path"])
        rows_out = {r["run_id"]: r for r in clean_df.collect()}

        r1 = rows_out[1]
        r2 = rows_out[2]
        us_c1, us_u1, el1 = _us(r1["created_at_utc"]), _us(r1["updated_at_utc"]), r1["elapsed_seconds"]
        us_c2, us_u2, el2 = _us(r2["created_at_utc"]), _us(r2["updated_at_utc"]), r2["elapsed_seconds"]

        print("[time] row1 created_at_utc.us = %s  (expected 900001)" % us_c1)
        print("[time] row1 updated_at_utc.us = %s  (expected 100002)" % us_u1)
        print("[time] row1 elapsed_seconds   = %s  (expected ~0.200001)" % el1)
        print("[time] row2 created_at_utc.us = %s  (expected 500000)" % us_c2)
        print("[time] row2 updated_at_utc.us = %s  (expected 200000)" % us_u2)
        print("[time] row2 elapsed_seconds   = %s  (expected None)" % el2)

        checks = [
            ("row1 created_at_utc.us == 900001", us_c1 == 900001),
            ("row1 updated_at_utc.us == 100002", us_u1 == 100002),
            ("row1 elapsed_seconds ~= 0.200001",
             el1 is not None and abs(el1 - 0.200001) < 1e-6),
            ("row2 created_at_utc.us == 500000", us_c2 == 500000),
            ("row2 updated_at_utc.us == 200000", us_u2 == 200000),
            ("row2 elapsed_seconds is None", el2 is None),
        ]
        ok = True
        print("=" * 60)
        for name, passed in checks:
            print(("PASS" if passed else "FAIL"), "-", name)
            if not passed:
                ok = False
        print("=" * 60)
        print("RESULT:", "ALL PASS" if ok else "SOME FAILED")
        return 0 if ok else 1
    finally:
        shutil.rmtree(tmp, True)
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())