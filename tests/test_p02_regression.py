# -*- coding: utf-8 -*-
# usage: $env:PYTHONUTF8="1"
# $env:PYTHONIOENCODING="utf-8"
# python -m unittest discover -s tests -v
import importlib
import importlib.machinery
import importlib.util
import inspect
import json
import os
import sys
import tempfile
import types
import unittest
import urllib.error
import urllib.request
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
P02_DIR = REPO_ROOT / "ghawtrace" / "p02"

_COMPAT_SHIMS_INSTALLED = False
_USED_PYSPARK_STUB = False


def _install_compat_shims():
    global _COMPAT_SHIMS_INSTALLED, _USED_PYSPARK_STUB
    if _COMPAT_SHIMS_INSTALLED:
        return

    if "urllib2" not in sys.modules:
        try:
            import urllib2 as _real_urllib2  # noqa: F401
        except ImportError:
            urllib2_mod = types.ModuleType("urllib2")
            urllib2_mod.__spec__ = importlib.machinery.ModuleSpec("urllib2", loader=None)
            urllib2_mod.urlopen = urllib.request.urlopen
            urllib2_mod.Request = urllib.request.Request
            urllib2_mod.URLError = urllib.error.URLError
            urllib2_mod.HTTPError = urllib.error.HTTPError
            sys.modules["urllib2"] = urllib2_mod

    def _pyspark_is_usable():
        try:
            from pyspark.sql import SparkSession  # noqa: F401
            from pyspark.sql import functions as _F  # noqa: F401
            from pyspark.sql.types import (  # noqa: F401
                DoubleType, LongType, StringType,
                StructField, StructType, TimestampType,
            )
            from pyspark.sql.window import Window  # noqa: F401
            return True
        except Exception:
            return False

    has_real = _pyspark_is_usable()
    
    if not has_real:
        _USED_PYSPARK_STUB = True
        pyspark_mod = types.ModuleType("pyspark")
        pyspark_mod.__spec__ = importlib.machinery.ModuleSpec("pyspark", loader=None)
        pyspark_mod.__path__ = []

        sql_mod = types.ModuleType("pyspark.sql")
        sql_mod.__spec__ = importlib.machinery.ModuleSpec("pyspark.sql", loader=None)
        sql_mod.__path__ = []

        functions_mod = types.ModuleType("pyspark.sql.functions")
        functions_mod.__spec__ = importlib.machinery.ModuleSpec("pyspark.sql.functions", loader=None)

        types_mod = types.ModuleType("pyspark.sql.types")
        types_mod.__spec__ = importlib.machinery.ModuleSpec("pyspark.sql.types", loader=None)

        window_mod = types.ModuleType("pyspark.sql.window")
        window_mod.__spec__ = importlib.machinery.ModuleSpec("pyspark.sql.window", loader=None)

        class _DummySparkContext(object):
            appId = "local-stub-app"
            applicationId = "local-stub-app"

        class _DummyWriter(object):
            def mode(self, save_mode):
                return self

            def parquet(self, path):
                Path(path).mkdir(parents=True, exist_ok=True)

        class _DummyDataFrame(object):
            def __init__(self, data, schema=None):
                self._data = data
                self._schema = schema
                self.write = _DummyWriter()

        class _DummySparkSession(object):
            class builder(object):
                @classmethod
                def appName(cls, name):
                    return cls

                @classmethod
                def master(cls, url):
                    return cls

                @classmethod
                def config(cls, *args, **kwargs):
                    return cls

                @classmethod
                def enableHiveSupport(cls):
                    return cls

                @classmethod
                def getOrCreate(cls):
                    return _DummySparkSession()

            def __init__(self):
                self.sparkContext = _DummySparkContext()

            def createDataFrame(self, data, schema=None):
                return _DummyDataFrame(data, schema)

            def stop(self):
                pass

        class _DummyCol(object):
            def __init__(self, *args, **kwargs):
                pass

            def cast(self, *args, **kwargs):
                return self

            def desc(self):
                return self

            def asc(self):
                return self

        class _DummyWindowSpec(object):
            def partitionBy(self, *args, **kwargs):
                return self

            def orderBy(self, *args, **kwargs):
                return self

            def rowsBetween(self, *args, **kwargs):
                return self

            def rangeBetween(self, *args, **kwargs):
                return self

        class _DummyWindow(object):
            unboundedPreceding = -1
            unboundedFollowing = 1
            currentRow = 0

            @staticmethod
            def partitionBy(*args, **kwargs):
                return _DummyWindowSpec()

            @staticmethod
            def orderBy(*args, **kwargs):
                return _DummyWindowSpec()

            @staticmethod
            def rowsBetween(*args, **kwargs):
                return _DummyWindowSpec()

            @staticmethod
            def rangeBetween(*args, **kwargs):
                return _DummyWindowSpec()

        def _udf(fn, return_type=None):
            return fn

        sql_mod.SparkSession = _DummySparkSession
        sql_mod.functions = functions_mod
        sql_mod.Window = _DummyWindow
        window_mod.Window = _DummyWindow
        window_mod.WindowSpec = _DummyWindowSpec
        functions_mod.udf = _udf
        functions_mod.col = lambda *a, **kw: _DummyCol(*a, **kw)
        types_mod.DoubleType = lambda: "double"
        types_mod.LongType = lambda: "long"
        types_mod.StringType = lambda: "string"
        types_mod.TimestampType = lambda: "timestamp"
        types_mod.StructField = lambda name, typ, nullable=True, metadata=None: (name, typ)
        types_mod.StructType = lambda fields: list(fields)
        pyspark_mod.sql = sql_mod

        for mod_name, mod_obj in [
            ("pyspark", pyspark_mod),
            ("pyspark.sql", sql_mod),
            ("pyspark.sql.functions", functions_mod),
            ("pyspark.sql.types", types_mod),
            ("pyspark.sql.window", window_mod),
        ]:
            sys.modules[mod_name] = mod_obj

    _COMPAT_SHIMS_INSTALLED = True


def _load_p02_module(module_name, file_path):
    _install_compat_shims()
    spec = importlib.util.spec_from_file_location(module_name, str(file_path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


convert_mod = _load_p02_module("convert_for_spark2", P02_DIR / "convert_for_spark2.py")
clean_mod = _load_p02_module("clean_runs", P02_DIR / "clean_runs.py")
bench_mod = _load_p02_module("partition_bench", P02_DIR / "partition_bench.py")


def _make_sample_table(num_rows=2):
    cols = {
        "run_id": [101 + i for i in range(num_rows)],
        "repo_id": [1001 for _ in range(num_rows)],
        "workflow_id": [2001 for _ in range(num_rows)],
        "workflow_path": [".github/workflows/ci.yml" for _ in range(num_rows)],
        "created_at": ["2026-01-01T00:00:00.900Z" for _ in range(num_rows)],
        "updated_at": ["2026-01-01T00:00:01.100Z" for _ in range(num_rows)],
        "event": ["push" for _ in range(num_rows)],
        "status": ["completed" for _ in range(num_rows)],
        "conclusion": ["success" for _ in range(num_rows)],
        "run_attempts": [1 for _ in range(num_rows)],
        "log_archive_count": [1 for _ in range(num_rows)],
        "log_file_count": [2 for _ in range(num_rows)],
        "empty_archive_count": [0 for _ in range(num_rows)],
        "github_metadata_json": ['{"extra": true}' for _ in range(num_rows)],
        "head_sha": ["abc123" for _ in range(num_rows)],
    }
    return pa.table(cols)


def _run_convert_main(args):
    orig_argv = sys.argv
    arg_list = [str(a) for a in args]
    try:
        sys.argv = ["convert_for_spark2.py"] + arg_list
        params = list(inspect.signature(convert_mod.main).parameters.values())
        required_params = [
            p for p in params
            if p.default is inspect.Parameter.empty
            and p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        ]
        if len(required_params) >= 1:
            rc = convert_mod.main(arg_list)
        else:
            rc = convert_mod.main()
        return 0 if rc is None else int(rc)
    except SystemExit as exc:
        return 0 if exc.code in (None, 0) else int(exc.code)
    finally:
        sys.argv = orig_argv

def _p02_run_schema():
    """显式 PySpark StructType，对应 clean_mod.P01_RUN_COLUMNS。
    用于 workflow_id 等整列为 None 时 Spark 2.0 无法推断类型。"""
    from pyspark.sql.types import LongType, StringType, StructField, StructType
    return StructType([
        StructField("run_id", LongType(), True),
        StructField("repo_id", LongType(), True),
        StructField("workflow_id", LongType(), True),
        StructField("workflow_path", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("event", StringType(), True),
        StructField("status", StringType(), True),
        StructField("conclusion", StringType(), True),
        StructField("run_attempts", LongType(), True),
        StructField("log_archive_count", LongType(), True),
        StructField("log_file_count", LongType(), True),
        StructField("empty_archive_count", LongType(), True),
    ])


class TestConvertForSpark2Regression(unittest.TestCase):
    def test_rejects_same_input_and_output_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            work_dir = Path(tmp) / "shard_dir"
            work_dir.mkdir()
            pq.write_table(_make_sample_table(2), work_dir / "part-000.parquet")
            before_hash = convert_mod._sha256_file(work_dir / "part-000.parquet")

            rc = _run_convert_main([str(work_dir), str(work_dir)])
            self.assertNotEqual(rc, 0)
            self.assertEqual(convert_mod._sha256_file(work_dir / "part-000.parquet"), before_hash)

    def test_rejects_overlapping_input_output_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            in_dir = Path(tmp) / "input"
            out_dir = in_dir / "nested_output"
            in_dir.mkdir(parents=True)
            pq.write_table(_make_sample_table(1), in_dir / "part-000.parquet")

            rc = _run_convert_main([str(in_dir), str(out_dir)])
            self.assertNotEqual(rc, 0)

    def test_limit_one_isolated_batch_has_exact_single_shard_and_preserves_source_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            in_dir = Path(tmp) / "raw_shards"
            out_full = Path(tmp) / "out_full"
            out_sample = Path(tmp) / "out_sample_limit1"
            in_dir.mkdir()

            shard_a = in_dir / "shard_000.parquet"
            shard_b = in_dir / "shard_001.parquet"
            pq.write_table(_make_sample_table(2), shard_a)
            pq.write_table(_make_sample_table(3), shard_b)

            hash_a_before = convert_mod._sha256_file(shard_a)
            hash_b_before = convert_mod._sha256_file(shard_b)

            rc_full = _run_convert_main([str(in_dir), str(out_full)])
            self.assertEqual(rc_full, 0)
            self.assertEqual(
                sorted(p.name for p in out_full.glob("*.parquet")),
                ["shard_000.parquet", "shard_001.parquet"],
            )

            rc_sample = _run_convert_main([str(in_dir), str(out_sample), "1"])
            self.assertEqual(rc_sample, 0)
            out_sample_shards = sorted(p.name for p in out_sample.glob("*.parquet"))
            self.assertEqual(out_sample_shards, ["shard_000.parquet"])

            manifest = json.loads((out_sample / "_conversion_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["shard_count"], 1)
            self.assertEqual(manifest["shards"][0]["source_sha256"], hash_a_before)

            self.assertEqual(convert_mod._sha256_file(shard_a), hash_a_before)
            self.assertEqual(convert_mod._sha256_file(shard_b), hash_b_before)


class TestCleanRunsTimestampAndPrecision(unittest.TestCase):
    def test_cross_second_fractional_duration_is_not_truncated_to_one_second(self):
        _, t_start = clean_mod.parse_utc_timestamp_py("2026-01-01T00:00:00.900Z")
        _, t_end = clean_mod.parse_utc_timestamp_py("2026-01-01T00:00:01.100Z")
        self.assertIsNotNone(t_start)
        self.assertIsNotNone(t_end)
        delta = round(t_end - t_start, 6)
        self.assertAlmostEqual(delta, 0.2, places=6)

    def test_same_second_subsecond_inversion_is_negative_delta(self):
        _, t_start = clean_mod.parse_utc_timestamp_py("2026-01-01T00:00:05.500Z")
        _, t_end = clean_mod.parse_utc_timestamp_py("2026-01-01T00:00:05.200Z")
        self.assertIsNotNone(t_start)
        self.assertIsNotNone(t_end)
        self.assertLess(t_end - t_start, 0.0)

    def test_invalid_or_empty_timestamp_returns_none_tuple(self):
        self.assertEqual(clean_mod.parse_utc_timestamp_py(None), (None, None))
        self.assertEqual(clean_mod.parse_utc_timestamp_py(""), (None, None))
        self.assertEqual(clean_mod.parse_utc_timestamp_py("not-a-timestamp"), (None, None))


class TestPartitionBenchTelemetryFailure(unittest.TestCase):
    def test_null_totals_marks_metrics_unavailable_instead_of_zero(self):
        totals = bench_mod._null_totals()
        self.assertIsNone(totals["stage_count"])
        self.assertIsNone(totals["total_tasks"])
        self.assertIsNone(totals["executor_run_time_ms"])
        self.assertIsNone(totals["shuffle_read_bytes"])
        self.assertIsNone(totals["shuffle_write_bytes"])

    def test_fetch_spark_rest_returns_error_dict_on_unreachable_endpoint(self):
        res = bench_mod.fetch_spark_rest("http://127.0.0.1:1", "app-test-0001", "stages")
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)

    def test_empty_stages_and_workers_are_not_acceptable(self):
        ok, breakdown, errors = bench_mod.evaluate_t05_evidence([], [])
        self.assertFalse(ok)
        self.assertEqual(breakdown["completed_stage_count"], 0)
        self.assertEqual(breakdown["total_completed_tasks"], 0)
        self.assertEqual(breakdown["workers_with_completed_tasks"], 0)
        self.assertTrue(any(e["endpoint"] == "stages" for e in errors))
        self.assertTrue(any(e["endpoint"] == "executors" for e in errors))

    def test_http_error_is_not_acceptable(self):
        ok, _, errors = bench_mod.evaluate_t05_evidence(
            {"error": "Connection refused"},
            {"error": "Connection refused"},
        )
        self.assertFalse(ok)
        self.assertEqual(len(errors), 2)

    def test_completed_stages_without_tasks_are_not_acceptable(self):
        stages = [{"stageId": 0, "status": "COMPLETE", "numCompleteTasks": 0}]
        executors = [{"id": "1", "completedTasks": 4}]
        ok, breakdown, errors = bench_mod.evaluate_t05_evidence(stages, executors)
        self.assertFalse(ok)
        self.assertEqual(breakdown["completed_stage_count"], 1)
        self.assertEqual(breakdown["total_completed_tasks"], 0)

    def test_completed_stage_without_worker_is_not_acceptable(self):
        stages = [{"stageId": 0, "status": "COMPLETE", "numCompleteTasks": 4}]
        executors = [{"id": "driver", "completedTasks": 0}]
        ok, breakdown, errors = bench_mod.evaluate_t05_evidence(stages, executors)
        self.assertFalse(ok)
        self.assertEqual(breakdown["workers_with_completed_tasks"], 0)

    def test_completed_stage_with_tasks_and_worker_is_acceptable(self):
        stages = [
            {"stageId": 0, "status": "COMPLETE", "numCompleteTasks": 4},
            {"stageId": 1, "status": "COMPLETE", "numCompleteTasks": 3},
        ]
        executors = [
            {"id": "driver", "completedTasks": 0},
            {"id": "1", "completedTasks": 4},
            {"id": "2", "completedTasks": 3},
        ]
        ok, breakdown, errors = bench_mod.evaluate_t05_evidence(stages, executors)
        self.assertTrue(ok)
        self.assertEqual(breakdown["completed_stage_count"], 2)
        self.assertEqual(breakdown["total_completed_tasks"], 7)
        self.assertEqual(breakdown["workers_with_completed_tasks"], 2)
        self.assertEqual(errors, [])


class TestPySparkEndToEndIfAvailable(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.has_pyspark = not _USED_PYSPARK_STUB

    def test_clean_runs_retains_unfinished_and_nulls_inverted_elapsed_seconds(self):
        if not self.has_pyspark:
            self.skipTest("pyspark not installed in local Python environment")

        spark_sql_mod = importlib.import_module("pyspark.sql")
        SparkSession = spark_sql_mod.SparkSession

        spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("test-p02-clean-runs")
            .getOrCreate()
        )
        try:
            rows = [
                (1, 10, 100, ".github/workflows/a.yml",
                 "2026-01-01T00:00:00.900Z", "2026-01-01T00:00:01.100Z",
                 "push", "completed", "success", 1, 1, 1, 0),
                (2, 10, 101, ".github/workflows/b.yml",
                 "2026-01-01T00:01:00.000Z", "2026-01-01T00:01:02.000Z",
                 "push", "queued", None, 1, 0, 0, 0),
                (3, 10, 102, ".github/workflows/c.yml",
                 "2026-01-01T00:02:00.500Z", "2026-01-01T00:02:00.200Z",
                 "pull_request", "completed", "failure", 1, 0, 0, 0),
                (4, 10, 103, "",
                 "2026-01-01T00:03:00.000Z", "2026-01-01T00:03:01.000Z",
                 "push", "completed", "success", 1, 0, 0, 0),
            ]
            cols = clean_mod.P01_RUN_COLUMNS
            in_df = spark.createDataFrame(rows, cols)

            with tempfile.TemporaryDirectory() as tmp:
                in_dir = os.path.join(tmp, "in_parquet")
                out_dir = os.path.join(tmp, "out_clean")
                report_path = os.path.join(tmp, "report.json")
                in_dir_uri = Path(in_dir).as_uri()
                out_dir_uri = Path(out_dir).as_uri()
                in_df.write.mode("overwrite").parquet(in_dir_uri)

                orig_argv = sys.argv
                orig_stop = spark.stop
                spark.stop = lambda: None
                try:
                    sys.argv = ["clean_runs.py", in_dir_uri, out_dir_uri, report_path]
                    clean_mod.main()
                finally:
                    sys.argv = orig_argv
                    spark.stop = orig_stop

                rep = json.loads(Path(report_path).read_text(encoding="utf-8"))
                self.assertTrue(rep["reconciled_equals_input"])
                self.assertEqual(rep["retained_rows"], 3)
                self.assertEqual(rep["total_quarantined_rows"], 1)
                self.assertEqual(rep["retained_unfinished_runs"]["total_rows"], 1)
                self.assertEqual(rep["quality_statistics"]["time_inversion_count"], 1)

                clean_df = spark.read.parquet(rep["clean_path"])
                clean_rows = {r["run_id"]: r for r in clean_df.collect()}
                self.assertIn(2, clean_rows)
                self.assertAlmostEqual(clean_rows[1]["elapsed_seconds"], 0.2, places=5)
                self.assertIsNone(clean_rows[3]["elapsed_seconds"])
        finally:
            spark.stop()

    def _run_clean_with_rows(self, rows, schema=None):
        """起 Spark，写 rows 到 parquet，调 clean_mod.main，返回 (report, spark, clean_df, quarantine_df)。

        关键点：
        - 用 file:// URI 让 Spark 走本地 FS（节点默认 fs.defaultFS 是 HDFS）
        - 临时禁用 spark.stop，避免 main() 内部 stop 后再读 parquet 报 stopped SparkContext
        - 从 report 的 clean_path / quarantine_path 读，不拼 out_dir/runs_clean
          （生产代码把文件放到 batches/<token>/ 下）
        """
        import shutil
        spark_sql_mod = importlib.import_module("pyspark.sql")
        SparkSession = spark_sql_mod.SparkSession
        spark = (SparkSession.builder
                 .master("local[1]")
                 .appName("test-p02-nullable-fields")
                 .getOrCreate())
        if schema is None:
            schema = clean_mod.P01_RUN_COLUMNS
        in_df = spark.createDataFrame(rows, schema)
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        in_dir = os.path.join(tmp, "in_parquet")
        out_dir = os.path.join(tmp, "out_clean")
        report_path = os.path.join(tmp, "report.json")
        in_dir_uri = Path(in_dir).as_uri()
        out_dir_uri = Path(out_dir).as_uri()
        in_df.write.mode("overwrite").parquet(in_dir_uri)

        orig_argv = sys.argv
        orig_stop = spark.stop
        spark.stop = lambda: None
        try:
            sys.argv = ["clean_runs.py", in_dir_uri, out_dir_uri, report_path]
            clean_mod.main()
        finally:
            sys.argv = orig_argv
            spark.stop = orig_stop

        rep = json.loads(Path(report_path).read_text(encoding="utf-8"))
        clean_df = spark.read.parquet(rep["clean_path"])
        quarantine_df = spark.read.parquet(rep["quarantine_path"])
        self.addCleanup(spark.stop)
        return rep, spark, clean_df, quarantine_df

    def test_null_workflow_id_is_retained(self):
        if not self.has_pyspark:
            self.skipTest("pyspark not installed in local Python environment")
        rows = [
            (1, 10, None, ".github/workflows/a.yml",
             "2026-01-01T00:00:00Z", "2026-01-01T00:00:01Z",
             "push", "completed", "success", 1, 0, 0, 0),
        ]
        # workflow_id 整列为 None，Spark 2.0 无法推断类型，需显式 schema
        rep, spark, clean_df, quarantine_df = self._run_clean_with_rows(rows, _p02_run_schema())
        self.assertEqual(rep["retained_rows"], 1)
        self.assertEqual(rep["total_quarantined_rows"], 0)
        clean_rows = {r["run_id"]: r for r in clean_df.collect()}
        self.assertIn(1, clean_rows)
        self.assertIsNone(clean_rows[1]["workflow_id"])
        self.assertEqual(quarantine_df.count(), 0)

    def test_null_status_is_activity_not_outcome(self):
        if not self.has_pyspark:
            self.skipTest("pyspark not installed in local Python environment")
        rows = [
            (1, 10, 100, ".github/workflows/a.yml",
             "2026-01-01T00:00:00Z", "2026-01-01T00:00:01Z",
             "push", None, None, 1, 0, 0, 0),
            (2, 10, 100, ".github/workflows/a.yml",
             "2026-01-01T00:01:00Z", "2026-01-01T00:01:02Z",
             "push", "completed", "failure", 1, 0, 0, 0),
        ]
        rep, spark, clean_df, quarantine_df = self._run_clean_with_rows(rows)
        self.assertEqual(rep["retained_rows"], 2)
        self.assertEqual(rep["total_quarantined_rows"], 0)
        self.assertEqual(rep["retained_unfinished_runs"]["total_rows"], 1)
        self.assertEqual(rep["retained_unfinished_runs"]["by_status"].get("<NULL>"), 1)
        self.assertEqual(rep["independent_rule_hit_counts"]["null_status"], 1)
        self.assertEqual(rep["independent_rule_hit_counts"]["null_conclusion"], 1)
        clean_rows = {r["run_id"]: r for r in clean_df.collect()}
        self.assertIn(1, clean_rows)
        self.assertIsNone(clean_rows[1]["status"])
        self.assertIsNone(clean_rows[1]["conclusion"])
        self.assertEqual(quarantine_df.count(), 0)

    def test_missing_workflow_path_is_quarantined(self):
        if not self.has_pyspark:
            self.skipTest("pyspark not installed in local Python environment")
        rows = [
            (1, 10, 100, None,
             "2026-01-01T00:00:00Z", "2026-01-01T00:00:01Z",
             "push", "completed", "success", 1, 0, 0, 0),
            (2, 10, 100, "",
             "2026-01-01T00:01:00Z", "2026-01-01T00:01:02Z",
             "push", "completed", "success", 1, 0, 0, 0),
            (3, 10, 100, ".github/workflows/ok.yml",
             "2026-01-01T00:02:00Z", "2026-01-01T00:02:01Z",
             "push", "completed", "success", 1, 0, 0, 0),
        ]
        rep, spark, clean_df, quarantine_df = self._run_clean_with_rows(rows)
        self.assertEqual(rep["retained_rows"], 1)
        self.assertEqual(rep["total_quarantined_rows"], 2)
        self.assertEqual(rep["mutually_exclusive_quarantine_breakdown"].get("INVALID_WORKFLOW_PATH"), 2)
        q_rows = quarantine_df.collect()
        self.assertEqual({r["run_id"] for r in q_rows}, {1, 2})
        self.assertTrue(all(r["quarantine_reason"] == "INVALID_WORKFLOW_PATH" for r in q_rows))
        clean_rows = {r["run_id"]: r for r in clean_df.collect()}
        self.assertIn(3, clean_rows)


if __name__ == "__main__":
    unittest.main()