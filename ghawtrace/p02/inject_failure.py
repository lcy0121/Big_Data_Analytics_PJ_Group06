# -*- coding: utf-8 -*-
"""Fault injection acceptance for publish robustness (review item 3 / item 2).

Run from repo root on a Spark 2.x node:

    spark-submit --master local[1] \
      --py-files ghawtrace/p02/clean_runs.py \
      ghawtrace/p02/inject_failure.py

Env:
    CLEAN_RUNS_PATH  override; default: ./clean_runs.py (same dir)
"""
from __future__ import print_function
import os
import sys
import json
import glob
import shutil
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
clean_runs_path = os.environ.get("CLEAN_RUNS_PATH") or os.path.join(HERE, "clean_runs.py")
if not os.path.isfile(clean_runs_path):
    print("ERROR: clean_runs.py not found at", clean_runs_path)
    sys.exit(2)
print("[setup] loading clean_runs from:", clean_runs_path)
clean_mod = load_module("clean_runs", clean_runs_path)
InjectedFault = getattr(clean_mod, "InjectedFault", None)
if InjectedFault is None:
    print("ERROR: clean_runs.py does not export InjectedFault.")
    print("Make sure _maybe_inject_fault raises InjectedFault(stage_name).")
    sys.exit(2)

from pyspark.sql import SparkSession

ROWS = [
    (1, 10, None, ".github/workflows/a.yml", "2026-01-01T00:00:00Z", "2026-01-01T00:00:01Z",
     "push", "completed", "success", 1, 0, 0, 0),
    (2, 10, 100, ".github/workflows/a.yml", "2026-01-01T00:01:00Z", "2026-01-01T00:01:02Z",
     "push", None, None, 1, 0, 0, 0),
    (3, 10, 100, None, "2026-01-01T00:02:00Z", "2026-01-01T00:02:01Z",
     "push", "completed", "success", 1, 0, 0, 0),
    (4, 10, 100, ".github/workflows/b.yml", "2026-01-01T00:03:00Z", "2026-01-01T00:03:01Z",
     "push", "completed", "failure", 1, 0, 0, 0),
]

FAULT_POINTS = [
    "after_batch_clean_write",
    "after_batch_success",
    "after_stage_success",
    "after_rename_current_to_backup",
]

_spark_ref = [None]


def make_input(spark, path):
    df = spark.createDataFrame(ROWS, clean_mod.P01_RUN_COLUMNS)
    df.write.mode("overwrite").parquet("file://" + path)


def run_clean(input_dir, output_dir, report_path):
    orig_argv = sys.argv
    orig_stop = _spark_ref[0].stop
    _spark_ref[0].stop = lambda: None
    try:
        sys.argv = ["clean_runs.py", "file://" + input_dir, "file://" + output_dir, report_path]
        clean_mod.main()
    finally:
        sys.argv = orig_argv
        _spark_ref[0].stop = orig_stop


def read_current_token(output_dir):
    p = os.path.join(output_dir, "current", "_SUCCESS")
    if not os.path.exists(p):
        return None
    with open(p) as f:
        try:
            return json.load(f).get("batch_token")
        except Exception:
            return None


def current_is_complete(output_dir):
    cur = os.path.join(output_dir, "current")
    if not os.path.isdir(cur):
        return False, "current dir missing"
    if not os.path.exists(os.path.join(cur, "_SUCCESS")):
        return False, "current/_SUCCESS missing"
    for sub in ("runs_clean", "runs_quarantine"):
        d = os.path.join(cur, sub)
        if not os.path.isdir(d):
            return False, "%s missing" % sub
        parts = glob.glob(os.path.join(d, "part-*.parquet"))
        if not parts:
            return False, "%s has no parquet" % sub
    return True, "ok"


def _local(p):
    return p[len("file://"):] if p.startswith("file://") else p


def verify_rep1_paths(spark, rep1_path):
    """Read the two fixed paths from the first report and check them
    against the row counts recorded in that same report.

    Missing table, unreadable table, or any row-count mismatch is a FAIL.
    A quarantine table that exists and really holds 0 rows is acceptable
    only when the report also says total_quarantined_rows == 0.
    """
    with open(rep1_path) as f:
        r1 = json.load(f)
    old_clean = r1["clean_path"]
    old_q = r1["quarantine_path"]
    old_token = r1["batch_token"]
    expected_clean = int(r1["retained_rows"])
    expected_q = int(r1["total_quarantined_rows"])

    # Always try to read both fixed paths; a missing table is a failure,
    # not an implicit zero.
    try:
        clean_rows = spark.read.parquet(old_clean).count()
    except Exception as e:
        return None, "cannot read rep1 clean_path %s: %s" % (old_clean, str(e)[:200])

    try:
        q_rows = spark.read.parquet(old_q).count()
    except Exception as e:
        return None, "cannot read rep1 quarantine_path %s: %s" % (old_q, str(e)[:200])

    success_file = _local(old_clean.rsplit("/", 1)[0] + "/_SUCCESS")
    try:
        with open(success_file) as f:
            success_dict = json.load(f)
    except Exception as e:
        return None, "cannot read _SUCCESS %s: %s" % (success_file, str(e)[:200])
    success_token = success_dict.get("batch_token")

    if success_token != old_token:
        return None, "rep1 _SUCCESS token mismatch: %s != %s" % (success_token, old_token)
    if clean_rows != expected_clean:
        return None, "rep1 clean rows mismatch: actual=%d expected=%d" % (clean_rows, expected_clean)
    if q_rows != expected_q:
        return None, "rep1 quarantine rows mismatch: actual=%d expected=%d" % (q_rows, expected_q)

    return (old_token, clean_rows, q_rows), "ok"


def run_one_fault_point(spark, fp):
    """Returns (ok, message). On PASS, message carries stage/batch_token/rows."""
    tmp = tempfile.mkdtemp(prefix="p02_ft_%s_" % fp)
    try:
        input_dir = os.path.join(tmp, "in")
        output_dir = os.path.join(tmp, "out")
        rep1 = os.path.join(tmp, "rep1.json")
        rep2 = os.path.join(tmp, "rep2.json")

        make_input(spark, input_dir)

        if "P02_FAULT_INJECT" in os.environ:
            del os.environ["P02_FAULT_INJECT"]
        run_clean(input_dir, output_dir, rep1)
        token1 = read_current_token(output_dir)
        ok1, msg1 = current_is_complete(output_dir)
        if not ok1 or not token1:
            return False, "first run not usable: " + msg1

        os.environ["P02_FAULT_INJECT"] = fp
        fault_hit = False
        fault_mismatch = None
        other_exc = None
        try:
            run_clean(input_dir, output_dir, rep2)
        except InjectedFault as e:
            if getattr(e, "stage_name", None) == fp:
                fault_hit = True
            else:
                fault_mismatch = getattr(e, "stage_name", "<missing>")
        except Exception as e:
            other_exc = "%s: %s" % (type(e).__name__, str(e)[:200])
        finally:
            if "P02_FAULT_INJECT" in os.environ:
                del os.environ["P02_FAULT_INJECT"]

        if fault_mismatch is not None:
            return False, "InjectedFault for wrong stage: %s" % fault_mismatch
        if other_exc is not None:
            return False, "unexpected non-injected exception: %s" % other_exc
        if not fault_hit:
            return False, "no exception raised; injection point not reached"

        info, msg = verify_rep1_paths(spark, rep1)
        if info is None:
            return False, msg

        batch_token, clean_rows, q_rows = info
        token_after = read_current_token(output_dir)
        if token_after != token1:
            return False, "current changed: before=%s after=%s" % (token1, token_after)
        ok2, msg2 = current_is_complete(output_dir)
        if not ok2:
            return False, "current incomplete after fault: " + msg2

        return True, (
            "stage=%s; batch_token=%s; clean_rows=%d; quarantine_rows=%d"
            % (fp, batch_token, clean_rows, q_rows)
        )
    finally:
        shutil.rmtree(tmp, True)


def run_negative_regression(spark):
    """Non-injection failure before publish must NOT count as a fault hit.

    Simulates an ordinary input error on the second run: delete the input
    parquet so clean_runs.py fails before reaching any publish stage.
    Expected:
      - clean_runs.py raises a non-InjectedFault exception
      - current/ still points to the first batch, intact
    """
    tmp = tempfile.mkdtemp(prefix="p02_neg_")
    try:
        input_dir = os.path.join(tmp, "in")
        output_dir = os.path.join(tmp, "out")
        rep1 = os.path.join(tmp, "rep1.json")
        rep2 = os.path.join(tmp, "rep2.json")

        make_input(spark, input_dir)
        if "P02_FAULT_INJECT" in os.environ:
            del os.environ["P02_FAULT_INJECT"]
        run_clean(input_dir, output_dir, rep1)
        token1 = read_current_token(output_dir)
        ok1, msg1 = current_is_complete(output_dir)
        if not ok1 or not token1:
            return False, "negative regression: first run not usable: " + msg1

        # Corrupt input: remove all parquet files
        for name in os.listdir(input_dir):
            if name.endswith(".parquet"):
                os.remove(os.path.join(input_dir, name))

        got_injected = False
        got_other = None
        try:
            run_clean(input_dir, output_dir, rep2)
        except InjectedFault:
            got_injected = True
        except Exception as e:
            got_other = "%s: %s" % (type(e).__name__, str(e)[:200])

        ok_cur, msg_cur = current_is_complete(output_dir)
        cur_token = read_current_token(output_dir)

        print("-" * 60)
        print("NEGATIVE REGRESSION: input corrupted, no fault injection")
        print("  got_InjectedFault       :", got_injected, "(expected False)")
        print("  got_other_exception     :", got_other, "(expected non-None)")
        print("  current_intact          :", ok_cur, msg_cur)
        print("  current_token_unchanged :", cur_token == token1)

        ok = (
            (not got_injected)
            and (got_other is not None)
            and ok_cur
            and (cur_token == token1)
        )
        return ok, ("input_corrupted: InjectedFault=%s, other_exc=%s, current_unchanged=%s"
                    % (got_injected, "yes" if got_other else "no", cur_token == token1))
    finally:
        shutil.rmtree(tmp, True)

def run_row_mismatch_regressions(spark):
    """Row-count reconciliation regression.

    Case A quarantine_missing : delete quarantine parquet dir -> expect FAIL
    Case B clean_shrunk       : clean table shrunk to 1 row    -> expect FAIL
    Case C quarantine_zero_ok : quarantine exists but 0 rows,
                                report says total_quarantined_rows=0 -> expect PASS
    """
    results = []

    def _local(p):
        return p[len("file://"):] if p.startswith("file://") else p

    def _preserve_success(path):
        src = os.path.join(path, "_SUCCESS")
        if not os.path.exists(src):
            return None
        tmp = src + ".kept"
        shutil.move(src, tmp)
        return tmp

    def _restore_success(path, tmp):
        if tmp and os.path.exists(tmp):
            shutil.move(tmp, os.path.join(path, "_SUCCESS"))

    def _run_case(case_name, mutate, expect_pass):
        tmp = tempfile.mkdtemp(prefix="p02_row_%s_" % case_name)
        try:
            input_dir = os.path.join(tmp, "in")
            output_dir = os.path.join(tmp, "out")
            rep1 = os.path.join(tmp, "rep1.json")
            rep2 = os.path.join(tmp, "rep2.json")

            make_input(spark, input_dir)
            if "P02_FAULT_INJECT" in os.environ:
                del os.environ["P02_FAULT_INJECT"]
            run_clean(input_dir, output_dir, rep1)

            ok1, msg1 = current_is_complete(output_dir)
            if not ok1:
                return (case_name, False, "first run not usable: " + msg1)

            try:
                mutate(spark, rep1)
            except Exception as e:
                return (case_name, False,
                        "mutate failed: %s: %s" % (type(e).__name__, str(e)[:200]))

            os.environ["P02_FAULT_INJECT"] = "after_stage_success"
            fault_hit = False
            other_exc = None
            try:
                run_clean(input_dir, output_dir, rep2)
            except InjectedFault as e:
                if getattr(e, "stage_name", None) == "after_stage_success":
                    fault_hit = True
            except Exception as e:
                other_exc = "%s: %s" % (type(e).__name__, str(e)[:200])
            finally:
                if "P02_FAULT_INJECT" in os.environ:
                    del os.environ["P02_FAULT_INJECT"]

            if not fault_hit:
                return (case_name, False,
                        "fault not hit" + (" (other exc: %s)" % other_exc if other_exc else ""))

            info, msg = verify_rep1_paths(spark, rep1)
            got_pass = info is not None
            if expect_pass:
                return (case_name, got_pass,
                        ("PASS as expected: " + msg) if got_pass
                        else ("expected PASS but rejected: " + msg))
            else:
                return (case_name, (not got_pass),
                        ("rejected as expected: " + msg) if (not got_pass)
                        else "expected FAIL but passed")
        finally:
            shutil.rmtree(tmp, True)

    def _mutate_quarantine_missing(spark, rep1_path):
        with open(rep1_path) as f:
            r1 = json.load(f)
        q_path = _local(r1["quarantine_path"])
        if os.path.isdir(q_path):
            shutil.rmtree(q_path)
        if os.path.exists(q_path):
            raise RuntimeError("could not remove quarantine dir: %s" % q_path)

    def _mutate_clean_shrunk(spark, rep1_path):
        with open(rep1_path) as f:
            r1 = json.load(f)
        clean_path = r1["clean_path"]
        clean_local = _local(clean_path)
        shrunk = clean_local + "_shrunk"

        # 1) materialize limited data into a SEPARATE directory first
        spark.read.parquet(clean_path).limit(1).write.mode("overwrite").parquet(
            "file://" + shrunk
        )
        # 2) preserve _SUCCESS
        kept = _preserve_success(clean_local)
        # 3) swap directories
        shutil.rmtree(clean_local)
        shutil.move(shrunk, clean_local)
        # 4) restore _SUCCESS
        _restore_success(clean_local, kept)

    def _mutate_quarantine_zero_ok(spark, rep1_path):
        with open(rep1_path) as f:
            r1 = json.load(f)
        q_path = r1["quarantine_path"]
        q_local = _local(q_path)

        original_schema = spark.read.parquet(q_path).schema
        kept = _preserve_success(q_local)

        empty = spark.createDataFrame([], original_schema)
        empty.write.mode("overwrite").parquet(q_path)

        _restore_success(q_local, kept)

        r1["total_quarantined_rows"] = 0
        with open(rep1_path, "w") as f:
            json.dump(r1, f)

    results.append(_run_case("quarantine_missing", _mutate_quarantine_missing, expect_pass=False))
    results.append(_run_case("clean_shrunk", _mutate_clean_shrunk, expect_pass=False))
    results.append(_run_case("quarantine_zero_ok", _mutate_quarantine_zero_ok, expect_pass=True))
    return results


def main():
    spark = (SparkSession.builder
             .master("local[1]")
             .appName("inject-failure")
             .getOrCreate())
    _spark_ref[0] = spark

    results = []
    try:
        for fp in FAULT_POINTS:
            ok, msg = run_one_fault_point(spark, fp)
            results.append((fp, ok, msg))
        neg_ok, neg_msg = run_negative_regression(spark)
        results.append(("input_corrupted", neg_ok, neg_msg))
        for case_name, ok, msg in run_row_mismatch_regressions(spark):
            results.append((case_name, ok, msg))
    finally:
        spark.stop()

    print("=" * 60)
    print("FAULT INJECTION RESULTS")
    for fp, ok, msg in results:
        print(("PASS" if ok else "FAIL"), "-", fp, "-", msg)
    print("=" * 60)

    all_pass = bool(results) and all(ok for _, ok, _ in results)
    print("RESULT:", "ALL PASS" if all_pass else "SOME FAILED")
    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()