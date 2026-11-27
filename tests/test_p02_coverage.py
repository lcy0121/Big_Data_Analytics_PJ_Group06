from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
import tarfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from ghawtrace.inputs import RUN_INPUT_SCHEMA
from ghawtrace.p02.coverage import audit
from ghawtrace.pipeline import prepare_batch


class CoverageTest(unittest.TestCase):
    def audit_fixture(self, extra=False):
        scratch = TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        root = Path(scratch.name)
        source = root / "source"
        logs, runs = source / "logs", source / "runs"
        logs.mkdir(parents=True)
        runs.mkdir()
        rows = [
            dict(run_id=i, repo_id=i, workflow_path="same.yml", event=e, created_at=t)
            for i, e, t in [(1, "push", "2026-01-31T23:00:00Z"), (2, None, "2026-02-01T00:00:00Z")]
        ]
        pq.write_table(pa.Table.from_pylist(rows, schema=RUN_INPUT_SCHEMA), runs / "part-00000.parquet")
        members = [(1, 1), (1, 2)]
        if extra:
            members += [(1, 1), (2, 1), (99, 1)]
        with tarfile.open(logs / "1.tar", "w") as tar:
            for rid, attempt in members:
                info = tarfile.TarInfo(f"{rid}/attempt-{attempt}.zip")
                info.size = 1
                tar.addfile(info, BytesIO(b"x"))
        if extra:
            # P01 deliberately rejects duplicate keys; replay a faulty inventory against
            # the valid P01 index, preserving the independent TAR scan under test.
            from unittest.mock import patch
            indexed = [dict(run_id=1, repo_id=1, attempt=a, tar_path="logs/1.tar",
                            tar_member=f"1/attempt-{a}.zip", archive_bytes=1) for a in (1, 2)]
            with patch("ghawtrace.pipeline.index_attempts", return_value=indexed):
                batch, _ = prepare_batch(source, root / "batches", "full")
        else:
            batch, _ = prepare_batch(source, root / "batches", "full")
        return audit(batch, logs)

    def test_multiple_attempts_and_zero_coverage_group(self):
        report = self.audit_fixture()
        self.assertEqual(report["status"], "success")
        self.assertEqual(report["totals"]["total_workflows"], 2)
        self.assertEqual(report["totals"]["covered_runs"], 1)
        self.assertEqual([r["coverage_rate"] for r in report["by_utc_month_event"]], [1.0, 0.0])

    def test_invalid_archive_does_not_inflate_coverage(self):
        report = self.audit_fixture(extra=True)
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["totals"]["covered_runs"], 1)
        for issue in ("duplicate_attempt_extra_entries", "orphan_zip_members", "repo_mismatch_members"):
            self.assertEqual(report["issues"][issue], 1)
        self.assertEqual(report["issues"]["p01_index_rows_extra"], 3)
