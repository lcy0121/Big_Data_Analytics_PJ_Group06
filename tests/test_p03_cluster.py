"""Spark reducer contract and fail-closed receipt checks using synthetic workflows."""
from collections import defaultdict
import gzip
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from ghawtrace.feature_pipeline import prepare_features, sha256_file
from ghawtrace.p03.spark_profile import profile
from ghawtrace.p03.verify_cluster import verify


class ClusterProfileTest(unittest.TestCase):
    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.spark = self.root / "spark"
        self.spark.mkdir()
        (self.spark / "_SUCCESS").touch()
        fixture = [
            (1, 10, 900001, 1100002, "new-event", "completed", "success", 2),
            (1, 11, 900002, 900001, None, "completed", "skipped", 1),
            (2, None, 1000001, None, "schedule", None, None, None),
        ]
        groups = defaultdict(list)
        records = []
        for i, (repo, wid, start, end, event, status, conclusion, attempts) in enumerate(fixture):
            groups[(repo, "same.yml")].append((wid, start, end, event, status, conclusion, attempts))
            records.append(dict(run_id=i+1, repo_id=repo, workflow_path="same.yml", workflow_id=wid,
                                created_at_utc=start, updated_at_utc=end, event=event, status=status,
                                conclusion=conclusion, run_attempts=attempts))
        source = self.root / "clean.parquet"
        table = pa.Table.from_pylist(records)
        for field in ("created_at_utc", "updated_at_utc"):
            table = table.set_column(table.schema.get_field_index(field), field,
                                     table[field].cast(pa.timestamp("us", tz="UTC")))
        pq.write_table(table, source)
        self.ref, _ = prepare_features(source, self.root / "out")
        self.marker = self.root / "marker.json"
        self.marker.write_text(json.dumps(dict(batch_token="synthetic", retained_rows=3, total_input_rows=3,
                                               quarantined_rows=0, reconciled_equals_input=True)))
        self.hashes = self.root / "clean.sha256"
        self.hashes.write_text(f"{sha256_file(source)}  {source.name}\n")
        self.records = [dict(batch_token="synthetic", partition=i, worker=f"test-{i}", application_id="app-test",
                             profile=profile(key, rows)) for i, (key, rows) in enumerate(groups.items())]

    def run_check(self):
        for i, item in enumerate(self.records):
            with gzip.open(self.spark / f"part-{i:05d}.gz", "wt") as out:
                out.write(json.dumps(item) + "\n")
        return verify(self.ref, self.spark, self.marker, self.hashes)

    def test_all_columns_microseconds_empty_denominators_and_event_buckets(self):
        report = self.run_check()
        self.assertEqual((report["runs"], report["workflows"], report["mismatches"]), (3, 2, 0))
        self.assertEqual(report["compared_cells"], 2 * report["profile_columns"])

    def test_rejects_single_worker(self):
        self.records[1]["worker"] = self.records[0]["worker"]
        with self.assertRaisesRegex(ValueError, "distributed execution"):
            self.run_check()

    def test_rejects_same_row_count_with_duplicate_workflow(self):
        self.records[1]["profile"] = self.records[0]["profile"]
        with self.assertRaisesRegex(ValueError, "duplicate workflow"):
            self.run_check()

    def test_rejects_null_replaced_by_zero(self):
        self.records[1]["profile"]["retry_rate"] = 0
        with self.assertRaisesRegex(ValueError, "retry_rate"):
            self.run_check()

    def test_rejects_checksum_drift(self):
        self.hashes.write_text("0" * 64 + "  clean.parquet\n")
        with self.assertRaisesRegex(ValueError, "differs"):
            self.run_check()
