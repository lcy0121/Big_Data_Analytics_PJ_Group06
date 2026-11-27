"""Audit the classroom TAR subset against a completed P01 batch (UTC creation month)."""
import argparse
from collections import Counter
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tarfile
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ghawtrace.inputs import ATTEMPT_MEMBER
from ghawtrace.pipeline import sha256_file, write_json


def read_runs(db, path):
    db.execute("CREATE TABLE runs(id INTEGER PRIMARY KEY, repo INTEGER NOT NULL, "
               "workflow TEXT NOT NULL, month TEXT NOT NULL, event TEXT)")
    with pq.ParquetFile(path) as source:
        for batch in source.iter_batches(columns=["run_id", "repo_id", "workflow_path", "created_at", "event"]):
            ids, repos, paths, dates, events = batch.columns
            if ids.null_count or dates.null_count:
                raise ValueError("Missing run_id or created_at")
            utc = pc.cast(dates, pa.timestamp("us", tz="UTC"))
            months = pc.strftime(utc, format="%Y-%m")
            db.executemany("INSERT INTO runs VALUES (?,?,?,?,?)",
                           zip(*(col.to_pylist() for col in (ids, repos, paths, months, events))))


def read_archives(db, archives, sample_ids):
    db.execute("CREATE TABLE archives(tar TEXT PRIMARY KEY, repo INTEGER NOT NULL)")
    db.execute("CREATE TABLE entries(tar TEXT, repo INTEGER, id INTEGER, attempt INTEGER, bytes INTEGER)")
    invalid = Counter()
    for archive in archives:
        repo = int(archive.stem)
        db.execute("INSERT INTO archives VALUES (?,?)", (archive.name, repo))
        rows = []
        with tarfile.open(archive, "r:") as source:
            for member in source:
                if member.isdir():
                    continue
                match = ATTEMPT_MEMBER.fullmatch(member.name)
                if not member.isfile() or not match:
                    invalid[archive.name] += 1
                    continue
                rid, attempt = map(int, match.groups())
                if sample_ids is None or rid in sample_ids:
                    rows.append((archive.name, repo, rid, attempt, member.size))
        db.executemany("INSERT INTO entries VALUES (?,?,?,?,?)", rows)
    db.execute("CREATE INDEX archive_run ON entries(id, attempt)")
    return invalid


def dictionaries(db, query):
    cursor = db.execute(query)
    fields = [col[0] for col in cursor.description]
    return [dict(zip(fields, row)) for row in cursor]


def snapshot(path):
    info = path.stat()
    return info.st_ino, info.st_size, info.st_mtime_ns


def audit(batch, tar_dir):
    manifest_path = batch / "manifest.json"
    manifest_before = snapshot(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    mode = manifest["parameters"]["mode"]
    if manifest["status"] != "success" or mode not in ("sample", "full"):
        raise ValueError("Expected a completed P01 sample or full batch")
    archives = sorted(tar_dir.glob("*.tar"))
    expected = {Path(f["path"]).name: f for f in manifest["inputs"] if f["path"].startswith("logs/")}
    if not archives or {p.name for p in archives} != set(expected):
        raise ValueError("TAR inventory differs from the P01 batch")
    inputs = [batch / "runs_input.parquet", batch / "attempt_index.parquet"]
    before = {p: snapshot(p) for p in archives + inputs}
    before[manifest_path] = manifest_before
    for path in inputs:
        if path.stat().st_size != manifest["outputs"][path.name]["bytes"]:
            raise ValueError("Input size differs from P01: " + path.name)
    for path in archives:
        if sha256_file(path) != expected[path.name]["sha256"]:
            raise ValueError("TAR content differs from P01: " + path.name)
    with TemporaryDirectory() as scratch, closing(sqlite3.connect(Path(scratch) / "audit.sqlite")) as db:
        read_runs(db, inputs[0])
        selected = {r[0] for r in db.execute("SELECT id FROM runs")} if mode == "sample" else None
        invalid = read_archives(db, archives, selected)
        db.execute("CREATE TEMP TABLE covered AS SELECT DISTINCT r.id FROM runs r JOIN entries e "
                   "ON e.id=r.id AND e.repo=r.repo WHERE e.bytes>0")
        db.execute("CREATE UNIQUE INDEX covered_key ON covered(id)")
        # Independent TAR scan vs. the P01 index, including member size and multiplicity.
        expected_rows = Counter(tuple(row[c] for c in ("tar_path", "repo_id", "run_id", "attempt", "archive_bytes"))
                                for row in pq.read_table(inputs[1]).to_pylist())
        scanned = Counter(db.execute("SELECT 'logs/' || tar, repo, id, attempt, bytes FROM entries"))
        def scalar(sql):
            return db.execute(sql).fetchone()[0]

        if sum(expected_rows.values()) != manifest["outputs"][inputs[1].name]["rows"]:
            raise ValueError("Attempt count differs from P01 manifest")
        issues = {
            "malformed_members": sum(invalid.values()),
            "empty_zip_members": scalar("SELECT count(*) FROM entries WHERE bytes=0"),
            "duplicate_attempt_extra_entries": scalar("SELECT count(*) FROM entries") -
                scalar("SELECT count(*) FROM (SELECT id,attempt FROM entries GROUP BY id,attempt)"),
            "orphan_zip_members": scalar("SELECT count(*) FROM entries e LEFT JOIN runs r ON r.id=e.id WHERE r.id IS NULL"),
            "repo_mismatch_members": scalar("SELECT count(*) FROM entries e JOIN runs r ON r.id=e.id WHERE r.repo<>e.repo"),
            "p01_index_rows_missing": sum((expected_rows - scanned).values()),
            "p01_index_rows_extra": sum((scanned - expected_rows).values()),
        }
        tar_rows = dictionaries(db, """
            SELECT a.tar,a.repo AS repo_id,count(e.id) AS zip_members,count(DISTINCT e.id) AS distinct_run_ids,
                   count(DISTINCT CASE WHEN e.bytes>0 AND a.repo=r.repo THEN r.id END) AS covered_runs,
                   count(DISTINCT CASE WHEN e.bytes>0 AND a.repo=r.repo THEN r.workflow END) AS covered_workflows
            FROM archives a LEFT JOIN entries e ON e.tar=a.tar LEFT JOIN runs r ON r.id=e.id
            GROUP BY a.tar,a.repo ORDER BY a.repo,a.tar""")
        groups = dictionaries(db, """
            SELECT month,event,count(*) AS total_runs,count(c.id) AS covered_runs,
                   1.0*count(c.id)/count(*) AS coverage_rate
            FROM runs r LEFT JOIN covered c ON r.id=c.id GROUP BY month,event ORDER BY month,event""")
        totals = {
            "tar_files": len(archives), "zip_members": sum(scanned.values()),
            "archived_run_ids": scalar("SELECT count(DISTINCT id) FROM entries"),
            "total_runs": scalar("SELECT count(*) FROM runs"),
            "covered_runs": scalar("SELECT count(*) FROM covered"),
            "total_repos": scalar("SELECT count(DISTINCT repo) FROM runs"),
            "covered_repos": scalar("SELECT count(DISTINCT repo) FROM runs JOIN covered USING(id)"),
            "total_workflows": scalar("SELECT count(*) FROM (SELECT DISTINCT repo,workflow FROM runs)"),
            "covered_workflows": scalar("SELECT count(*) FROM (SELECT DISTINCT repo,workflow FROM runs JOIN covered USING(id))"),
        }
        if totals["total_runs"] != manifest["outputs"]["runs_input.parquet"]["rows"]:
            raise ValueError("Run count differs from P01 manifest")
        if sum(r["total_runs"] for r in groups) != totals["total_runs"] or sum(r["covered_runs"] for r in groups) != totals["covered_runs"]:
            raise ValueError("Coverage group reconciliation failed")
    digests = {p.name: sha256_file(p) for p in inputs}
    if sorted(tar_dir.glob("*.tar")) != archives or any(snapshot(p) != start for p, start in before.items()):
        raise ValueError("Input changed during audit")
    totals["coverage_rate"] = totals["covered_runs"] / totals["total_runs"] if totals["total_runs"] else None
    return {"status": "failed" if any(issues.values()) else "success", "mode": mode, "p01_batch": manifest["batch_id"],
            "input_sha256": digests, "totals": totals, "issues": issues,
            "tar_inventory": tar_rows, "by_utc_month_event": groups}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", required=True, type=Path)
    parser.add_argument("--tar-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path, help="Aggregate JSON; must be outside input directories")
    args = parser.parse_args()
    target = args.output.resolve()
    if any(target.is_relative_to(p.resolve()) for p in (args.batch, args.tar_dir)):
        parser.error("Output must be outside inputs")
    result = audit(args.batch, args.tar_dir)
    target.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=target.parent) as tmp:
        stage = Path(tmp) / "audit.json"
        write_json(stage, result)
        stage.replace(target)
    print(json.dumps({"status": result["status"], **result["totals"]}, sort_keys=True))
    return int(result["status"] != "success")


if __name__ == "__main__":
    raise SystemExit(main())
