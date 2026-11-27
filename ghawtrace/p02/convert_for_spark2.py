# -*- coding: utf-8 -*-
import hashlib
import json
import shutil
import sys
from pathlib import Path

import pyarrow.parquet as pq

# 对齐 P01 inputs.py 中的 13 列标准字段
P01_COLUMNS = [
    "run_id", "repo_id", "workflow_id", "workflow_path",
    "created_at", "updated_at", "event", "status", "conclusion",
    "run_attempts", "log_archive_count", "log_file_count", "empty_archive_count",
]


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _is_relative_to(child: Path, parent: Path) -> bool:
    try:
        child.relative_to(parent)
        return True
    except ValueError:
        return False


def _validate_paths(in_dir: Path, out_dir: Path, clean_output: bool) -> None:
    if not in_dir.exists() or not in_dir.is_dir():
        raise ValueError(f"Input directory does not exist or is not a directory: {in_dir}")

    if in_dir == out_dir:
        raise ValueError(
            f"Refusing to overwrite input directory in place: input and output resolve to the same path ({in_dir})"
        )

    if _is_relative_to(out_dir, in_dir) or _is_relative_to(in_dir, out_dir):
        raise ValueError(
            f"Refusing overlapping input/output directories: input={in_dir}, output={out_dir}"
        )

    if out_dir.exists():
        if not out_dir.is_dir():
            raise ValueError(f"Output path exists and is not a directory: {out_dir}")
        existing_entries = list(out_dir.iterdir())
        if existing_entries and not clean_output:
            raise ValueError(
                f"Output directory is not empty ({out_dir}); use a new batch directory or pass --clean-output to avoid stale shards"
            )


def _prepare_output_dir(out_dir: Path, clean_output: bool) -> None:
    if out_dir.exists() and clean_output:
        for entry in out_dir.iterdir():
            if entry.is_dir():
                shutil.rmtree(entry)
            else:
                entry.unlink()
    out_dir.mkdir(parents=True, exist_ok=True)


def _parse_args(argv: list[str]) -> tuple[Path, Path, int | None, bool]:
    if len(argv) < 3:
        raise ValueError(
            "Usage: python convert_for_spark2.py <input_dir> <output_dir> [limit] [--clean-output]"
        )

    clean_output = False
    positional: list[str] = []
    for arg in argv[1:]:
        if arg == "--clean-output":
            clean_output = True
        elif arg.startswith("--"):
            raise ValueError(f"Unknown option: {arg}")
        else:
            positional.append(arg)

    if len(positional) < 2 or len(positional) > 3:
        raise ValueError(
            "Usage: python convert_for_spark2.py <input_dir> <output_dir> [limit] [--clean-output]"
        )

    in_dir = Path(positional[0]).resolve()
    out_dir = Path(positional[1]).resolve()
    limit: int | None = None
    if len(positional) == 3:
        limit = int(positional[2])
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")

    return in_dir, out_dir, limit, clean_output


def main(argv: list[str] | None = None) -> int:
    try:
        in_dir, out_dir, limit, clean_output = _parse_args(sys.argv if argv is None else argv)
        _validate_paths(in_dir, out_dir, clean_output)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    all_shards = sorted(in_dir.glob("*.parquet"))
    if not all_shards:
        print(f"Error: No .parquet shards found in input directory: {in_dir}", file=sys.stderr)
        return 1

    shards = all_shards[:limit] if limit is not None else all_shards
    source_hashes = {src.name: _sha256_file(src) for src in shards}

    _prepare_output_dir(out_dir, clean_output)

    total_rows = 0
    for i, src in enumerate(shards, 1):
        table = pq.read_table(src, columns=P01_COLUMNS)
        dst = out_dir / src.name
        pq.write_table(
            table,
            dst,
            compression="snappy",
            version="1.0",
        )
        total_rows += table.num_rows
        if i % 20 == 0 or i == len(shards):
            print(f"[{i}/{len(shards)}] converted {src.name} (rows={table.num_rows})")

    actual_out_shards = sorted(p.name for p in out_dir.glob("*.parquet"))
    expected_out_shards = sorted(src.name for src in shards)
    assert actual_out_shards == expected_out_shards, (
        f"Output shard mismatch: expected {expected_out_shards}, got {actual_out_shards}"
    )

    manifest = {
        "input_dir": str(in_dir),
        "output_dir": str(out_dir),
        "limit": limit,
        "shard_count": len(shards),
        "total_rows": total_rows,
        "shards": [
            {
                "name": src.name,
                "source_sha256": source_hashes[src.name],
                "output_sha256": _sha256_file(out_dir / src.name),
            }
            for src in shards
        ],
    }
    (out_dir / "_conversion_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"Done! Converted {len(shards)} shard(s), total rows = {total_rows:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())