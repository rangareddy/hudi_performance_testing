#!/usr/bin/env python3
"""
Run Hudi read benchmarks for multiple table types and Hudi versions, then write results to CSV.
Each run increments a persistent sequence number (how many times this script has been run).

Usage:
  python run_benchmark_suite.py
  python run_benchmark_suite.py --table-types MERGE_ON_READ --hudi-versions 0.14.1,0.14.2
  python run_benchmark_suite.py --output results/benchmark_results.csv

Output:
  - CSV with columns: run_sequence, table_type, hudi_version, execution_time_seconds, count, run_timestamp_utc
  - Sequence file: benchmark_run_sequence.txt (in project_dir)
"""

import argparse
import csv
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


# Default combinations (matches README)
DEFAULT_TABLE_TYPES = ["COPY_ON_WRITE", "MERGE_ON_READ"]
DEFAULT_HUDI_VERSIONS = ["0.14.1", "0.14.2"]
SEQUENCE_FILENAME = "benchmark_run_sequence.txt"
DEFAULT_CSV = "hudi_benchmark_results.csv"
CSV_HEADER = [
    "run_sequence",
    "table_type",
    "hudi_version",
    "execution_time_seconds",
    "count",
    "run_timestamp_utc",
    "status",
]


def get_project_dir(project_dir: str | None) -> Path:
    """Project root where test_hudi_benchmark.sh and load_config.sh live."""
    if project_dir:
        p = Path(project_dir).resolve()
        if not p.is_dir():
            raise FileNotFoundError(f"Project dir not found: {p}")
        return p
    # Assume script is in project root or in a subdir
    script_path = Path(__file__).resolve()
    for d in [script_path.parent, Path.cwd()]:
        if (d / "test_hudi_benchmark.sh").exists() and (d / "load_config.sh").exists():
            return d
    return Path.cwd()


def read_and_increment_sequence(project_dir: Path) -> int:
    """Read current sequence from file, increment, write back, return new value."""
    seq_file = project_dir / SEQUENCE_FILENAME
    try:
        with open(seq_file) as f:
            n = int(f.read().strip())
    except (FileNotFoundError, ValueError):
        n = 0
    n += 1
    with open(seq_file, "w") as f:
        f.write(str(n))
    return n


def run_benchmark(project_dir: Path, table_type: str, hudi_version: str) -> tuple[float | None, int | None, str]:
    """
    Run test_hudi_benchmark.sh and parse output.
    Returns (execution_time_seconds, count, status).
    status is 'ok' or error message.
    """
    script = project_dir / "test_hudi_benchmark.sh"
    if not script.exists():
        return None, None, f"script not found: {script}"

    cmd = [
        "bash",
        str(script),
        "--table-type", table_type,
        "--target-hudi-version", hudi_version,
    ]
    try:
        result = subprocess.run(
            cmd,
            cwd=str(project_dir),
            capture_output=True,
            text=True,
            timeout=3600,
        )
        out = result.stdout + "\n" + result.stderr
    except subprocess.TimeoutExpired:
        return None, None, "timeout"
    except Exception as e:
        return None, None, str(e)

    # Parse: "Total execution time: 12.34 seconds" and "Execution Complete. Count: 12345"
    time_match = re.search(r"Total execution time:\s*([\d.]+)\s*seconds", out)
    count_match = re.search(r"Execution Complete\.\s*Count:\s*(\d+)", out)

    exec_time = float(time_match.group(1)) if time_match else None
    count = int(count_match.group(1)) if count_match else None

    if result.returncode != 0:
        status = f"exit_code_{result.returncode}"
    elif exec_time is None or count is None:
        status = "parse_failed"
    else:
        status = "ok"

    return exec_time, count, status


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Hudi benchmarks for multiple table types and versions, write results to CSV with run sequence.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--project-dir",
        type=str,
        default=None,
        help="Project root (where test_hudi_benchmark.sh and load_config.sh are). Default: auto-detect or cwd.",
    )
    parser.add_argument(
        "--table-types",
        type=str,
        default=",".join(DEFAULT_TABLE_TYPES),
        help="Comma-separated table types, e.g. COPY_ON_WRITE,MERGE_ON_READ",
    )
    parser.add_argument(
        "--hudi-versions",
        type=str,
        default=",".join(DEFAULT_HUDI_VERSIONS),
        help="Comma-separated Hudi versions, e.g. 0.14.1,0.14.2",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=DEFAULT_CSV,
        help="Output CSV path (created or appended).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print what would be run and exit.",
    )
    args = parser.parse_args()

    project_dir = get_project_dir(args.project_dir)
    table_types = [t.strip().upper() for t in args.table_types.split(",") if t.strip()]
    hudi_versions = [v.strip() for v in args.hudi_versions.split(",") if v.strip()]

    if not table_types or not hudi_versions:
        print("Need at least one table type and one hudi version.", file=sys.stderr)
        return 1

    run_sequence = read_and_increment_sequence(project_dir)
    run_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")

    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = project_dir / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    file_existed = output_path.exists()

    if args.dry_run:
        print(f"Project dir: {project_dir}")
        print(f"Run sequence: {run_sequence}")
        print(f"Table types: {table_types}")
        print(f"Hudi versions: {hudi_versions}")
        print(f"Output CSV: {output_path}")
        for tt in table_types:
            for hv in hudi_versions:
                print(f"  Would run: test_hudi_benchmark.sh --table-type {tt} --target-hudi-version {hv}")
        return 0

    rows: list[dict[str, str | int | float | None]] = []

    for table_type in table_types:
        for hudi_version in hudi_versions:
            print(f"[Run #{run_sequence}] {table_type} @ {hudi_version} ...", flush=True)
            exec_time, count, status = run_benchmark(project_dir, table_type, hudi_version)
            row = {
                "run_sequence": run_sequence,
                "table_type": table_type,
                "hudi_version": hudi_version,
                "execution_time_seconds": exec_time if exec_time is not None else "",
                "count": count if count is not None else "",
                "run_timestamp_utc": run_ts,
                "status": status,
            }
            rows.append(row)
            if exec_time is not None and count is not None:
                print(f"  -> {exec_time:.2f}s, count={count}, status={status}")
            else:
                print(f"  -> status={status}")

    with open(output_path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADER)
        if not file_existed:
            w.writeheader()
        w.writerows(rows)

    print(f"\nRun sequence for this suite: {run_sequence}")
    print(f"Results appended to: {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
