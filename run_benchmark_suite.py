#!/usr/bin/env python3
"""
Run Hudi read benchmarks for multiple table types and Hudi versions, then write results to CSV.
Each run increments a persistent sequence number (how many times this script has been run).

Iteration count comes from env READ_PERFORMANCE_ITERATIONS (exported from common.properties) or from
scripts/common.properties, default 1. Each (hudi_version, iteration) produces
one CSV row with an iteration column.

Usage:
  python run_benchmark_suite.py \
    --table-types MERGE_ON_READ \
    --hudi-versions 0.14.1,0.14.2 \
    --output results/benchmark_results.csv

Output:
  - CSV columns include run_sequence, table_type, hudi_version, batch_id, iteration, execution_time_seconds, ...
  - Sequence file: benchmark_run_sequence.txt
"""

import argparse
import csv
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_TABLE_TYPES = ["COPY_ON_WRITE", "MERGE_ON_READ"]
DEFAULT_HUDI_VERSIONS = ["0.14.1", "0.14.2"]
SEQUENCE_FILENAME = "benchmark_run_sequence.txt"
DEFAULT_CSV = "hudi_benchmark_results.csv"
CSV_HEADER = [
    "run_sequence",
    "table_type",
    "hudi_version",
    "batch_id",
    "iteration",
    "execution_time_seconds",
    "count",
    "run_timestamp_utc",
    "start_time",
    "end_time",
    "status",
]
SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


def read_and_increment_sequence() -> int:
    """Read current sequence from file, increment, write back, return new value."""
    seq_file = SCRIPT_DIR / SEQUENCE_FILENAME
    try:
        with open(seq_file) as f:
            n = int(f.read().strip())
    except (FileNotFoundError, ValueError):
        n = 0
    n += 1
    with open(seq_file, "w") as f:
        f.write(str(n))
    return n


def read_iteration_count(cli_override: Optional[int]) -> int:
    if cli_override is not None and cli_override >= 1:
        return cli_override
    raw = (
        os.environ.get("READ_PERFORMANCE_ITERATIONS")
        or os.environ.get("read_performance_iterations")
        or ""
    ).strip()
    if not raw:
        props = SCRIPT_DIR / "scripts" / "common.properties"
        if props.is_file():
            try:
                text = props.read_text(encoding="utf-8", errors="replace")
            except OSError:
                text = ""
            for line in text.splitlines():
                line = line.split("#")[0].strip()
                if not line or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                k = key.strip()
                if k in ("READ_PERFORMANCE_ITERATIONS", "read_performance_iterations"):
                    raw = val.strip()
                    break
    if not raw:
        raw = "1"
    try:
        n = int(float(raw))
    except ValueError:
        n = 1
    return max(1, n)


def read_existing_benchmark_rows(path: Path) -> List[Dict[str, str]]:
    """Load rows and normalize to CSV_HEADER; legacy rows without iteration default to 1."""
    if not path.is_file():
        return []
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        raw_rows = list(csv.DictReader(f))
    out: List[Dict[str, str]] = []
    for row in raw_rows:
        merged = {k: str(row.get(k) or "").strip() for k in CSV_HEADER}
        if not merged["iteration"]:
            merged["iteration"] = "1"
        out.append(merged)
    return out


def write_benchmark_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADER, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in CSV_HEADER})


def run_benchmark(
    table_type: str,
    hudi_version: str,
    batch_id: int,
    table_name_suffix: str = "",
) -> Tuple[Optional[float], Optional[int], str]:
    """
    Run run_hudi_benchmark.sh and parse output.
    Returns (execution_time_seconds, count, status).
    status is 'ok' or error message.
    """
    script = SCRIPT_DIR / "run_hudi_benchmark.sh"
    if not script.exists():
        return None, None, f"script not found: {script}"

    cmd = [
        "bash",
        str(script),
        "--table-type",
        table_type,
        "--target-hudi-version",
        hudi_version,
        "--batch-id",
        str(batch_id),
    ]
    if table_name_suffix:
        cmd.extend(["--table-name-suffix", table_name_suffix])
    print("Running the command: ", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd,
            cwd=str(SCRIPT_DIR),
            capture_output=True,
            text=True,
            timeout=3600,
        )
        out = result.stdout + "\n" + result.stderr
    except subprocess.TimeoutExpired:
        return None, None, "timeout"
    except Exception as e:
        return None, None, str(e)

    print("Command output: ", out)
    print("Command return code: ", result.returncode)

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
        description="Run Hudi benchmarks for multiple Hudi versions, write results to CSV with run sequence.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--table-type",
        type=str,
        default="COPY_ON_WRITE",
        help="Table type, e.g. COPY_ON_WRITE,MERGE_ON_READ",
    )
    parser.add_argument(
        "--hudi-versions",
        type=str,
        default=",".join(DEFAULT_HUDI_VERSIONS),
        help="Comma-separated Hudi versions, e.g. 0.14.1,0.14.2 (default matches common.properties).",
    )
    parser.add_argument(
        "--batch-id",
        type=int,
        default=0,
        help="Batch ID (default 0).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(SCRIPT_DIR / DEFAULT_CSV),
        help="Output CSV path (existing rows preserved; file rewritten with current schema).",
    )
    parser.add_argument(
        "--table-name-suffix",
        type=str,
        default="",
        help="Optional suffix for Hudi table path (e.g. baseline / experiment).",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=None,
        help="Override READ_PERFORMANCE_ITERATIONS (default: env or common.properties, min 1).",
    )

    args = parser.parse_args()
    if not args.hudi_versions:
        print("❌ Error: --hudi-versions is required", file=sys.stderr)
        return 1
    if not args.table_type:
        print("❌ Error: --table-type is required", file=sys.stderr)
        return 1
    hudi_versions = [v.strip() for v in args.hudi_versions.split(",") if v.strip()]
    iterations = read_iteration_count(args.iterations)
    run_sequence = read_and_increment_sequence()
    batch_id = args.batch_id
    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = SCRIPT_DIR / output_path

    existing = read_existing_benchmark_rows(output_path)
    new_rows: List[Dict[str, Any]] = []
    any_failed = False

    for hudi_version in hudi_versions:
        for iteration in range(1, iterations + 1):
            ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            print(
                f"[Run #{run_sequence}] iteration {iteration}/{iterations} "
                f"{args.table_type} @ {hudi_version} ...",
                flush=True,
            )
            t0 = time.time()
            exec_time, count, status = run_benchmark(
                args.table_type, hudi_version, batch_id, args.table_name_suffix
            )
            t1 = time.time()
            start_str = datetime.fromtimestamp(t0).strftime("%Y-%m-%d %H:%M:%S")
            end_str = datetime.fromtimestamp(t1).strftime("%Y-%m-%d %H:%M:%S")
            row = {
                "run_sequence": run_sequence,
                "table_type": args.table_type,
                "hudi_version": hudi_version,
                "batch_id": batch_id,
                "iteration": iteration,
                "execution_time_seconds": exec_time if exec_time is not None else "",
                "count": count if count is not None else "",
                "run_timestamp_utc": ts_utc,
                "start_time": start_str,
                "end_time": end_str,
                "status": status,
            }
            new_rows.append(row)
            if status != "ok":
                any_failed = True
            if exec_time is not None and count is not None:
                print(f"  -> {exec_time:.2f}s, count={count}, status={status}")
            else:
                print(f"  -> status={status}")

    write_benchmark_csv(output_path, existing + new_rows)

    print(f"\nRun sequence for this suite: {run_sequence}")
    print(f"Iterations per version: {iterations}")
    print(f"Results written to: {output_path}")
    return 1 if any_failed else 0


if __name__ == "__main__":
    sys.exit(main())
