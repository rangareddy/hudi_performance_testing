#!/usr/bin/env python3
"""
Run Hudi read benchmarks for multiple table types and Hudi versions, then write results to CSV.
Each run increments a persistent sequence number (how many times this script has been run).

E2E passes a shared --run-sequence across READ_PERFORMANCE_ITERATIONS invocations; per-iteration
rows include --iteration. After the last iteration, when iterations > 1, summary rows with
read_aggregate=avg (mean execution_time_seconds per Hudi version) are appended.

Usage:
  python run_benchmark_suite.py \
    --table-types MERGE_ON_READ \
    --hudi-versions 0.14.1,0.14.2 \
    --output results/benchmark_results.csv

Output:
  - CSV columns include run_sequence, iteration, read_aggregate, execution_time_seconds, ...
  - Sequence file: benchmark_run_sequence.txt
"""

import argparse
import csv
import os
import re
import statistics
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
    "read_aggregate",
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


def run_benchmark(
    table_type: str,
    hudi_version: str,
    batch_id: int,
    table_name_suffix: Optional[str] = None,
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
        "--table-type", table_type,
        "--target-hudi-version", hudi_version,
        "--batch-id", str(batch_id),
    ]
    if table_name_suffix:
        cmd.extend(["--table-name-suffix", table_name_suffix])

    print("Running the command: ", " ".join(cmd), flush=True)
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

    print("Command output: ", out, flush=True)
    print("Command return code: ", result.returncode, flush=True)

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


def _int_field(val: Any, default: int = 0) -> int:
    if val is None or val == "":
        return default
    try:
        return int(str(val).strip())
    except ValueError:
        return default


def _float_field(val: Any) -> Optional[float]:
    if val is None or val == "":
        return None
    try:
        return float(str(val).strip())
    except ValueError:
        return None


def append_read_average_rows(
    output_path: Path,
    *,
    run_sequence: int,
    batch_id: int,
    table_type: str,
    total_iterations: int,
) -> None:
    """
    Append one row per hudi_version with read_aggregate=avg (mean execution time over iterations).
    Requires CSV rows with iteration 1..total_iterations, same run_sequence/batch_id/table_type, status ok.
    Skips if 'iteration' column is missing (legacy CSV) or avg rows already exist for this key.
    """
    if total_iterations <= 1:
        return

    with open(output_path, newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        if not fieldnames or "iteration" not in fieldnames:
            print(
                "⚠ Skipping average rows: CSV has no 'iteration' column (legacy file).",
                file=sys.stderr,
                flush=True,
            )
            return
        rows = list(reader)

    # Drop existing avg rows for this logical run (re-entrant last iteration)
    rows = [
        r
        for r in rows
        if not (
            (r.get("read_aggregate") or "").strip() == "avg"
            and _int_field(r.get("run_sequence")) == run_sequence
            and _int_field(r.get("batch_id")) == batch_id
        )
    ]

    # Collect per-version times from iteration rows (exactly one row per iteration 1..N)
    by_version: Dict[str, Dict[int, float]] = {}
    counts_by_version: Dict[str, int] = {}
    expected_it = set(range(1, total_iterations + 1))
    for r in rows:
        if (r.get("read_aggregate") or "").strip() == "avg":
            continue
        if (r.get("status") or "").strip() != "ok":
            continue
        if _int_field(r.get("run_sequence")) != run_sequence:
            continue
        if _int_field(r.get("batch_id")) != batch_id:
            continue
        if (r.get("table_type") or "").strip() != table_type.strip():
            continue
        it = _int_field(r.get("iteration"), -1)
        if it < 1 or it > total_iterations:
            continue
        sec = _float_field(r.get("execution_time_seconds"))
        if sec is None:
            continue
        hv = (r.get("hudi_version") or "").strip()
        if not hv:
            continue
        by_version.setdefault(hv, {})[it] = sec
        if hv not in counts_by_version:
            cnt = _int_field(r.get("count"), 0)
            if cnt:
                counts_by_version[hv] = cnt

    avg_rows: List[Dict[str, Any]] = []
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    for hv in sorted(by_version):
        by_it = by_version[hv]
        if set(by_it.keys()) != expected_it:
            print(
                f"⚠ Skipping avg for {hv}: expected iterations {sorted(expected_it)}, got {sorted(by_it.keys())}",
                file=sys.stderr,
                flush=True,
            )
            continue
        times = [by_it[i] for i in range(1, total_iterations + 1)]
        mean_sec = statistics.mean(times)
        cnt = counts_by_version.get(hv, 0)
        avg_rows.append(
            {
                "run_sequence": run_sequence,
                "table_type": table_type,
                "hudi_version": hv,
                "batch_id": batch_id,
                "iteration": "",
                "read_aggregate": "avg",
                "execution_time_seconds": round(mean_sec, 4),
                "count": cnt if cnt else "",
                "run_timestamp_utc": ts,
                "start_time": "",
                "end_time": "",
                "status": "ok",
            }
        )

    if not avg_rows:
        return

    with open(output_path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADER)
        w.writerows(avg_rows)

    print("", flush=True)
    print(
        f"Average read performance (run_sequence={run_sequence}, batch_id={batch_id}, "
        f"over {total_iterations} iterations):",
        flush=True,
    )
    for r in avg_rows:
        hv = r["hudi_version"]
        mean_sec = float(r["execution_time_seconds"])
        by_it = by_version[hv]
        times = [by_it[i] for i in range(1, total_iterations + 1)]
        times_fmt = ", ".join(f"{t:.2f}s" for t in times)
        print(f"  {hv}  mean={mean_sec:.2f}s  (iterations: {times_fmt})", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Hudi benchmarks for multiple table types and versions, write results to CSV with run sequence.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--allocate-run-sequence-only",
        action="store_true",
        help="Allocate and print next run_sequence to stdout only (for E2E bash).",
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
        "--iteration",
        type=int,
        default=1,
        help="Read benchmark iteration index (1-based; E2E sets per loop).",
    )
    parser.add_argument(
        "--run-sequence",
        type=int,
        default=None,
        help="Fixed run_sequence for this write (E2E); if omitted, allocate from sequence file.",
    )
    parser.add_argument(
        "--read-performance-iterations",
        type=int,
        default=None,
        help="Total read iterations for this block (default: READ_PERFORMANCE_ITERATIONS env or 1).",
    )
    parser.add_argument(
        "--table-name-suffix",
        type=str,
        default=None,
        help="Passed to run_hudi_benchmark.sh (e.g. baseline / experiment).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(SCRIPT_DIR / DEFAULT_CSV),
        help="Output CSV path (created or appended).",
    )

    args = parser.parse_args()

    if args.allocate_run_sequence_only:
        seq = read_and_increment_sequence()
        print(seq, flush=True)
        return 0

    if not args.hudi_versions:
        print("❌ Error: --hudi-versions is required", file=sys.stderr)
        return 1
    if not args.table_type:
        print("❌ Error: --table-type is required", file=sys.stderr)
        return 1

    total_iterations = args.read_performance_iterations
    if total_iterations is None:
        try:
            total_iterations = int(os.environ.get("READ_PERFORMANCE_ITERATIONS", "1"))
        except ValueError:
            total_iterations = 1
    if total_iterations < 1:
        total_iterations = 1

    hudi_versions = [v.strip() for v in args.hudi_versions.split(",") if v.strip()]
    if args.run_sequence is not None:
        run_sequence = args.run_sequence
    else:
        run_sequence = read_and_increment_sequence()

    batch_id = args.batch_id
    iteration = args.iteration
    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = SCRIPT_DIR / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    file_existed = output_path.exists()

    print(
        f"[Run #{run_sequence} iteration {iteration}/{total_iterations}] "
        f"{args.table_type} @ {hudi_versions} ...",
        flush=True,
    )

    rows = []  # type: List[Dict[str, Any]]
    for hudi_version in hudi_versions:
        print(f"  Hudi {hudi_version} ...", flush=True)
        start_wall = time.time()
        exec_time, count, status = run_benchmark(
            args.table_type,
            hudi_version,
            batch_id,
            table_name_suffix=args.table_name_suffix,
        )
        end_wall = time.time()
        start_str = datetime.fromtimestamp(start_wall).strftime("%Y-%m-%d %H:%M:%S")
        end_str = datetime.fromtimestamp(end_wall).strftime("%Y-%m-%d %H:%M:%S")
        row = {
            "run_sequence": run_sequence,
            "table_type": args.table_type,
            "hudi_version": hudi_version,
            "batch_id": batch_id,
            "iteration": iteration,
            "read_aggregate": "",
            "execution_time_seconds": exec_time if exec_time is not None else "",
            "count": count if count is not None else "",
            "run_timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "start_time": start_str,
            "end_time": end_str,
            "status": status,
        }
        rows.append(row)
        if exec_time is not None and count is not None:
            print(
                f"    iteration {iteration}/{total_iterations}: {exec_time:.2f}s, count={count}, status={status}",
                flush=True,
            )
        else:
            print(f"    iteration {iteration}/{total_iterations}: status={status}", flush=True)

    with open(output_path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADER)
        if not file_existed:
            w.writeheader()
        w.writerows(rows)

    if total_iterations > 1 and iteration == total_iterations:
        append_read_average_rows(
            output_path,
            run_sequence=run_sequence,
            batch_id=batch_id,
            table_type=args.table_type,
            total_iterations=total_iterations,
        )

    print(f"\nRun sequence for this suite: {run_sequence}", flush=True)
    print(f"Results appended to: {output_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
