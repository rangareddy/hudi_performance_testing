#!/usr/bin/env python3
"""
Aggregate read/write CSVs from baseline vs experiment E2E phases and emit a comparison report.
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def _float_or_zero(val: str) -> float:
    val = (val or "").strip()
    if not val:
        return 0.0
    try:
        return float(val)
    except ValueError:
        return 0.0


def sum_read_seconds(path: Path) -> Tuple[float, int]:
    """Sum execution_time_seconds for rows with status ok."""
    if not path.is_file():
        return 0.0, 0
    total = 0.0
    n = 0
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            if (row.get("status") or "").strip() != "ok":
                continue
            sec = _float_or_zero(row.get("execution_time_seconds", ""))
            total += sec
            n += 1
    return total, n


def sum_write_seconds(path: Path, operation_filter: Optional[str] = None) -> Tuple[float, int]:
    """Sum execution_time_seconds for rows with status ok; optional operation substring filter."""
    if not path.is_file():
        return 0.0, 0
    total = 0.0
    n = 0
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            if (row.get("status") or "").strip() != "ok":
                continue
            op = (row.get("operation") or "").strip()
            if operation_filter and operation_filter not in op:
                continue
            sec = _float_or_zero(row.get("execution_time_seconds", ""))
            total += sec
            n += 1
    return total, n


def pct_delta(baseline: float, experiment: float) -> str:
    if baseline == 0:
        return "" if experiment == 0 else "n/a"
    return f"{100.0 * (experiment - baseline) / baseline:.2f}"


def main() -> int:
    p = argparse.ArgumentParser(description="Compare baseline vs experiment E2E read/write CSVs.")
    p.add_argument("--baseline-read", type=Path, required=True)
    p.add_argument("--experiment-read", type=Path, required=True)
    p.add_argument("--baseline-write", type=Path, required=True)
    p.add_argument("--experiment-write", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument(
        "--baseline-read-post",
        type=Path,
        default=None,
        help="Optional MOR post-compaction read benchmark CSV (baseline phase).",
    )
    p.add_argument(
        "--experiment-read-post",
        type=Path,
        default=None,
        help="Optional MOR post-compaction read benchmark CSV (experiment phase).",
    )
    args = p.parse_args()

    rows: List[Dict[str, str]] = []

    br, br_n = sum_read_seconds(args.baseline_read)
    er, er_n = sum_read_seconds(args.experiment_read)
    rows.append(
        {
            "metric": "read_benchmark_total_seconds",
            "baseline_value": f"{br:.4f}",
            "experiment_value": f"{er:.4f}",
            "delta_seconds": f"{er - br:.4f}",
            "delta_percent": pct_delta(br, er),
            "baseline_row_count_ok": str(br_n),
            "experiment_row_count_ok": str(er_n),
        }
    )

    bw, bw_n = sum_write_seconds(args.baseline_write)
    ew, ew_n = sum_write_seconds(args.experiment_write)
    rows.append(
        {
            "metric": "write_total_seconds",
            "baseline_value": f"{bw:.4f}",
            "experiment_value": f"{ew:.4f}",
            "delta_seconds": f"{ew - bw:.4f}",
            "delta_percent": pct_delta(bw, ew),
            "baseline_row_count_ok": str(bw_n),
            "experiment_row_count_ok": str(ew_n),
        }
    )

    bp, _ = sum_write_seconds(args.baseline_write, "parquet")
    ep, _ = sum_write_seconds(args.experiment_write, "parquet")
    rows.append(
        {
            "metric": "write_parquet_seconds",
            "baseline_value": f"{bp:.4f}",
            "experiment_value": f"{ep:.4f}",
            "delta_seconds": f"{ep - bp:.4f}",
            "delta_percent": pct_delta(bp, ep),
            "baseline_row_count_ok": "",
            "experiment_row_count_ok": "",
        }
    )

    bh, _ = sum_write_seconds(args.baseline_write, "hudi_delta_streamer")
    eh, _ = sum_write_seconds(args.experiment_write, "hudi_delta_streamer")
    rows.append(
        {
            "metric": "write_hudi_delta_streamer_seconds",
            "baseline_value": f"{bh:.4f}",
            "experiment_value": f"{eh:.4f}",
            "delta_seconds": f"{eh - bh:.4f}",
            "delta_percent": pct_delta(bh, eh),
            "baseline_row_count_ok": "",
            "experiment_row_count_ok": "",
        }
    )

    bc, _ = sum_write_seconds(args.baseline_write, "hudi_compaction")
    ec, _ = sum_write_seconds(args.experiment_write, "hudi_compaction")
    rows.append(
        {
            "metric": "write_hudi_compaction_seconds",
            "baseline_value": f"{bc:.4f}",
            "experiment_value": f"{ec:.4f}",
            "delta_seconds": f"{ec - bc:.4f}",
            "delta_percent": pct_delta(bc, ec),
            "baseline_row_count_ok": "",
            "experiment_row_count_ok": "",
        }
    )

    if args.baseline_read_post and args.experiment_read_post:
        if args.baseline_read_post.is_file() and args.experiment_read_post.is_file():
            bpr, bpr_n = sum_read_seconds(args.baseline_read_post)
            epr, epr_n = sum_read_seconds(args.experiment_read_post)
            rows.append(
                {
                    "metric": "read_benchmark_post_compaction_total_seconds",
                    "baseline_value": f"{bpr:.4f}",
                    "experiment_value": f"{epr:.4f}",
                    "delta_seconds": f"{epr - bpr:.4f}",
                    "delta_percent": pct_delta(bpr, epr),
                    "baseline_row_count_ok": str(bpr_n),
                    "experiment_row_count_ok": str(epr_n),
                }
            )
        else:
            print(
                "Note: post-compaction read CSVs not found; skipping read_benchmark_post_compaction_total_seconds",
                file=sys.stderr,
            )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "metric",
        "baseline_value",
        "experiment_value",
        "delta_seconds",
        "delta_percent",
        "baseline_row_count_ok",
        "experiment_row_count_ok",
    ]
    with open(args.output, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"Comparison written to: {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
