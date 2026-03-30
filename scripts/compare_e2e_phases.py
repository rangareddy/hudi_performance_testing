#!/usr/bin/env python3
"""
Aggregate read/write CSVs from baseline vs experiment E2E phases.

Writes:
  1) Summary CSV: rolled-up metrics (--output, or auto per stem with --report-dir / --report-root).
  2) Per-batch Markdown + CSV tables: write (hudi_delta_streamer only) and read
     (execution_time_seconds), with diff (s) and % difference. Lower time is better.

Report layout:
  --report-dir BUNDLE   expects BUNDLE/read/ and BUNDLE/write/ (hudi_benchmark_results_*,
                        hudi_write_performance_*).
  --report-root ROOT    processes each immediate subdirectory that has read/ and write/.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

READ_CSV_PREFIX = "hudi_benchmark_results_"
READ_CSV_BASELINE_SUFFIX = "_baseline.csv"
WRITE_CSV_PREFIX = "hudi_write_performance_"


def discover_read_stems(read_dir: Path) -> List[str]:
    """Stems shared by read/write CSVs, e.g. cow_false_0_15 (excludes post_compact-only names)."""
    stems: set[str] = set()
    if not read_dir.is_dir():
        return []
    for p in read_dir.glob(f"{READ_CSV_PREFIX}*{READ_CSV_BASELINE_SUFFIX}"):
        if "post_compact" in p.name:
            continue
        name = p.name
        if not name.endswith(READ_CSV_BASELINE_SUFFIX):
            continue
        stem = name[len(READ_CSV_PREFIX) : -len(READ_CSV_BASELINE_SUFFIX)]
        if stem:
            stems.add(stem)
    return sorted(stems)


def iter_report_bundles(report_root: Path) -> List[Tuple[Path, Path, Path]]:
    """Immediate subdirs that contain both read/ and write/."""
    out: List[Tuple[Path, Path, Path]] = []
    if not report_root.is_dir():
        return out
    for child in sorted(report_root.iterdir()):
        if not child.is_dir():
            continue
        rd, wd = child / "read", child / "write"
        if rd.is_dir() and wd.is_dir():
            out.append((child, rd, wd))
    return out


def _float_or_zero(val: str) -> float:
    val = (val or "").strip()
    if not val:
        return 0.0
    try:
        return float(val)
    except ValueError:
        return 0.0


def _int_or_zero(val: str) -> int:
    val = (val or "").strip()
    if not val:
        return 0
    try:
        return int(float(val))
    except ValueError:
        return 0


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


def env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(float(raw))
    except ValueError:
        return default


def short_table_type(table_type: str) -> str:
    t = (table_type or "").upper()
    if "MERGE" in t:
        return "MOR"
    return "COW"


def batch_label(batch_id: int) -> str:
    if batch_id == 0:
        return "Initial Ingestion"
    return f"Incremental Batch{batch_id}"


def batch_size_for(batch_id: int, initial_size: int, incremental_size: int) -> int:
    return initial_size if batch_id == 0 else incremental_size


def load_hudi_delta_streamer_by_batch(path: Path) -> Dict[int, float]:
    """Write perf: only operation == hudi_delta_streamer, status ok; batch_id -> seconds."""
    out: Dict[int, float] = {}
    if not path.is_file():
        return out
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            if (row.get("status") or "").strip() != "ok":
                continue
            if (row.get("operation") or "").strip() != "hudi_delta_streamer":
                continue
            bid = _int_or_zero(row.get("batch_id", ""))
            out[bid] = _float_or_zero(row.get("execution_time_seconds", ""))
    return out


def load_read_by_batch(path: Path) -> Dict[int, Tuple[float, int]]:
    """Read benchmark: execution_time_seconds and count for status ok; last row wins per batch_id."""
    best: Dict[int, Tuple[int, float, int]] = {}  # batch_id -> (run_sequence, sec, count)
    if not path.is_file():
        return {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            if (row.get("status") or "").strip() != "ok":
                continue
            bid = _int_or_zero(row.get("batch_id", ""))
            seq = _int_or_zero(row.get("run_sequence", "0"))
            sec = _float_or_zero(row.get("execution_time_seconds", ""))
            cnt = _int_or_zero(row.get("count", "0"))
            prev = best.get(bid)
            if prev is None or seq >= prev[0]:
                best[bid] = (seq, sec, cnt)
    return {k: (v[1], v[2]) for k, v in best.items()}


def hudi_version_label(path: Path) -> str:
    if not path.is_file():
        return "unknown"
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            if (row.get("status") or "").strip() != "ok":
                continue
            v = (row.get("hudi_version") or "").strip()
            if v:
                return v
    return "unknown"


def table_type_from_read_csv(path: Path) -> str:
    if not path.is_file():
        return "COPY_ON_WRITE"
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            t = (row.get("table_type") or "").strip()
            if t:
                return t
    return "COPY_ON_WRITE"


def fmt_time(v: float, as_int_if_whole: bool = False) -> str:
    if as_int_if_whole and abs(v - round(v)) < 1e-6:
        return str(int(round(v)))
    return f"{v:.2f}"


def md_bold_lower_better(baseline: float, experiment: float, use_baseline_col: bool) -> str:
    """Lower seconds is better; bold the winning cell."""
    if baseline <= 0 and experiment <= 0:
        return "-"
    b_win = baseline < experiment
    e_win = experiment < baseline
    if use_baseline_col:
        val = fmt_time(baseline, as_int_if_whole=True)
        return f"**{val}**" if b_win and not e_win else val
    val = fmt_time(experiment, as_int_if_whole=True)
    return f"**{val}**" if e_win and not b_win else val


def write_per_batch_tables(
    baseline_write: Path,
    experiment_write: Path,
    baseline_read: Path,
    experiment_read: Path,
    initial_batch_size: int,
    incremental_batch_size: int,
    md_path: Path,
    csv_detail_path: Path,
    baseline_read_post: Optional[Path],
    experiment_read_post: Optional[Path],
) -> None:
    tt = table_type_from_read_csv(baseline_read)
    tt_short = short_table_type(tt)
    bv = hudi_version_label(baseline_read)
    ev = hudi_version_label(experiment_read)

    w_base = load_hudi_delta_streamer_by_batch(baseline_write)
    w_exp = load_hudi_delta_streamer_by_batch(experiment_write)
    r_base = load_read_by_batch(baseline_read)
    r_exp = load_read_by_batch(experiment_read)

    batch_ids = [0, 1, 2, 3, 4]
    write_csv_rows: List[Dict[str, str]] = []
    read_csv_rows: List[Dict[str, str]] = []

    md_lines: List[str] = [
        "# Hudi performance: baseline vs experiment",
        "",
        f"**Table type:** {tt_short} ({tt})",
        "",
        "## Hudi write performance (Delta Streamer / `hudi_delta_streamer` only)",
        "",
        f"| Table Type | Batch Info | Batch Size | Baseline ({bv}) | Experiment ({ev}) | Diff (s) | Diff (%) |",
        "|------------|------------|------------|-----------------|-------------------|----------|----------|",
    ]

    for bid in batch_ids:
        bsz = batch_size_for(bid, initial_batch_size, incremental_batch_size)
        bl = batch_label(bid)
        hb = bid in w_base
        he = bid in w_exp
        wb = w_base[bid] if hb else float("nan")
        we = w_exp[bid] if he else float("nan")
        if not hb or not he:
            diff_s = ""
            diff_p = ""
            bcell = "-" if not hb else fmt_time(wb, as_int_if_whole=True)
            ecell = "-" if not he else fmt_time(we, as_int_if_whole=True)
            bdisp = bcell
            edisp = ecell
        else:
            diff_s = f"{we - wb:.2f}"
            diff_p = pct_delta(wb, we)
            bcell = md_bold_lower_better(wb, we, True)
            ecell = md_bold_lower_better(wb, we, False)
            bdisp = fmt_time(wb, as_int_if_whole=True)
            edisp = fmt_time(we, as_int_if_whole=True)
        md_lines.append(
            f"| {tt_short} | {bl} | {bsz} | {bcell} | {ecell} | {diff_s or '-'} | {diff_p or '-'} |"
        )
        write_csv_rows.append(
            {
                "table_type": tt_short,
                "batch_info": bl,
                "batch_size": str(bsz),
                "baseline_seconds": bdisp.replace("**", ""),
                "experiment_seconds": edisp.replace("**", ""),
                "diff_seconds": diff_s,
                "diff_percent": diff_p,
            }
        )

    md_lines.extend(
        [
            "",
            "## Hudi read performance (`execution_time_seconds`)",
            "",
            f"| Table Type | Batch Info | Baseline ({bv}) | Experiment ({ev}) | Count | Diff (s) | Diff (%) |",
            "|------------|------------|-----------------|-------------------|-------|----------|----------|",
        ]
    )

    for bid in batch_ids:
        bl = batch_label(bid)
        hb = bid in r_base
        he = bid in r_exp
        wb, cb = r_base[bid] if hb else (float("nan"), 0)
        we, ce = r_exp[bid] if he else (float("nan"), 0)
        cnt = (ce or cb) if (hb or he) else 0
        if not hb or not he:
            diff_s = ""
            diff_p = ""
            bcell = "-" if not hb else fmt_time(wb)
            ecell = "-" if not he else fmt_time(we)
            bdisp = bcell
            edisp = ecell
        else:
            diff_s = f"{we - wb:.2f}"
            diff_p = pct_delta(wb, we)
            bcell = md_bold_lower_better(wb, we, True)
            ecell = md_bold_lower_better(wb, we, False)
            bdisp = fmt_time(wb)
            edisp = fmt_time(we)
        md_lines.append(
            f"| {tt_short} | {bl} | {bcell} | {ecell} | {cnt} | {diff_s or '-'} | {diff_p or '-'} |"
        )
        read_csv_rows.append(
            {
                "table_type": tt_short,
                "batch_info": bl,
                "baseline_seconds": bdisp.replace("**", ""),
                "experiment_seconds": edisp.replace("**", ""),
                "count": str(cnt),
                "diff_seconds": diff_s,
                "diff_percent": diff_p,
            }
        )

    # Optional post-compaction read (single logical step, e.g. batch_id 5)
    if (
        baseline_read_post
        and experiment_read_post
        and baseline_read_post.is_file()
        and experiment_read_post.is_file()
    ):
        rpb = load_read_by_batch(baseline_read_post)
        rpe = load_read_by_batch(experiment_read_post)
        post_ids = sorted(set(rpb.keys()) | set(rpe.keys()))
        if post_ids:
            md_lines.extend(["", "## Hudi read performance (post-compaction)", ""])
            md_lines.append(
                f"| Table Type | Batch Info | Baseline ({bv}) | Experiment ({ev}) | Count | Diff (s) | Diff (%) |"
            )
            md_lines.append(
                "|------------|------------|-----------------|-------------------|-------|----------|----------|"
            )
        for bid in post_ids:
            label = f"Post-compaction (batch_id {bid})" if bid != 5 else "Post-compaction read"
            hb = bid in rpb
            he = bid in rpe
            wb, cb = rpb[bid] if hb else (float("nan"), 0)
            we, ce = rpe[bid] if he else (float("nan"), 0)
            cnt = (ce or cb) if (hb or he) else 0
            if not hb or not he:
                diff_s = ""
                diff_p = ""
                bcell = "-" if not hb else fmt_time(wb)
                ecell = "-" if not he else fmt_time(we)
                bdisp = bcell
                edisp = ecell
            else:
                diff_s = f"{we - wb:.2f}"
                diff_p = pct_delta(wb, we)
                bcell = md_bold_lower_better(wb, we, True)
                ecell = md_bold_lower_better(wb, we, False)
                bdisp = fmt_time(wb)
                edisp = fmt_time(we)
            md_lines.append(
                f"| {tt_short} | {label} | {bcell} | {ecell} | {cnt} | {diff_s or '-'} | {diff_p or '-'} |"
            )
            read_csv_rows.append(
                {
                    "table_type": tt_short,
                    "batch_info": label,
                    "baseline_seconds": bdisp.replace("**", ""),
                    "experiment_seconds": edisp.replace("**", ""),
                    "count": str(cnt),
                    "diff_seconds": diff_s,
                    "diff_percent": diff_p,
                }
            )

    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    csv_detail_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_detail_path, "w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "section",
                "table_type",
                "batch_info",
                "batch_size",
                "baseline_seconds",
                "experiment_seconds",
                "count",
                "diff_seconds",
                "diff_percent",
            ],
        )
        w.writeheader()
        for row in write_csv_rows:
            w.writerow(
                {
                    "section": "write_hudi_delta_streamer",
                    "table_type": row["table_type"],
                    "batch_info": row["batch_info"],
                    "batch_size": row["batch_size"],
                    "baseline_seconds": row["baseline_seconds"],
                    "experiment_seconds": row["experiment_seconds"],
                    "count": "",
                    "diff_seconds": row["diff_seconds"],
                    "diff_percent": row["diff_percent"],
                }
            )
        for row in read_csv_rows:
            w.writerow(
                {
                    "section": "read_benchmark",
                    "table_type": row["table_type"],
                    "batch_info": row["batch_info"],
                    "batch_size": "",
                    "baseline_seconds": row["baseline_seconds"],
                    "experiment_seconds": row["experiment_seconds"],
                    "count": row["count"],
                    "diff_seconds": row["diff_seconds"],
                    "diff_percent": row["diff_percent"],
                }
            )

    print(f"Per-batch Markdown report: {md_path}")
    print(f"Per-batch detail CSV:      {csv_detail_path}")


def run_one_comparison(
    baseline_read: Path,
    experiment_read: Path,
    baseline_write: Path,
    experiment_write: Path,
    output: Path,
    baseline_read_post: Optional[Path],
    experiment_read_post: Optional[Path],
    initial_batch_size: int,
    incremental_batch_size: int,
    tables_md: Optional[Path],
    tables_csv: Optional[Path],
) -> None:
    md_path = tables_md
    if md_path is None:
        md_path = output.parent / f"{output.stem}_per_batch_tables.md"
    csv_detail = tables_csv
    if csv_detail is None:
        csv_detail = output.parent / f"{output.stem}_per_batch_tables.csv"

    write_per_batch_tables(
        baseline_write,
        experiment_write,
        baseline_read,
        experiment_read,
        initial_batch_size,
        incremental_batch_size,
        md_path,
        csv_detail,
        baseline_read_post,
        experiment_read_post,
    )

    rows: List[Dict[str, str]] = []

    br, br_n = sum_read_seconds(baseline_read)
    er, er_n = sum_read_seconds(experiment_read)
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

    bw, bw_n = sum_write_seconds(baseline_write)
    ew, ew_n = sum_write_seconds(experiment_write)
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

    bp, _ = sum_write_seconds(baseline_write, "parquet")
    ep, _ = sum_write_seconds(experiment_write, "parquet")
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

    bh, _ = sum_write_seconds(baseline_write, "hudi_delta_streamer")
    eh, _ = sum_write_seconds(experiment_write, "hudi_delta_streamer")
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

    bc, _ = sum_write_seconds(baseline_write, "hudi_compaction")
    ec, _ = sum_write_seconds(experiment_write, "hudi_compaction")
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

    if baseline_read_post and experiment_read_post:
        if baseline_read_post.is_file() and experiment_read_post.is_file():
            bpr, bpr_n = sum_read_seconds(baseline_read_post)
            epr, epr_n = sum_read_seconds(experiment_read_post)
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

    output.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "metric",
        "baseline_value",
        "experiment_value",
        "delta_seconds",
        "delta_percent",
        "baseline_row_count_ok",
        "experiment_row_count_ok",
    ]
    with open(output, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"Comparison summary CSV: {output}")


def process_report_bundle(
    bundle_dir: Path,
    read_dir: Path,
    write_dir: Path,
    initial_batch_size: int,
    incremental_batch_size: int,
) -> int:
    stems = discover_read_stems(read_dir)
    if not stems:
        print(f"No {READ_CSV_PREFIX}*{READ_CSV_BASELINE_SUFFIX} in {read_dir}", file=sys.stderr)
        return 1
    ok = 0
    for stem in stems:
        baseline_read = read_dir / f"{READ_CSV_PREFIX}{stem}{READ_CSV_BASELINE_SUFFIX}"
        experiment_read = read_dir / f"{READ_CSV_PREFIX}{stem}_experiment.csv"
        baseline_write = write_dir / f"{WRITE_CSV_PREFIX}{stem}_baseline.csv"
        experiment_write = write_dir / f"{WRITE_CSV_PREFIX}{stem}_experiment.csv"
        if not baseline_read.is_file() or not experiment_read.is_file():
            print(f"Skip stem {stem}: missing read CSVs", file=sys.stderr)
            continue
        if not baseline_write.is_file() or not experiment_write.is_file():
            print(
                f"Skip stem {stem}: missing write CSVs "
                f"({baseline_write.name}, {experiment_write.name})",
                file=sys.stderr,
            )
            continue
        out = bundle_dir / f"e2e_baseline_vs_experiment_{stem}.csv"
        post_b = read_dir / f"{READ_CSV_PREFIX}{stem}_baseline_post_compact.csv"
        post_e = read_dir / f"{READ_CSV_PREFIX}{stem}_experiment_post_compact.csv"
        br_post = post_b if post_b.is_file() else None
        er_post = post_e if post_e.is_file() else None
        print(f"--- {bundle_dir.name}: {stem} ---")
        run_one_comparison(
            baseline_read,
            experiment_read,
            baseline_write,
            experiment_write,
            out,
            br_post,
            er_post,
            initial_batch_size,
            incremental_batch_size,
            None,
            None,
        )
        ok += 1
    if ok == 0:
        return 1
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Compare baseline vs experiment E2E read/write CSVs.")
    p.add_argument("--baseline-read", type=Path, default=None)
    p.add_argument("--experiment-read", type=Path, default=None)
    p.add_argument("--baseline-write", type=Path, default=None)
    p.add_argument("--experiment-write", type=Path, default=None)
    p.add_argument("--output", type=Path, default=None)
    p.add_argument(
        "--report-dir",
        type=Path,
        default=None,
        help="Directory with read/ and write/ subfolders; writes e2e_baseline_vs_experiment_<stem>.* per pair.",
    )
    p.add_argument(
        "--report-root",
        type=Path,
        default=None,
        help="Scan immediate subdirectories for read/ and write/ (e.g. hudi_015/, hudi_015_lts/).",
    )
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
    p.add_argument(
        "--initial-batch-size",
        type=int,
        default=env_int("NUM_OF_PARTITIONS", 2000),
        help="Batch size label for batch_id 0 (write table). Default: env NUM_OF_PARTITIONS or 2000.",
    )
    p.add_argument(
        "--incremental-batch-size",
        type=int,
        default=env_int("NUM_OF_RECORDS_TO_UPDATE", 100),
        help="Batch size label for batch_id 1–4 (write table). Default: env NUM_OF_RECORDS_TO_UPDATE or 100.",
    )
    p.add_argument(
        "--tables-md",
        type=Path,
        default=None,
        help="Markdown per-batch tables (default: next to --output with _per_batch_tables.md).",
    )
    p.add_argument(
        "--tables-csv",
        type=Path,
        default=None,
        help="CSV per-batch rows (default: next to --output with _per_batch_tables.csv).",
    )
    args = p.parse_args()

    explicit = (
        args.baseline_read is not None
        and args.experiment_read is not None
        and args.baseline_write is not None
        and args.experiment_write is not None
        and args.output is not None
    )
    report_dir = args.report_dir is not None
    report_root = args.report_root is not None
    modes = sum([explicit, report_dir, report_root])
    if modes != 1:
        p.error(
            "Specify exactly one mode: "
            "four CSV paths plus --output, or --report-dir, or --report-root."
        )

    if report_dir:
        d = args.report_dir.resolve()
        rd, wd = d / "read", d / "write"
        if not rd.is_dir() or not wd.is_dir():
            p.error(f"--report-dir must contain read/ and write/ subdirectories (got {d})")
        return process_report_bundle(d, rd, wd, args.initial_batch_size, args.incremental_batch_size)

    if report_root:
        root = args.report_root.resolve()
        bundles = iter_report_bundles(root)
        if not bundles:
            p.error(
                f"No subdirectories with both read/ and write/ under {root} "
                "(use --report-dir on a single bundle, e.g. .../hudi_015)."
            )
        rc = 0
        for bundle_dir, rd, wd in bundles:
            print(f"=== Bundle: {bundle_dir} ===")
            r = process_report_bundle(bundle_dir, rd, wd, args.initial_batch_size, args.incremental_batch_size)
            rc = max(rc, r)
        return rc

    run_one_comparison(
        args.baseline_read,
        args.experiment_read,
        args.baseline_write,
        args.experiment_write,
        args.output,
        args.baseline_read_post,
        args.experiment_read_post,
        args.initial_batch_size,
        args.incremental_batch_size,
        args.tables_md,
        args.tables_csv,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
