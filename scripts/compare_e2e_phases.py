#!/usr/bin/env python3
"""
Aggregate read/write CSVs from baseline vs experiment E2E phases.

Writes:
  1) Summary CSV: rolled-up metrics; first columns table_type, is_logical_timestamp (true/false
     from filename stem), then metric columns. Bundle mode merges all COW/MOR stems.
  2) Per-batch detail CSV: first columns table_type, is_logical_timestamp; batch_size/count holds
     batch size (write) or count (read) with no slash in the cell. Hudi versions are in filenames.
     Lower time is better.

Report layout:
  --report-dir BUNDLE   expects BUNDLE/read/ and BUNDLE/write/.
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
# run_hudi_ingestion.sh logs operation "hudi_streamer"; older runs used "hudi_delta_streamer"
STREAMER_WRITE_OPERATIONS: Tuple[str, ...] = ("hudi_delta_streamer", "hudi_streamer")
# CSV header uses slash; DictWriter accepts this as the field name.
COL_BATCH_SIZE_SLASH_COUNT = "batch_size/count"


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


def hudi_version_from_csv(path: Path) -> str:
    """First hudi_version from an ok row in a benchmark or write CSV."""
    if not path.is_file():
        return ""
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            if (row.get("status") or "").strip() != "ok":
                continue
            v = (row.get("hudi_version") or "").strip()
            if v:
                return v
    return ""


def hudi_version_for_filename_from_read(path: Path) -> str:
    """Prefer ok benchmark rows; fall back to any row with hudi_version (avoids unknown_vs_unknown when status is missing)."""
    v = hudi_version_from_csv(path)
    if v:
        return v
    if not path.is_file():
        return ""
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            v = (row.get("hudi_version") or "").strip()
            if v:
                return v
    return ""


def hudi_version_for_filename_from_write_phase(path: Path, phase: str) -> str:
    """Infer tag version from write perf: baseline uses streamer row at min batch_id; experiment at max batch_id."""
    if not path.is_file():
        return ""
    candidates: List[Tuple[int, str]] = []
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            if (row.get("status") or "").strip() != "ok":
                continue
            op = (row.get("operation") or "").strip()
            if op not in STREAMER_WRITE_OPERATIONS:
                continue
            hv = (row.get("hudi_version") or "").strip()
            if not hv:
                continue
            bid = _int_or_zero(row.get("batch_id", ""))
            candidates.append((bid, hv))
    if not candidates:
        return ""
    if phase == "experiment":
        return max(candidates, key=lambda x: x[0])[1]
    return min(candidates, key=lambda x: x[0])[1]


def version_tag_for_filenames(
    baseline_read: Path,
    experiment_read: Path,
    *,
    baseline_write: Optional[Path] = None,
    experiment_write: Optional[Path] = None,
    baseline_hudi_version: str = "",
    experiment_hudi_version: str = "",
) -> str:
    """Safe token for filenames, e.g. 0.15.0_vs_0.15.1."""

    def tok(s: str) -> str:
        t = (s or "").strip() or "unknown"
        for c in '\\/:*?"<>|':
            t = t.replace(c, "_")
        return t.replace(" ", "_")

    bv = (baseline_hudi_version or "").strip()
    if not bv:
        bv = hudi_version_for_filename_from_read(baseline_read)
    if not bv and baseline_write is not None:
        bv = hudi_version_for_filename_from_write_phase(baseline_write, "baseline")
    if not bv:
        bv = (os.environ.get("SOURCE_HUDI_VERSION") or "").strip()

    ev = (experiment_hudi_version or "").strip()
    if not ev:
        ev = hudi_version_for_filename_from_read(experiment_read)
    if not ev and experiment_write is not None:
        ev = hudi_version_for_filename_from_write_phase(experiment_write, "experiment")
    if not ev:
        ev = (os.environ.get("TARGET_HUDI_VERSION") or "").strip()

    return f"{tok(bv)}_vs_{tok(ev)}"


def config_stem_from_read_baseline(path: Path) -> str:
    """Parse stem from hudi_benchmark_results_<stem>_baseline.csv."""
    name = path.name
    if name.startswith(READ_CSV_PREFIX) and name.endswith(READ_CSV_BASELINE_SUFFIX):
        return name[len(READ_CSV_PREFIX) : -len(READ_CSV_BASELINE_SUFFIX)]
    return ""


def effective_config_stem(explicit_stem: str, baseline_read: Path) -> str:
    return explicit_stem if explicit_stem.strip() else config_stem_from_read_baseline(baseline_read)


def is_logical_timestamp_from_stem(config_stem: str) -> str:
    """Stem token after table kind: false → false, true → true (matches IS_LOGICAL_TIMESTAMP_ENABLED)."""
    if not config_stem:
        return ""
    parts = config_stem.split("_")
    if len(parts) >= 2:
        if parts[1] == "false":
            return "false"
        if parts[1] == "true":
            return "true"
    return ""


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


def sum_write_seconds(
    path: Path,
    operation_filter: Optional[str] = None,
    *,
    matching_operations: Optional[Tuple[str, ...]] = None,
) -> Tuple[float, int]:
    """Sum execution_time_seconds for rows with status ok.

    If matching_operations is set, operation must equal one of those strings (exact).
    Else if operation_filter is set, operation must contain that substring.
    """
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
            if matching_operations is not None:
                if op not in matching_operations:
                    continue
            elif operation_filter and operation_filter not in op:
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


def default_initial_batch_size() -> int:
    """Row count for batch 0: partitions × records/partition (matches scripts/initial_batch.scala)."""
    return env_int("NUM_OF_PARTITIONS", 2000) * env_int("NUM_OF_RECORDS_PER_PARTITION", 1)


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
    """Write perf: HoodieStreamer rows (hudi_streamer or hudi_delta_streamer), status ok; batch_id -> seconds."""
    out: Dict[int, float] = {}
    if not path.is_file():
        return out
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            if (row.get("status") or "").strip() != "ok":
                continue
            op = (row.get("operation") or "").strip()
            if op not in STREAMER_WRITE_OPERATIONS:
                continue
            bid = _int_or_zero(row.get("batch_id", ""))
            out[bid] = _float_or_zero(row.get("execution_time_seconds", ""))
    return out


def load_read_by_batch(path: Path) -> Dict[int, Tuple[float, int]]:
    """Read benchmark: for each batch_id, pick one ok row with max (run_sequence, iteration)."""
    best: Dict[int, Tuple[int, int, float, int]] = {}  # batch_id -> (run_sequence, iteration, sec, count)
    if not path.is_file():
        return {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            if (row.get("status") or "").strip() != "ok":
                continue
            bid = _int_or_zero(row.get("batch_id", ""))
            seq = _int_or_zero(row.get("run_sequence", "0"))
            itn = _int_or_zero((row.get("iteration") or "1").strip())
            sec = _float_or_zero(row.get("execution_time_seconds", ""))
            cnt = _int_or_zero(row.get("count", "0"))
            prev = best.get(bid)
            if prev is None or (seq, itn) > (prev[0], prev[1]):
                best[bid] = (seq, itn, sec, cnt)
    return {k: (v[2], v[3]) for k, v in best.items()}


def sum_read_seconds(
    path: Path,
    *,
    only_batch_ids: Optional[Tuple[int, ...]] = None,
    exclude_batch_ids: Optional[Tuple[int, ...]] = None,
) -> Tuple[float, int]:
    """Sum execution_time_seconds over batch_ids using the same best-row rule as load_read_by_batch."""
    by_batch = load_read_by_batch(path)
    if only_batch_ids is not None:
        allow = set(only_batch_ids)
        by_batch = {k: v for k, v in by_batch.items() if k in allow}
    if exclude_batch_ids is not None:
        ex = set(exclude_batch_ids)
        by_batch = {k: v for k, v in by_batch.items() if k not in ex}
    if not by_batch:
        return 0.0, 0
    total = sum(sec for sec, _ in by_batch.values())
    return total, len(by_batch)


def _uses_legacy_split_post_compact_csvs(
    baseline_read: Path,
    experiment_read: Path,
    baseline_read_post: Optional[Path],
    experiment_read_post: Optional[Path],
) -> bool:
    if baseline_read_post is None or experiment_read_post is None:
        return False
    if not baseline_read_post.is_file() or not experiment_read_post.is_file():
        return False
    try:
        if baseline_read_post.resolve() == baseline_read.resolve():
            return False
        if experiment_read_post.resolve() == experiment_read.resolve():
            return False
    except OSError:
        return False
    return True


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


SECTION_WRITE_BENCHMARK = "write benchmark"
SECTION_READ_BENCHMARK = "read_benchmark"
SECTION_READ_POST_COMPACT = "read_benchmark_post_compaction"


def build_per_batch_detail(
    baseline_write: Path,
    experiment_write: Path,
    baseline_read: Path,
    experiment_read: Path,
    initial_batch_size: int,
    incremental_batch_size: int,
    baseline_read_post: Optional[Path],
    experiment_read_post: Optional[Path],
) -> List[Dict[str, str]]:
    tt = table_type_from_read_csv(baseline_read)
    tt_short = short_table_type(tt)

    w_base = load_hudi_delta_streamer_by_batch(baseline_write)
    w_exp = load_hudi_delta_streamer_by_batch(experiment_write)
    r_base = load_read_by_batch(baseline_read)
    r_exp = load_read_by_batch(experiment_read)

    batch_ids = [0, 1, 2, 3, 4]
    detail_rows: List[Dict[str, str]] = []

    for bid in batch_ids:
        bsz = batch_size_for(bid, initial_batch_size, incremental_batch_size)
        bl = batch_label(bid)
        # batch_size/count: write rows show batch_size only (no slash in value).
        size_count_write = str(bsz)
        hb = bid in w_base
        he = bid in w_exp
        wb = w_base[bid] if hb else float("nan")
        we = w_exp[bid] if he else float("nan")
        if not hb or not he:
            diff_s = ""
            diff_p = ""
            bdisp = "-" if not hb else fmt_time(wb, as_int_if_whole=True)
            edisp = "-" if not he else fmt_time(we, as_int_if_whole=True)
        else:
            diff_s = f"{we - wb:.2f}"
            diff_p = pct_delta(wb, we)
            bdisp = fmt_time(wb, as_int_if_whole=True)
            edisp = fmt_time(we, as_int_if_whole=True)
        detail_rows.append(
            {
                "section": SECTION_WRITE_BENCHMARK,
                "table_type": tt_short,
                "batch_info": bl,
                COL_BATCH_SIZE_SLASH_COUNT: size_count_write,
                "baseline_seconds": bdisp,
                "experiment_seconds": edisp,
                "diff_seconds": diff_s,
                "diff_percent": diff_p,
            }
        )

    # COW / MOR: one pre-compaction (or sole) read in benchmark CSV at batch_id 0.
    if tt_short == "COW":
        read_specs: List[Tuple[int, str]] = [(0, "Full table read")]
    elif tt_short == "MOR":
        read_specs = [(0, "Read before compaction")]
    else:
        read_specs = [(bid, batch_label(bid)) for bid in batch_ids]

    for bid, bl in read_specs:
        hb = bid in r_base
        he = bid in r_exp
        wb, cb = r_base[bid] if hb else (float("nan"), 0)
        we, ce = r_exp[bid] if he else (float("nan"), 0)
        cnt = (ce or cb) if (hb or he) else 0
        size_count_read = str(cnt)
        if not hb or not he:
            diff_s = ""
            diff_p = ""
            bdisp = "-" if not hb else fmt_time(wb)
            edisp = "-" if not he else fmt_time(we)
        else:
            diff_s = f"{we - wb:.2f}"
            diff_p = pct_delta(wb, we)
            bdisp = fmt_time(wb)
            edisp = fmt_time(we)
        detail_rows.append(
            {
                "section": SECTION_READ_BENCHMARK,
                "table_type": tt_short,
                "batch_info": bl,
                COL_BATCH_SIZE_SLASH_COUNT: size_count_read,
                "baseline_seconds": bdisp,
                "experiment_seconds": edisp,
                "diff_seconds": diff_s,
                "diff_percent": diff_p,
            }
        )

    # Post-compaction read (batch_id 5): legacy *_post_compact.csv pair, or same rows in main read CSV.
    if _uses_legacy_split_post_compact_csvs(
        baseline_read,
        experiment_read,
        baseline_read_post,
        experiment_read_post,
    ):
        rpb = load_read_by_batch(baseline_read_post)  # type: ignore[arg-type]
        rpe = load_read_by_batch(experiment_read_post)  # type: ignore[arg-type]
    else:
        rpb = {5: r_base[5]} if 5 in r_base else {}
        rpe = {5: r_exp[5]} if 5 in r_exp else {}

    if rpb or rpe:
        post_ids = sorted(set(rpb.keys()) | set(rpe.keys()))
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
                bdisp = "-" if not hb else fmt_time(wb)
                edisp = "-" if not he else fmt_time(we)
            else:
                diff_s = f"{we - wb:.2f}"
                diff_p = pct_delta(wb, we)
                bdisp = fmt_time(wb)
                edisp = fmt_time(we)
            detail_rows.append(
                {
                    "section": SECTION_READ_POST_COMPACT,
                    "table_type": tt_short,
                    "batch_info": label,
                    COL_BATCH_SIZE_SLASH_COUNT: str(cnt),
                    "baseline_seconds": bdisp,
                    "experiment_seconds": edisp,
                    "diff_seconds": diff_s,
                    "diff_percent": diff_p,
                }
            )

    return detail_rows


DETAIL_CSV_FIELDNAMES = [
    "table_type",
    "is_logical_timestamp",
    "section",
    "batch_info",
    COL_BATCH_SIZE_SLASH_COUNT,
    "baseline_seconds",
    "experiment_seconds",
    "diff_seconds",
    "diff_percent",
]

SUMMARY_CSV_FIELDNAMES = [
    "table_type",
    "is_logical_timestamp",
    "metric",
    "baseline_value",
    "experiment_value",
    "delta_seconds",
    "delta_percent",
    "baseline_row_count_ok",
    "experiment_row_count_ok",
]


def detail_csv_rows_with_logical_ts_flag(
    config_stem: str, detail_rows: List[Dict[str, str]]
) -> List[Dict[str, str]]:
    lts = is_logical_timestamp_from_stem(config_stem)
    out: List[Dict[str, str]] = []
    for r in detail_rows:
        out.append(
            {
                "table_type": r["table_type"],
                "is_logical_timestamp": lts,
                "section": r["section"],
                "batch_info": r["batch_info"],
                COL_BATCH_SIZE_SLASH_COUNT: r[COL_BATCH_SIZE_SLASH_COUNT],
                "baseline_seconds": r["baseline_seconds"],
                "experiment_seconds": r["experiment_seconds"],
                "diff_seconds": r["diff_seconds"],
                "diff_percent": r["diff_percent"],
            }
        )
    return out


def write_detail_csv(path: Path, rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=DETAIL_CSV_FIELDNAMES)
        w.writeheader()
        w.writerows(rows)


def build_summary_rows(
    baseline_read: Path,
    experiment_read: Path,
    baseline_write: Path,
    experiment_write: Path,
    baseline_read_post: Optional[Path],
    experiment_read_post: Optional[Path],
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []

    # batch_id 5 = MOR post-compaction read when merged into the same CSV as pre-compact (batch 0).
    br, br_n = sum_read_seconds(baseline_read, exclude_batch_ids=(5,))
    er, er_n = sum_read_seconds(experiment_read, exclude_batch_ids=(5,))
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

    bh, _ = sum_write_seconds(
        baseline_write, matching_operations=STREAMER_WRITE_OPERATIONS
    )
    eh, _ = sum_write_seconds(
        experiment_write, matching_operations=STREAMER_WRITE_OPERATIONS
    )
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

    if _uses_legacy_split_post_compact_csvs(
        baseline_read,
        experiment_read,
        baseline_read_post,
        experiment_read_post,
    ):
        bpr, bpr_n = sum_read_seconds(baseline_read_post)  # type: ignore[arg-type]
        epr, epr_n = sum_read_seconds(experiment_read_post)  # type: ignore[arg-type]
        post_ok = bpr_n > 0 and epr_n > 0
    else:
        bpr, bpr_n = sum_read_seconds(baseline_read, only_batch_ids=(5,))
        epr, epr_n = sum_read_seconds(experiment_read, only_batch_ids=(5,))
        post_ok = bpr_n > 0 and epr_n > 0

    if post_ok:
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
    elif table_type_from_read_csv(baseline_read) == "MERGE_ON_READ":
        print(
            "Note: no post-compaction read (batch_id 5) in read CSVs; skipping read_benchmark_post_compaction_total_seconds",
            file=sys.stderr,
        )

    return rows


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
    tables_csv: Optional[Path],
    config_stem: str = "",
    baseline_hudi_version: str = "",
    experiment_hudi_version: str = "",
) -> None:
    stem_eff = effective_config_stem(config_stem, baseline_read)
    ver_tag = version_tag_for_filenames(
        baseline_read,
        experiment_read,
        baseline_write=baseline_write,
        experiment_write=experiment_write,
        baseline_hudi_version=baseline_hudi_version,
        experiment_hudi_version=experiment_hudi_version,
    )
    output = output.parent / f"{output.stem}_{ver_tag}{output.suffix}"

    detail_core = build_per_batch_detail(
        baseline_write,
        experiment_write,
        baseline_read,
        experiment_read,
        initial_batch_size,
        incremental_batch_size,
        baseline_read_post,
        experiment_read_post,
    )
    csv_detail = tables_csv
    if csv_detail is None:
        csv_detail = output.parent / f"{output.stem}_per_batch_tables.csv"
    write_detail_csv(csv_detail, detail_csv_rows_with_logical_ts_flag(stem_eff, detail_core))

    rows = build_summary_rows(
        baseline_read,
        experiment_read,
        baseline_write,
        experiment_write,
        baseline_read_post,
        experiment_read_post,
    )
    tt_short = short_table_type(table_type_from_read_csv(baseline_read))
    lts = is_logical_timestamp_from_stem(stem_eff)
    for r in rows:
        r["table_type"] = tt_short
        r["is_logical_timestamp"] = lts

    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_CSV_FIELDNAMES)
        w.writeheader()
        w.writerows(rows)

    print(f"Per-batch detail CSV:      {csv_detail}")
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
    combined_summary: List[Dict[str, str]] = []
    combined_detail: List[Dict[str, str]] = []
    ok = 0
    version_tag = ""
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
        post_b = read_dir / f"{READ_CSV_PREFIX}{stem}_baseline_post_compact.csv"
        post_e = read_dir / f"{READ_CSV_PREFIX}{stem}_experiment_post_compact.csv"
        br_post = post_b if post_b.is_file() else None
        er_post = post_e if post_e.is_file() else None
        print(f"--- {bundle_dir.name}: {stem} ---")
        if not version_tag:
            version_tag = version_tag_for_filenames(
                baseline_read,
                experiment_read,
                baseline_write=baseline_write,
                experiment_write=experiment_write,
            )
        detail_core = build_per_batch_detail(
            baseline_write,
            experiment_write,
            baseline_read,
            experiment_read,
            initial_batch_size,
            incremental_batch_size,
            br_post,
            er_post,
        )
        combined_detail.extend(detail_csv_rows_with_logical_ts_flag(stem, detail_core))
        summ = build_summary_rows(
            baseline_read,
            experiment_read,
            baseline_write,
            experiment_write,
            br_post,
            er_post,
        )
        tt_short = short_table_type(table_type_from_read_csv(baseline_read))
        lts = is_logical_timestamp_from_stem(stem)
        for r in summ:
            combined_summary.append(
                {**r, "table_type": tt_short, "is_logical_timestamp": lts}
            )
        ok += 1
    if ok == 0:
        return 1
    summary_out = bundle_dir / f"e2e_baseline_vs_experiment_{version_tag}.csv"
    detail_out = bundle_dir / f"e2e_baseline_vs_experiment_{version_tag}_per_batch_tables.csv"
    summary_out.parent.mkdir(parents=True, exist_ok=True)
    with open(summary_out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_CSV_FIELDNAMES)
        w.writeheader()
        w.writerows(combined_summary)
    write_detail_csv(detail_out, combined_detail)
    print(f"Comparison summary CSV (all stems): {summary_out}")
    print(f"Per-batch detail CSV (all stems):   {detail_out}")
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
        help="Directory with read/ and write/; writes e2e_baseline_vs_experiment_<hudi_ver>_vs_<hudi_ver>.csv (all stems) plus matching per_batch_tables CSV.",
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
        help="Legacy only: separate baseline post-compaction read CSV. New E2E appends batch_id=5 rows to the main read CSV.",
    )
    p.add_argument(
        "--experiment-read-post",
        type=Path,
        default=None,
        help="Legacy only: separate experiment post-compaction read CSV.",
    )
    p.add_argument(
        "--initial-batch-size",
        type=int,
        default=default_initial_batch_size(),
        help=(
            "Batch size label for batch_id 0 (write table). "
            "Default: NUM_OF_PARTITIONS × NUM_OF_RECORDS_PER_PARTITION from env (initial load size)."
        ),
    )
    p.add_argument(
        "--incremental-batch-size",
        type=int,
        default=env_int("NUM_OF_RECORDS_TO_UPDATE", 100),
        help="Batch size label for batch_id 1–4 (write table). Default: env NUM_OF_RECORDS_TO_UPDATE or 100.",
    )
    p.add_argument(
        "--tables-csv",
        type=Path,
        default=None,
        help="Per-batch detail CSV (default: next to --output with _per_batch_tables.csv).",
    )
    p.add_argument(
        "--baseline-hudi-version",
        default="",
        help="Override baseline version in output filename (default: read CSV, write CSV, or SOURCE_HUDI_VERSION).",
    )
    p.add_argument(
        "--experiment-hudi-version",
        default="",
        help="Override experiment version in output filename (default: read CSV, write CSV, or TARGET_HUDI_VERSION).",
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
        args.tables_csv,
        "",
        baseline_hudi_version=(args.baseline_hudi_version or ""),
        experiment_hudi_version=(args.experiment_hudi_version or ""),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
