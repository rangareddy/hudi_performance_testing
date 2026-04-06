"""
Golden-file tests for scripts/compare_e2e_phases.py.

tests/tests_report/read and tests/tests_report/write hold captured benchmark CSVs.
Running process_report_bundle (same as --report-dir) must produce the two E2E summary
files next to the bundle root (not under read/ or write/), matching the golden CSVs
committed under tests/tests_report/.
"""

import shutil
import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import compare_e2e_phases as e2e  # noqa: E402

DATA_FIXTURES = Path(__file__).resolve().parent / "tests_report"
READ_FIXTURES = DATA_FIXTURES / "read"
WRITE_FIXTURES = DATA_FIXTURES / "write"
# Matches version_tag from fixture read/write CSVs (0.14.1 baseline / 0.14.2-SNAPSHOT experiment)
GOLDEN_SUMMARY = DATA_FIXTURES / "e2e_baseline_vs_experiment_0.14.1_vs_0.14.2-SNAPSHOT.csv"
GOLDEN_DETAIL = (
    DATA_FIXTURES / "e2e_baseline_vs_experiment_0.14.1_vs_0.14.2-SNAPSHOT_per_batch_tables.csv"
)
# Stem 500_2000_100 → initial rows = 2000 partitions × 100 records/partition
INITIAL_BATCH_SIZE = 200000
INCREMENTAL_BATCH_SIZE = 100


def _normalize_csv_text(s: str) -> str:
    return s.replace("\r\n", "\n").strip()


class TestCompareE2EFixtures(unittest.TestCase):
    def test_process_report_bundle_matches_golden_outputs(self):
        self.assertTrue(READ_FIXTURES.is_dir(), f"Missing {READ_FIXTURES}")
        self.assertTrue(WRITE_FIXTURES.is_dir(), f"Missing {WRITE_FIXTURES}")
        self.assertTrue(GOLDEN_SUMMARY.is_file(), f"Missing golden {GOLDEN_SUMMARY}")
        self.assertTrue(GOLDEN_DETAIL.is_file(), f"Missing golden {GOLDEN_DETAIL}")

        expected_summary = _normalize_csv_text(GOLDEN_SUMMARY.read_text())
        expected_detail = _normalize_csv_text(GOLDEN_DETAIL.read_text())

        with tempfile.TemporaryDirectory() as tmp:
            bundle = Path(tmp) / "bundle"
            shutil.copytree(READ_FIXTURES, bundle / "read")
            shutil.copytree(WRITE_FIXTURES, bundle / "write")
            rc = e2e.process_report_bundle(
                bundle,
                bundle / "read",
                bundle / "write",
                INITIAL_BATCH_SIZE,
                INCREMENTAL_BATCH_SIZE,
            )
            self.assertEqual(rc, 0)
            out_summary = bundle / GOLDEN_SUMMARY.name
            out_detail = bundle / GOLDEN_DETAIL.name
            self.assertTrue(out_summary.is_file())
            self.assertTrue(out_detail.is_file())
            self.assertEqual(
                _normalize_csv_text(out_summary.read_text()),
                expected_summary,
                msg="Summary CSV differs from tests/tests_report golden (regenerate: "
                "python3 scripts/compare_e2e_phases.py --report-dir tests/tests_report "
                f"--initial-batch-size {INITIAL_BATCH_SIZE} "
                f"--incremental-batch-size {INCREMENTAL_BATCH_SIZE})",
            )
            self.assertEqual(
                _normalize_csv_text(out_detail.read_text()),
                expected_detail,
                msg="Per-batch detail CSV differs from tests/tests_report golden",
            )


class TestLoadReadByBatchAvg(unittest.TestCase):
    """read_aggregate=avg rows (from READ_PERFORMANCE_ITERATIONS) win over max iteration."""

    def test_prefers_read_aggregate_avg_when_present(self):
        lines = [
            "run_sequence,table_type,hudi_version,batch_id,iteration,read_aggregate,"
            "execution_time_seconds,count,run_timestamp_utc,start_time,end_time,status",
            "1,COPY_ON_WRITE,0.14.1,0,1,,10,200000,,,,ok",
            "1,COPY_ON_WRITE,0.14.1,0,2,,30,200000,,,,ok",
            "1,COPY_ON_WRITE,0.14.1,0,,avg,20,200000,,,,ok",
        ]
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "read.csv"
            p.write_text("\n".join(lines) + "\n")
            by_batch = e2e.load_read_by_batch(p)
        self.assertIn(0, by_batch)
        sec, cnt = by_batch[0]
        self.assertAlmostEqual(sec, 20.0)
        self.assertEqual(cnt, 200000)

    def test_falls_back_to_max_iteration_without_avg(self):
        lines = [
            "run_sequence,table_type,hudi_version,batch_id,iteration,read_aggregate,"
            "execution_time_seconds,count,run_timestamp_utc,start_time,end_time,status",
            "7,COPY_ON_WRITE,0.14.1,0,1,,10,1,,,,ok",
            "7,COPY_ON_WRITE,0.14.1,0,2,,30,1,,,,ok",
        ]
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "read.csv"
            p.write_text("\n".join(lines) + "\n")
            by_batch = e2e.load_read_by_batch(p)
        self.assertIn(0, by_batch)
        self.assertAlmostEqual(by_batch[0][0], 30.0)


if __name__ == "__main__":
    unittest.main()
