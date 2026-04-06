"""
Unit tests for run_benchmark_suite.py

Tests the parsing logic, sequence tracking, and CSV output
without invoking Spark or AWS.
"""

import csv
import os
import re
import statistics
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Allow importing from the project root
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import run_benchmark_suite as suite


class TestSequenceTracking(unittest.TestCase):
    def test_first_run_starts_at_one(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            seq_file = Path(tmpdir) / suite.SEQUENCE_FILENAME
            with patch.object(suite, "SCRIPT_DIR", Path(tmpdir)):
                n = suite.read_and_increment_sequence()
            self.assertEqual(n, 1)
            self.assertEqual(seq_file.read_text().strip(), "1")

    def test_increments_on_each_call(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(suite, "SCRIPT_DIR", Path(tmpdir)):
                n1 = suite.read_and_increment_sequence()
                n2 = suite.read_and_increment_sequence()
                n3 = suite.read_and_increment_sequence()
            self.assertEqual([n1, n2, n3], [1, 2, 3])

    def test_resumes_from_existing_value(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            seq_file = Path(tmpdir) / suite.SEQUENCE_FILENAME
            seq_file.write_text("42")
            with patch.object(suite, "SCRIPT_DIR", Path(tmpdir)):
                n = suite.read_and_increment_sequence()
            self.assertEqual(n, 43)

    def test_recovers_from_corrupt_sequence_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            seq_file = Path(tmpdir) / suite.SEQUENCE_FILENAME
            seq_file.write_text("not_a_number")
            with patch.object(suite, "SCRIPT_DIR", Path(tmpdir)):
                n = suite.read_and_increment_sequence()
            self.assertEqual(n, 1)


class TestOutputParsing(unittest.TestCase):
    """Tests the regex patterns used to parse spark-submit output."""

    TIME_PATTERN = re.compile(r"Total execution time:\s*([\d.]+)\s*seconds")
    COUNT_PATTERN = re.compile(r"Execution Complete\.\s*Count:\s*(\d+)")

    def _parse(self, output):
        time_match = self.TIME_PATTERN.search(output)
        count_match = self.COUNT_PATTERN.search(output)
        return (
            float(time_match.group(1)) if time_match else None,
            int(count_match.group(1)) if count_match else None,
        )

    def test_parses_well_formed_output(self):
        output = (
            "Some Spark output...\n"
            "Execution Complete. Count: 100000\n"
            "Total execution time: 12.34 seconds\n"
        )
        exec_time, count = self._parse(output)
        self.assertAlmostEqual(exec_time, 12.34)
        self.assertEqual(count, 100000)

    def test_returns_none_for_missing_time(self):
        output = "Execution Complete. Count: 50000\n"
        exec_time, count = self._parse(output)
        self.assertIsNone(exec_time)
        self.assertEqual(count, 50000)

    def test_returns_none_for_missing_count(self):
        output = "Total execution time: 5.0 seconds\n"
        exec_time, count = self._parse(output)
        self.assertAlmostEqual(exec_time, 5.0)
        self.assertIsNone(count)

    def test_returns_none_for_empty_output(self):
        exec_time, count = self._parse("")
        self.assertIsNone(exec_time)
        self.assertIsNone(count)

    def test_handles_integer_time(self):
        output = "Total execution time: 30 seconds\nExecution Complete. Count: 1\n"
        exec_time, count = self._parse(output)
        self.assertAlmostEqual(exec_time, 30.0)
        self.assertEqual(count, 1)

    def test_handles_large_count(self):
        output = "Total execution time: 1.0 seconds\nExecution Complete. Count: 9999999999\n"
        exec_time, count = self._parse(output)
        self.assertEqual(count, 9999999999)


class TestRunBenchmark(unittest.TestCase):
    def test_returns_error_when_script_missing(self):
        with patch.object(suite, "SCRIPT_DIR", Path("/nonexistent")):
            exec_time, count, status = suite.run_benchmark("COPY_ON_WRITE", "0.14.1", 0)
        self.assertIsNone(exec_time)
        self.assertIsNone(count)
        self.assertIn("script not found", status)

    def test_returns_timeout_status(self):
        import subprocess
        mock_result = MagicMock()
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_script = Path(tmpdir) / "run_hudi_benchmark.sh"
            fake_script.touch()
            with patch.object(suite, "SCRIPT_DIR", Path(tmpdir)):
                with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("cmd", 3600)):
                    exec_time, count, status = suite.run_benchmark("COPY_ON_WRITE", "0.14.1", 0)
        self.assertIsNone(exec_time)
        self.assertIsNone(count)
        self.assertEqual(status, "timeout")

    def test_returns_parse_failed_when_output_unmatched(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "No recognizable output here."
        mock_result.stderr = ""
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_script = Path(tmpdir) / "run_hudi_benchmark.sh"
            fake_script.touch()
            with patch.object(suite, "SCRIPT_DIR", Path(tmpdir)):
                with patch("subprocess.run", return_value=mock_result):
                    exec_time, count, status = suite.run_benchmark("COPY_ON_WRITE", "0.14.1", 0)
        self.assertEqual(status, "parse_failed")

    def test_returns_ok_status_on_valid_output(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = (
            "Execution Complete. Count: 12345\n"
            "Total execution time: 9.87 seconds\n"
        )
        mock_result.stderr = ""
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_script = Path(tmpdir) / "run_hudi_benchmark.sh"
            fake_script.touch()
            with patch.object(suite, "SCRIPT_DIR", Path(tmpdir)):
                with patch("subprocess.run", return_value=mock_result):
                    exec_time, count, status = suite.run_benchmark("COPY_ON_WRITE", "0.14.1", 0)
        self.assertAlmostEqual(exec_time, 9.87)
        self.assertEqual(count, 12345)
        self.assertEqual(status, "ok")

    def test_returns_exit_code_status_on_nonzero(self):
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "Spark job failed"
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_script = Path(tmpdir) / "run_hudi_benchmark.sh"
            fake_script.touch()
            with patch.object(suite, "SCRIPT_DIR", Path(tmpdir)):
                with patch("subprocess.run", return_value=mock_result):
                    exec_time, count, status = suite.run_benchmark("COPY_ON_WRITE", "0.14.1", 0)
        self.assertEqual(status, "exit_code_1")


class TestCsvOutput(unittest.TestCase):
    def _run_main_with_mock(self, tmpdir, mock_output, returncode=0):
        mock_result = MagicMock()
        mock_result.returncode = returncode
        mock_result.stdout = mock_output
        mock_result.stderr = ""
        output_csv = Path(tmpdir) / "results.csv"
        fake_script = Path(tmpdir) / "run_hudi_benchmark.sh"
        fake_script.touch()
        args = [
            "run_benchmark_suite.py",
            "--table-type", "COPY_ON_WRITE",
            "--hudi-versions", "0.14.1",
            "--batch-id", "2",
            "--output", str(output_csv),
        ]
        with patch.object(suite, "SCRIPT_DIR", Path(tmpdir)):
            with patch("subprocess.run", return_value=mock_result):
                with patch("sys.argv", args):
                    exit_code = suite.main()
        return exit_code, output_csv

    def test_csv_written_with_correct_header(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            mock_output = (
                "Execution Complete. Count: 1000\n"
                "Total execution time: 5.0 seconds\n"
            )
            _, output_csv = self._run_main_with_mock(tmpdir, mock_output)
            self.assertTrue(output_csv.exists())
            with open(output_csv) as f:
                reader = csv.DictReader(f)
                self.assertEqual(reader.fieldnames, suite.CSV_HEADER)

    def test_csv_row_contains_expected_fields(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            mock_output = (
                "Execution Complete. Count: 500\n"
                "Total execution time: 3.14 seconds\n"
            )
            _, output_csv = self._run_main_with_mock(tmpdir, mock_output)
            with open(output_csv) as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["table_type"], "COPY_ON_WRITE")
            self.assertEqual(row["hudi_version"], "0.14.1")
            self.assertEqual(row["batch_id"], "2")
            self.assertEqual(row.get("iteration"), "1")
            self.assertEqual(row.get("read_aggregate"), "")
            self.assertEqual(row["count"], "500")
            self.assertAlmostEqual(float(row["execution_time_seconds"]), 3.14, places=1)
            self.assertEqual(row["status"], "ok")
            self.assertNotEqual(row["run_timestamp_utc"], "")

    def test_csv_appends_on_second_run(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            mock_output = (
                "Execution Complete. Count: 100\n"
                "Total execution time: 1.0 seconds\n"
            )
            _, output_csv = self._run_main_with_mock(tmpdir, mock_output)
            _, _ = self._run_main_with_mock(tmpdir, mock_output)
            with open(output_csv) as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["run_sequence"], "1")
            self.assertEqual(rows[1]["run_sequence"], "2")

    def test_multi_iteration_writes_avg_row_on_last(self):
        mock_output = (
            "Execution Complete. Count: 100\n"
            "Total execution time: 4.0 seconds\n"
        )

        def _run_iter(tmpdir, it, seq=7):
            fake_script = Path(tmpdir) / "run_hudi_benchmark.sh"
            fake_script.touch()
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stdout = mock_output.replace("4.0", str(10.0 + it))
            mock_result.stderr = ""
            args = [
                "run_benchmark_suite.py",
                "--table-type", "COPY_ON_WRITE",
                "--hudi-versions", "0.14.1",
                "--batch-id", "0",
                "--iteration", str(it),
                "--run-sequence", str(seq),
                "--read-performance-iterations", "3",
                "--output", str(Path(tmpdir) / "results.csv"),
            ]
            with patch.object(suite, "SCRIPT_DIR", Path(tmpdir)):
                with patch("subprocess.run", return_value=mock_result):
                    with patch("sys.argv", args):
                        return suite.main()

        with tempfile.TemporaryDirectory() as tmpdir:
            self.assertEqual(_run_iter(tmpdir, 1), 0)
            self.assertEqual(_run_iter(tmpdir, 2), 0)
            self.assertEqual(_run_iter(tmpdir, 3), 0)
            out = Path(tmpdir) / "results.csv"
            with open(out) as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(len(rows), 4)  # 3 iterations + 1 avg
            self.assertEqual(rows[0]["iteration"], "1")
            self.assertEqual(rows[2]["iteration"], "3")
            avg = rows[3]
            self.assertEqual(avg["read_aggregate"], "avg")
            self.assertEqual(avg.get("iteration"), "")
            mean = float(avg["execution_time_seconds"])
            self.assertAlmostEqual(mean, statistics.mean([11.0, 12.0, 13.0]), places=3)

    def test_allocate_run_sequence_only_prints_number(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from io import StringIO

            with patch.object(suite, "SCRIPT_DIR", Path(tmpdir)):
                with patch("sys.argv", ["run_benchmark_suite.py", "--allocate-run-sequence-only"]):
                    buf = StringIO()
                    with patch("sys.stdout", buf):
                        code = suite.main()
            self.assertEqual(code, 0)
            self.assertEqual(buf.getvalue().strip(), "1")


if __name__ == "__main__":
    unittest.main()
