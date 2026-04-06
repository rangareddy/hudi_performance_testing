"""
Unit tests for scripts/hudi_benchmark.py

Tests the benchmark entry-point logic without a live Spark cluster.
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))


class TestHudiBenchmarkMain(unittest.TestCase):
    """Test scripts/hudi_benchmark.py main() logic."""

    def _run_main(self, argv, mock_spark=None):
        """Import and run main() with patched argv and SparkSession."""
        import hudi_benchmark

        if mock_spark is None:
            mock_df = MagicMock()
            mock_df.distinct.return_value.count.return_value = 99999
            mock_spark = MagicMock()
            mock_spark.read.format.return_value.load.return_value = mock_df

        mock_builder = MagicMock()
        mock_builder.appName.return_value.getOrCreate.return_value = mock_spark

        mock_session_class = MagicMock()
        mock_session_class.builder = mock_builder

        with patch("sys.argv", argv):
            # hudi_benchmark imports SparkSession at load time; patch the module binding.
            with patch.object(hudi_benchmark, "SparkSession", mock_session_class):
                hudi_benchmark.main()
        return mock_spark

    def test_exits_without_path_argument(self):
        import hudi_benchmark
        with patch("sys.argv", ["hudi_benchmark.py"]):
            with self.assertRaises(SystemExit) as ctx:
                hudi_benchmark.main()
        self.assertEqual(ctx.exception.code, 1)

    def test_reads_hudi_format_from_given_path(self):
        mock_df = MagicMock()
        mock_df.distinct.return_value.count.return_value = 1234

        mock_spark = MagicMock()
        mock_spark.read.format.return_value.load.return_value = mock_df
        mock_spark.sql.return_value = None

        self._run_main(
            ["hudi_benchmark.py", "s3://bucket/my_table"],
            mock_spark=mock_spark,
        )
        mock_spark.read.format.assert_called_once_with("hudi")
        mock_spark.read.format.return_value.load.assert_called_once_with("s3://bucket/my_table")

    def test_calls_distinct_count(self):
        mock_df = MagicMock()
        mock_df.distinct.return_value.count.return_value = 42

        mock_spark = MagicMock()
        mock_spark.read.format.return_value.load.return_value = mock_df

        self._run_main(["hudi_benchmark.py", "s3://bucket/table"], mock_spark=mock_spark)
        mock_df.distinct.assert_called_once()
        mock_df.distinct.return_value.count.assert_called_once()

    def test_stops_spark_session(self):
        mock_df = MagicMock()
        mock_df.distinct.return_value.count.return_value = 1

        mock_spark = MagicMock()
        mock_spark.read.format.return_value.load.return_value = mock_df

        self._run_main(["hudi_benchmark.py", "s3://bucket/table"], mock_spark=mock_spark)
        mock_spark.stop.assert_called_once()

    def test_disables_metadata_and_skipping(self):
        mock_df = MagicMock()
        mock_df.distinct.return_value.count.return_value = 0

        mock_spark = MagicMock()
        mock_spark.read.format.return_value.load.return_value = mock_df

        self._run_main(["hudi_benchmark.py", "s3://bucket/table"], mock_spark=mock_spark)
        mock_spark.sql.assert_any_call("SET hoodie.metadata.enable=false")
        mock_spark.conf.set.assert_any_call("hoodie.enable.data.skipping", "false")


class TestOutputFormat(unittest.TestCase):
    """Verify the output lines that run_benchmark_suite.py parses."""

    def test_count_output_matches_parse_pattern(self):
        import re
        count = 123456
        line = f"Execution Complete. Count: {count}"
        m = re.search(r"Execution Complete\.\s*Count:\s*(\d+)", line)
        self.assertIsNotNone(m)
        self.assertEqual(int(m.group(1)), count)

    def test_time_output_matches_parse_pattern(self):
        import re
        elapsed = 42.57
        line = f"Total execution time: {elapsed:.2f} seconds"
        m = re.search(r"Total execution time:\s*([\d.]+)\s*seconds", line)
        self.assertIsNotNone(m)
        self.assertAlmostEqual(float(m.group(1)), elapsed, places=2)


if __name__ == "__main__":
    unittest.main()
