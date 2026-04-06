"""
Unit tests for scripts/incremental_batch.py

Tests range calculation and value generation without Spark.
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

import incremental_batch as ib


class TestCalculateRange(unittest.TestCase):
    def test_batch_0_maps_to_first_slice(self):
        start, end = ib.calculate_range(batch_id=0, num_records=100)
        self.assertEqual(start, 1)
        self.assertEqual(end, 100)

    def test_batch_1_starts_at_1(self):
        start, end = ib.calculate_range(batch_id=1, num_records=100)
        self.assertEqual(start, 1)
        self.assertEqual(end, 100)

    def test_batch_2_follows_batch_1(self):
        start, end = ib.calculate_range(batch_id=2, num_records=100)
        self.assertEqual(start, 101)
        self.assertEqual(end, 200)

    def test_batch_3_follows_batch_2(self):
        start, end = ib.calculate_range(batch_id=3, num_records=100)
        self.assertEqual(start, 201)
        self.assertEqual(end, 300)

    def test_small_num_records(self):
        start, end = ib.calculate_range(batch_id=1, num_records=10)
        self.assertEqual(start, 1)
        self.assertEqual(end, 10)

    def test_ranges_do_not_overlap(self):
        """Adjacent batches must produce non-overlapping ranges (inclusive end, next starts at end+1)."""
        _, end1 = ib.calculate_range(batch_id=1, num_records=50)
        start2, _ = ib.calculate_range(batch_id=2, num_records=50)
        self.assertEqual(end1 + 1, start2)

    def test_range_length_equals_num_records(self):
        for batch_id in range(1, 5):
            start, end = ib.calculate_range(batch_id=batch_id, num_records=100)
            self.assertEqual(end - start + 1, 100)


class TestGenerateValues(unittest.TestCase):
    def test_correct_count(self):
        values = ib.generate_values(start=1, end=100, batch_id=1)
        self.assertEqual(len(values), 100)

    def test_value_format(self):
        values = ib.generate_values(start=1, end=3, batch_id=2)
        self.assertEqual(values, ["value_1_2", "value_2_2", "value_3_2"])

    def test_batch_id_embedded_in_values(self):
        for batch_id in [1, 2, 3]:
            values = ib.generate_values(start=1, end=3, batch_id=batch_id)
            for v in values:
                self.assertTrue(v.endswith(f"_{batch_id}"), f"{v} doesn't end with _{batch_id}")

    def test_values_are_unique(self):
        values = ib.generate_values(start=1, end=101, batch_id=1)
        self.assertEqual(len(values), len(set(values)))

    def test_empty_range(self):
        # Inclusive end: start > end yields no indices.
        values = ib.generate_values(start=5, end=4, batch_id=1)
        self.assertEqual(values, [])

    def test_no_overlap_between_batches(self):
        """Values from different batches must be disjoint."""
        v1 = set(ib.generate_values(start=1, end=101, batch_id=1))
        v2 = set(ib.generate_values(start=101, end=201, batch_id=2))
        self.assertEqual(v1 & v2, set())


class TestGetEnvInt(unittest.TestCase):
    def test_reads_env_variable(self):
        with patch.dict("os.environ", {"MY_VAR": "42"}):
            self.assertEqual(ib.get_env_int("MY_VAR", 0), 42)

    def test_returns_default_when_missing(self):
        with patch.dict("os.environ", {}, clear=True):
            self.assertEqual(ib.get_env_int("MISSING_VAR", 99), 99)

    def test_returns_default_on_non_integer(self):
        with patch.dict("os.environ", {"BAD_VAR": "not_an_int"}):
            self.assertEqual(ib.get_env_int("BAD_VAR", 0), 0)


class TestRunIncrementalBatch(unittest.TestCase):
    def _make_spark_mock(self, record_count=10):
        spark = MagicMock()
        df = MagicMock()
        filtered_df = MagicMock()
        filtered_df.count.return_value = record_count
        filtered_df.persist.return_value = filtered_df
        filtered_df.repartition.return_value = filtered_df
        df.persist.return_value = df
        df.filter.return_value = filtered_df
        spark.read.parquet.return_value = df
        return spark, filtered_df

    def test_exits_if_source_data_missing(self):
        spark = MagicMock()
        env = {"TARGET_DATA": "/some/target", "NUM_OF_RECORDS_TO_UPDATE": "10"}
        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(SystemExit):
                ib.run_incremental_batch(spark, batch_id=1)

    def test_exits_if_target_data_missing(self):
        spark = MagicMock()
        env = {"SOURCE_DATA": "/some/source", "NUM_OF_RECORDS_TO_UPDATE": "10"}
        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(SystemExit):
                ib.run_incremental_batch(spark, batch_id=1)

    def test_reads_from_source_and_writes_to_target(self):
        spark, filtered_df = self._make_spark_mock(record_count=10)
        env = {
            "SOURCE_DATA": "s3://bucket/source",
            "TARGET_DATA": "s3://bucket/target/batch_1",
            "NUM_OF_RECORDS_TO_UPDATE": "10",
            "NUM_OF_FILE_GROUPS_TO_TOUCH": "10",
        }
        mock_col = MagicMock()
        with patch.dict("os.environ", env):
            with patch("incremental_batch.col", return_value=mock_col):
                ib.run_incremental_batch(spark, batch_id=1)
        spark.read.parquet.assert_called_once_with("s3://bucket/source")
        filtered_df.write.mode.assert_called_once_with("append")

    def test_filters_correct_values_for_batch_1(self):
        spark, filtered_df = self._make_spark_mock(record_count=5)
        df = spark.read.parquet.return_value
        env = {
            "SOURCE_DATA": "s3://bucket/source",
            "TARGET_DATA": "s3://bucket/target/batch_1",
            "NUM_OF_RECORDS_TO_UPDATE": "5",
            "NUM_OF_FILE_GROUPS_TO_TOUCH": "5",
        }
        mock_col = MagicMock()
        with patch.dict("os.environ", env):
            with patch("incremental_batch.col", return_value=mock_col):
                ib.run_incremental_batch(spark, batch_id=1)
        # col("col_1").isin([...]) - check filter was called
        df.filter.assert_called_once()

    def test_batch_id_gt_one_reads_prior_batch_without_count_validation(self):
        """batch_id > 1: read batch_{n-1} parquet; no total_records_to_process NameError."""
        spark = MagicMock()
        chained_df = MagicMock()
        chained_df.persist.return_value = chained_df
        chained_df.count.return_value = 42
        spark.read.parquet.return_value = chained_df
        env = {
            "SOURCE_DATA": "s3://bucket/source/batch_1",
            "TARGET_DATA": "s3://bucket/target/batch_2",
        }
        with patch.dict("os.environ", env):
            ib.run_incremental_batch(spark, batch_id=2)
        spark.read.parquet.assert_called_once_with("s3://bucket/source/batch_1")
        chained_df.repartition.assert_called_once_with(1)
        repartitioned = chained_df.repartition.return_value
        repartitioned.write.mode.assert_called_once_with("append")


if __name__ == "__main__":
    unittest.main()
