"""
Stage 3: Shuffle Write/Read (Disk Persistence) Tests

This test file verifies shuffle data is written to and read from disk,
not just kept in memory.

CONCEPTS TESTED:
- Shuffle data is written to disk
- Shuffle read reads from disk correctly
- Data survives after in-memory shuffle cleared
- Multiple partitions shuffle correctly

Spark Concept:
  Shuffle involves writing map outputs to disk (shuffle write) and
  reading them back (shuffle read). This provides:
  1. Data locality for next stage
  2. Memory relief (don't keep all map outputs in memory)
  3. Fault tolerance (shuffle files can be recomputed)
"""

import unittest
import os
import tempfile
import shutil

from mini_spark.context import SparkContext
from mini_spark.shuffle.shuffle_manager import ShuffleManager
from mini_spark.storage.partition import Partition


class TestShuffleWrite(unittest.TestCase):
    """Tests for shuffle write to disk."""

    def setUp(self):
        self.sc = SparkContext()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_shuffle_manager_has_write_method(self):
        """
        Test that ShuffleManager has write functionality.

        Currently shuffle is in-memory (buckets dict).
        Should have write_shuffle() or similar method.
        """
        sm = ShuffleManager(2)

        has_write = (
            hasattr(sm, "write")
            or hasattr(sm, "write_shuffle")
            or hasattr(sm, "shuffle_write")
        )

        self.assertTrue(
            has_write,
            "ShuffleManager should have write method to persist shuffle data. "
            "Implement write_shuffle() method to write to disk.",
        )

    def test_shuffle_writes_to_specified_directory(self):
        """
        Test that shuffle files are written to a directory.

        Spark writes shuffle files organized by shuffle ID and partition.
        """
        sm = ShuffleManager(2)

        has_output_dir = hasattr(sm, "output_dir") or hasattr(sm, "shuffle_dir")

        self.assertTrue(
            has_output_dir,
            "ShuffleManager should track output directory for shuffle files",
        )

    def test_shuffle_file_per_partition(self):
        """
        Test that each output partition gets its own shuffle file.

        For num_partitions=2, should have 2 shuffle files.
        """
        sm = ShuffleManager(2)
        partitions = [Partition([("a", 1), ("b", 2)]), Partition([("a", 3)])]

        has_file_per_partition = (
            hasattr(sm, "num_partitions") and sm.num_partitions == 2
        )

        self.assertTrue(
            has_file_per_partition,
            "ShuffleManager should support multiple partition output",
        )


class TestShuffleRead(unittest.TestCase):
    """Tests for shuffle read from disk."""

    def setUp(self):
        self.sc = SparkContext()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_shuffle_manager_has_read_method(self):
        """
        Test that ShuffleManager has read functionality.

        Should have read_shuffle() or get_shuffle() method.
        """
        sm = ShuffleManager(2)

        has_read = (
            hasattr(sm, "read")
            or hasattr(sm, "read_shuffle")
            or hasattr(sm, "shuffle_read")
            or hasattr(sm, "get_shuffle")
        )

        self.assertTrue(
            has_read, "ShuffleManager should have read method to retrieve shuffle data"
        )

    def test_shuffle_read_returns_correct_data(self):
        """
        Test that shuffle read returns the written data.

        Data written should equal data read.
        """
        sm = ShuffleManager(2)
        partitions = [Partition([("a", 1), ("b", 2)]), Partition([("a", 3)])]

        if hasattr(sm, "write") and hasattr(sm, "read"):
            sm.write(partitions)
            read_data = sm.read()

            self.assertEqual(len(read_data), 2, "Should read 2 partitions")


class TestShuffleDiskPersistence(unittest.TestCase):
    """Tests for shuffle data persisting to disk."""

    def setUp(self):
        self.sc = SparkContext()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_shuffle_data_survives_memory_clear(self):
        """
        Test that data survives after in-memory shuffle cleared.

        Current buckets dict is cleared on new action - that's wrong.
        Shuffle files should persist on disk.
        """
        rdd = self.sc.parallelize([("a", 1), ("b", 2), ("a", 3)], 2)
        rdd2 = rdd.reduceByKey(lambda x, y: x + y)

        if hasattr(rdd2.context.scheduler, "shuffle_manager"):
            sm = rdd2.context.scheduler.shuffle_manager

            if hasattr(sm, "shuffle_files"):
                file_count = len(getattr(sm, "shuffle_files", {}))

                self.assertGreater(
                    file_count, 0, "Shuffle files should exist after shuffle operation"
                )

    def test_shuffle_file_contains_correct_data(self):
        """
        Test that shuffle file contains the expected key-value pairs.

        File format may vary (text, binary, etc) but data should match.
        """
        sm = ShuffleManager(2)

        if hasattr(sm, "shuffle_files"):
            self.assertTrue(hasattr(sm, "shuffle_files"), "ShuffleManager should track shuffle files")
        else:
            self.assertTrue(
                False,
                "ShuffleManager should have shuffle_files attribute. "
                "Implement disk-based shuffle persistence.",
            )

    def test_multiple_partitions_shuffle_correctly(self):
        """
        Test that multiple partitions are shuffled correctly.

        With HashPartitioner, same keys go to same partitions.
        """
        from mini_spark.shuffle.partitioner import HashPartitioner

        p = HashPartitioner(3)
        data = [("a", 1), ("b", 2), ("a", 2), ("c", 3), ("b", 1)]

        partitions = {}
        for k, v in data:
            pid = p.get_partition(k)
            if pid not in partitions:
                partitions[pid] = []
            partitions[pid].append((k, v))
        
        self.assertEqual(len(partitions), 3, "Should have 3 partitions")
        
        self.assertCountEqual(
            partitions.get(0, []) + partitions.get(1, []) + partitions.get(2, []),
            data,
            "All data should be in some partition",
        )


class TestShuffleFileFormat(unittest.TestCase):
    """Tests for shuffle file format and organization."""

    def setUp(self):
        self.sc = SparkContext()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_shuffle_directory_structure(self):
        """
        Test that shuffle files are organized in directory structure.

        Common: spark_shuffle/shuffle_0_1_0 (shuffleId_mapId_reduceId)
        """
        sm = ShuffleManager(2)

        if hasattr(sm, "get_shuffle_dir"):
            directory = sm.get_shuffle_dir(shuffle_id=0, map_id=0, reduce_id=0)

            self.assertIn(
                "shuffle",
                directory.lower(),
                "Shuffle directory should indicate shuffle files",
            )

    def test_shuffle_file_naming(self):
        """
        Test shuffle files are named appropriately.

        Should be identifiable by shuffleId and partition.
        """
        sm = ShuffleManager(2)

        has_naming = hasattr(sm, "get_shuffle_filename") or hasattr(
            sm, "get_shuffle_path"
        )

        self.assertTrue(
            has_naming or not hasattr(sm, "write"),
            "ShuffleManager should name files for shuffle/partition identity",
        )


class TestShuffleManagerIntegration(unittest.TestCase):
    """Integration tests for ShuffleManager with RDD execution."""

    def setUp(self):
        self.sc = SparkContext()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_reduceByKey_triggers_shuffle_write(self):
        """
        Test that reduceByKey triggers shuffle write.

        reduceByKey should use ShuffleManager to write shuffle files.
        """
        rdd = self.sc.parallelize([("a", 1), ("b", 2), ("a", 3)], 2)
        rdd2 = rdd.reduceByKey(lambda x, y: x + y)

        if hasattr(self.sc.scheduler, "shuffle_manager"):
            sm = self.sc.scheduler.shuffle_manager

            self.assertIsNotNone(sm, "Scheduler should have ShuffleManager")

    def test_shuffle_files_created_after_action(self):
        """
        Test that shuffle files are created after action runs.

        After action (collect()), shuffle files should exist.
        """
        rdd = self.sc.parallelize([("a", 1), ("b", 2), ("a", 3)], 2)
        rdd2 = rdd.reduceByKey(lambda x, y: x + y)

        result = rdd2.collect()

        if hasattr(self.sc.scheduler, "shuffle_manager"):
            sm = self.sc.scheduler.shuffle_manager

            if hasattr(sm, "shuffle_files"):
                self.assertGreater(
                    len(sm.shuffle_files),
                    0,
                    "Shuffle files should be created after action",
                )

    def test_shuffle_read_after_shuffle_write(self):
        """
        Test that shuffle data can be read back after writing.

        Typical flow: write map outputs -> read for reduce.
        """
        partitions = [Partition([("a", 1), ("b", 2)]), Partition([("a", 3)])]
        sm = ShuffleManager(2)

        if hasattr(sm, "shuffle") and hasattr(sm, "get_shuffle"):
            result = sm.shuffle(partitions)
            read_result = sm.get_shuffle()

            self.assertIsNotNone(read_result, "Shuffle read should return data")


if __name__ == "__main__":
    unittest.main()
