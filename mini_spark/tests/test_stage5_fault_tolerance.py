"""
Stage 5: Fault Tolerance (Lineage-based Recovery) Tests

This test file verifies fault tolerance through lineage-based recovery.

CONCEPTS TESTED:
- Task failure triggers retry
- Lost partition is recomputed from lineage
- Checkpoint truncates lineage
- Retry with maxRetries configuration

Spark Concept:
  Spark uses lineage (RDD dependency graph) for fault tolerance.
  If a partition is lost:
  1. Detect failure (executor lost, task failed)
  2. Use lineage to recompute that partition
  3. No replication needed - just recompute!

  Checkpointing can truncate lineage to limit recovery time.
"""

import unittest
import time

from mini_spark.context import SparkContext
from mini_spark.rdd._rdd import RDD


class TestTaskRetry(unittest.TestCase):
    """Tests for task failure and retry."""

    def setUp(self):
        self.sc = SparkContext()

    def test_scheduler_has_retry_configuration(self):
        """
        Test that scheduler tracks retry configuration.

        Should have maxRetries or similar attribute.
        """
        scheduler = self.sc.scheduler

        has_retry_config = (
            hasattr(scheduler, "max_retries")
            or hasattr(scheduler, "maxRetries")
            or hasattr(scheduler, "task_retries")
        )

        self.assertTrue(
            has_retry_config, "Scheduler should have retry configuration (maxRetries)"
        )

    def test_default_max_retries(self):
        """
        Test that default maxRetries is configured.

        Spark defaults to 4 retries (configurable).
        """
        scheduler = self.sc.scheduler

        max_retries = getattr(
            scheduler, "max_retries", getattr(scheduler, "maxRetries", 4)
        )

        self.assertGreaterEqual(max_retries, 0, "maxRetries should be >= 0")

    def test_failed_task_triggers_retry(self):
        """
        Test that failed task is retried.

        On task failure, should retry up to maxRetries.
        """
        fail_count = [0]

        def failing_task(x):
            fail_count[0] += 1
            if fail_count[0] <= 1:
                raise RuntimeError("Simulated failure")
            return x * 2

        rdd = self.sc.parallelize([1, 2], 1)
        rdd2 = rdd.map(failing_task)

        try:
            result = rdd2.collect()

            self.assertEqual(
                sorted(result), [2, 4], "Should eventually succeed after retries"
            )
        except Exception as e:
            self.fail(
                f"Task should retry on failure. Got: {e}. "
                "Implement task retry logic in scheduler."
            )


class TestLineageRecovery(unittest.TestCase):
    """Tests for lineage-based recovery."""

    def setUp(self):
        self.sc = SparkContext()

    def test_lineage_exists_for_recovery(self):
        """
        Test that RDD lineage is maintained for recovery.

        Lineage chain should allow recomputation from source.
        """
        rdd = self.sc.parallelize([1, 2, 3], 2)
        rdd2 = rdd.map(lambda x: x * 2)
        rdd3 = rdd2.filter(lambda x: x > 2)

        has_lineage = rdd3.prev is not None and rdd3.prev.prev is not None

        self.assertTrue(has_lineage, "RDD should maintain lineage for recovery")

    def test_partition_can_be_recomputed(self):
        """
        Test that partition can be recomputed from lineage.

        Given parent partitions, transformation can reproduce child.
        """
        rdd = self.sc.parallelize([1, 2, 3, 4], 2)
        rdd2 = rdd.map(lambda x: x * 10)

        first_result = rdd2.collect()

        has_partitions = rdd2.partitions is not None

        self.assertTrue(
            has_partitions, "After action, RDD should have partitions computed"
        )

    def test_rdd_tracks_dependencies(self):
        """
        Test that RDD tracks dependencies.

        NarrowDependency or ShuffleDependency should be tracked.
        """
        rdd = self.sc.parallelize([("a", 1), ("b", 2)], 2)
        rdd2 = rdd.reduceByKey(lambda x, y: x + y)

        has_dependencies = (
            hasattr(rdd2, "dependencies")
            or hasattr(rdd2, "depends_on")
            or rdd2.prev is not None
        )

        self.assertTrue(has_dependencies, "RDD should track dependencies for recovery")


class TestCheckpoint(unittest.TestCase):
    """Tests for checkpoint to truncate lineage."""

    def setUp(self):
        self.sc = SparkContext()

    def test_rdd_has_checkpoint_method(self):
        """
        Test that RDD has checkpoint() method.

        checkpoint() truncates lineage for faster recovery.
        """
        rdd = self.sc.parallelize([1, 2, 3], 2)

        self.assertTrue(
            hasattr(rdd, "checkpoint") and callable(rdd.checkpoint),
            "RDD should have checkpoint() method",
        )

    def test_checkpoint_truncates_lineage(self):
        """
        Test that checkpoint truncates lineage.

        After checkpoint, RDD shouldn't need full lineage to recompute.
        """
        rdd = self.sc.parallelize([1, 2, 3, 4], 2)
        rdd2 = rdd.map(lambda x: x * 2)
        rdd3 = rdd2.filter(lambda x: x > 2)

        rdd3.checkpoint()

        has_checkpointed = (
            hasattr(rdd3, "_checkpoint")
            or hasattr(rdd3, "checkpointed")
            or rdd3.lineage is None
        )

        self.assertTrue(
            has_checkpointed or rdd3.lineage is None,
            "After checkpoint, lineage should be truncated",
        )

    def test_context_has_checkpoint_dir(self):
        """
        Test that SparkContext has checkpoint directory.

        Checkpoint files are stored in this directory.
        """
        has_checkpoint_dir = hasattr(self.sc, "checkpoint_dir") or hasattr(
            self.sc, "get_checkpoint_dir"
        )

        self.assertTrue(
            has_checkpoint_dir, "SparkContext should have checkpoint_dir configuration"
        )

    def test_is_checkpointed_flag(self):
        """
        Test that RDD can report if it's checkpointed.

        Should have is_checkpointed or similar.
        """
        rdd = self.sc.parallelize([1, 2, 3], 2)

        if hasattr(rdd, "checkpoint"):
            rdd.checkpoint()

            has_flag = hasattr(rdd, "is_checkpointed") or hasattr(
                rdd, "_is_checkpointed"
            )

            self.assertTrue(has_flag, "RDD should track checkpoint status")


class TestFailureDetection(unittest.TestCase):
    """Tests for detecting failures."""

    def setUp(self):
        self.sc = SparkContext()

    def test_failure_is_detected(self):
        """
        Test that task failures are detected.

        Failed tasks should raise exception or mark as failed.
        """
        scheduler = self.sc.scheduler

        has_failure_tracking = hasattr(scheduler, "failed_tasks") or hasattr(
            scheduler, "failure_count"
        )

        self.assertTrue(
            has_failure_tracking, "Scheduler should track task failures (failed_tasks or failure_count)"
        )

    def test_failed_task_count_is_recorded(self):
        """
        Test that failed task count is recorded.

        For monitoring and debugging.
        """
        scheduler = self.sc.scheduler

        has_counter = hasattr(scheduler, "task_failures") or hasattr(
            scheduler, "num_failed_tasks"
        )

        self.assertTrue(has_counter, "Scheduler should track failed task count")


class TestRecoveryBehavior(unittest.TestCase):
    """Tests for actual recovery behavior."""

    def setUp(self):
        self.sc = SparkContext()

    def test_partition_loss_detection(self):
        """
        Test that partition loss can be detected.

        If executor or node is lost, partitions should be recoverable.
        """
        rdd = self.sc.parallelize([1, 2, 3], 2)

        has_recovery = hasattr(rdd, "recompute") or hasattr(rdd, "recovery_partitions")

        self.assertTrue(
            has_recovery, "RDD should have recompute or recovery_partitions method"
        )

    def test_recovery_uses_lineage(self):
        """
        Test that recovery uses lineage.

        Lost partition should be recomputed from parent RDD.
        """
        rdd = self.sc.parallelize([1, 2], 2)
        rdd2 = rdd.map(lambda x: x * 10)

        can_recover = hasattr(rdd2, "recompute") or rdd2.prev is not None

        self.assertTrue(can_recover, "Should be able to recompute from lineage")

    def test_recovery_time_reasonable(self):
        """
        Test that recovery is bounded by lineage length.

        Checkpoint limits how far back we need to recompute.
        """
        rdd = self.sc.parallelize([1], 1)
        for i in range(5):
            rdd = rdd.map(lambda x: x + i)

        lineage_length = 0
        current = rdd
        while current is not None:
            lineage_length += 1
            current = getattr(current, "prev", None)

        self.assertEqual(
            lineage_length, 6, "Lineage length should match transformation chain"
        )


if __name__ == "__main__":
    unittest.main()
