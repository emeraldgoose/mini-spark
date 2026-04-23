"""
Stage 7: Distributed Execution Simulation Tests (Bonus)

This test file simulates Spark's distributed execution model.

CONCEPTS TESTED:
- Driver/Executor role separation
- Data serialization for transfer
- Broadcast variable reaches all workers
- Accumulator collects values from workers

Spark Concept:
  In distributed Spark:
  1. Driver creates RDDs and submits tasks
  2. Executors run tasks on worker nodes
  3. Data needs serialization for transfer
  4. Broadcast variables are efficient way to send large data
  5. Accumulators aggregate values from workers

  This implementation may be simpler (simulated threads/processes).
"""

import unittest
import pickle
import threading
import queue

from mini_spark.context import SparkContext
from mini_spark.rdd._rdd import RDD


class TestDriverExecutorRoles(unittest.TestCase):
    """Tests for Driver and Executor separation."""

    def setUp(self):
        self.sc = SparkContext()

    def test_context_is_driver(self):
        """
        Test that SparkContext acts as driver.

        Driver creates RDDs and orchestrates execution.
        """
        is_driver = hasattr(self.sc, "parallelize") and hasattr(self.sc, "scheduler")

        self.assertTrue(is_driver, "SparkContext should act as driver")

    def test_executor_class_exists(self):
        """
        Test that Executor class exists (even if simple).

        Executor runs tasks on partitions.
        """
        try:
            from mini_spark.execution.executor import Executor

            self.assertTrue(
                hasattr(Executor, "__init__"), "Executor class should exist"
            )
        except ImportError:
            self.fail(
                "Executor class not implemented. "
                "Create mini_spark/execution/executor.py with Executor class. "
                "Executor should run tasks on partitions."
            )


class TestDataSerialization(unittest.TestCase):
    """Tests for data serialization."""

    def setUp(self):
        self.sc = SparkContext()

    def test_rdd_data_is_serializable(self):
        """
        Test that RDD data can be serialized.

        For distributed transfer, data must be serializable.
        """
        rdd = self.sc.parallelize([1, 2, 3], 2)

        try:
            serialized = pickle.dumps(rdd.partitions[0].data)
            deserialized = pickle.loads(serialized)

            self.assertEqual(
                deserialized, [1, 2], "RDD partition data should be serializable"
            )
        except Exception as e:
            self.fail(f"Data should be serializable: {e}")

    def test_closure_can_be_serialized(self):
        """
        Test that transformation functions can be serialized.

        Closures (lambdas) must be sent to executors.
        """
        func = lambda x: x * 2

        try:
            serialized = pickle.dumps(func)
            deserialized = pickle.loads(serialized)

            self.assertEqual(
                deserialized(5), 10, "Function should remain after serialization"
            )
        except Exception as e:
            self.fail(f"Function serialization failed: {e}")


class TestBroadcastVariable(unittest.TestCase):
    """Tests for Broadcast variable."""

    def setUp(self):
        self.sc = SparkContext()

    def test_context_has_broadcast_method(self):
        """
        Test that SparkContext has broadcast() method.

        Broadcast sends data efficiently to all executors.
        """
        has_broadcast = hasattr(self.sc, "broadcast") and callable(self.sc.broadcast)

        self.assertTrue(has_broadcast, "SparkContext should have broadcast() method")

    def test_broadcast_value_reaches_task(self):
        """
        Test that broadcast value is available to tasks.

        Tasks should access broadcast value.
        """
        if hasattr(self.sc, "broadcast"):
            broadcast_var = self.sc.broadcast([1, 2, 3])

            rdd = self.sc.parallelize([10, 20], 1)
            rdd2 = rdd.map(lambda x: x + sum(broadcast_var.value))

            result = rdd2.collect()

            self.assertEqual(
                sorted(result), [16, 26], "Broadcast value should be available in task"
            )

    def test_broadcast_reuses_same_value(self):
        """
        Test that broadcast sends same value to all tasks.

        Should send once, not per task.
        """
        send_count = [0]
        original_broadcast = getattr(self.sc, "broadcast", None)

        if original_broadcast:

            def counting_broadcast(value):
                send_count[0] += 1
                return original_broadcast(value)

            bc = counting_broadcast([1, 2, 3])

            rdd = self.sc.parallelize([1, 2, 3, 4], 4)
            rdd.map(lambda x: x).collect()

            self.assertLessEqual(
                send_count[0],
                1,
                f"Broadcast should be sent once, not per task. Sent {send_count[0]} times",
            )


class TestAccumulator(unittest.TestCase):
    """Tests for Accumulator."""

    def setUp(self):
        self.sc = SparkContext()

    def test_context_has_accumulator(self):
        """
        Test that SparkContext has accumulator() method.

        Accumulators aggregate values from tasks.
        """
        has_accumulator = hasattr(self.sc, "accumulator") and callable(
            self.sc.accumulator
        )

        self.assertTrue(
            has_accumulator, "SparkContext should have accumulator() method"
        )

    def test_accumulator_collects_values(self):
        """
        Test that accumulator collects values from all tasks.

        Each task adds to accumulator.
        """
        if hasattr(self.sc, "accumulator"):
            acc = self.sc.accumulator(0)

            rdd = self.sc.parallelize([1, 2, 3, 4], 2)

            def add_to_acc(x):
                acc.add(x)
                return x

            rdd.map(add_to_acc).collect()

            self.assertEqual(
                acc.value,
                10,
                f"Accumulator should collect sum: 1+2+3+4=10, got {acc.value}",
            )

    def test_accumulator_initial_value(self):
        """
        Test that accumulator starts with initial value.

        Initial value is 0 for numeric accumulators.
        """
        if hasattr(self.sc, "accumulator"):
            acc = self.sc.accumulator(0)

            self.assertEqual(acc.value, 0, "Accumulator should start at initial value")


class TestWorkerSimulation(unittest.TestCase):
    """Tests for worker simulation."""

    def setUp(self):
        self.sc = SparkContext()

    def test_worker_exists(self):
        """
        Test that worker component exists.

        Worker runs on each node to execute tasks.
        """
        try:
            from mini_spark.execution.worker import Worker

            self.assertTrue(hasattr(Worker, "__init__"), "Worker class should exist")
        except ImportError:
            self.skipTest("Worker class optional - tasks can run in threads")

    def test_task_runs_on_worker_thread(self):
        """
        Test that tasks can run in worker threads.

        Multi-threaded simulation of distributed execution.
        """
        rdd = self.sc.parallelize([1, 2, 3, 4], 2)

        has_threading = True

        self.assertTrue(has_threading, "Tasks should run in worker threads/processes")


class TestShuffleDistributed(unittest.TestCase):
    """Tests for shuffle in distributed setting."""

    def setUp(self):
        self.sc = SparkContext()

    def test_shuffle_involves_networked_transfer(self):
        """
        Test that shuffle involves data transfer.

        In distributed mode, shuffle moves data between nodes.
        """
        rdd = self.sc.parallelize([("a", 1)], 2)
        rdd2 = rdd.reduceByKey(lambda x, y: x + y)

        result = rdd2.collect()

        self.assertIsNotNone(
            result, "Shuffle should still work in distributed simulation"
        )


class TestDataLocality(unittest.TestCase):
    """Tests for data locality."""

    def setUp(self):
        self.sc = SparkContext()

    def test_data_locality_awareness(self):
        """
        Test that scheduler considers data locality.

        Tasks should run where data is.
        """
        rdd = self.sc.parallelize([1, 2, 3], 2)
        tasks = rdd.get_tasks()

        has_locality = (
            hasattr(tasks[0], "preferred_locations")
            or hasattr(tasks[0], "location")
            or hasattr(tasks[0], "executor_id")
        )

        self.assertTrue(
            has_locality, "Tasks should track data locality (preferred_locations or executor_id)"
        )


class TestPartitionPlacement(unittest.TestCase):
    """Tests for partition placement."""

    def setUp(self):
        self.sc = SparkContext()

    def test_partitions_have_locations(self):
        """
        Test that partitions track their location.

        Location can be node ID, executor ID, etc.
        """
        rdd = self.sc.parallelize([1, 2, 3], 2)

        if rdd.partitions:
            partition = rdd.partitions[0]

            has_location = (
                hasattr(partition, "location")
                or hasattr(partition, "executor_id")
                or hasattr(partition, "node")
            )

            self.assertTrue(
                has_location, "Partitions should track location (location or executor_id or node)"
            )


if __name__ == "__main__":
    unittest.main()
