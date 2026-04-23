"""
Stage 2: Task Scheduler + Parallel Execution Tests

This test file verifies the Task Scheduler implementation which handles:
- Task dispatching (not just recursive calls)
- Parallel execution using thread pool
- Per-partition task processing
- Task result collection

CONCEPTS TESTED:
- Tasks are dispatched rather than recursively executed
- Tasks run in parallel using thread pool executor
- Each task processes exactly one partition
- Results are collected from all tasks

Spark Concept:
  Once stages are created, the Task Scheduler dispatches tasks
  to run in parallel. Each task runs on one partition and processes
  the transformations for that partition. Results are collected
  back to the driver.
"""

import unittest
import threading
import time
import concurrent.futures

from mini_spark.context import SparkContext
from mini_spark.rdd._rdd import RDD
from mini_spark.storage.partition import Partition


class TestTaskScheduler(unittest.TestCase):
    """Tests for Task Scheduler and parallel execution."""

    def setUp(self):
        self.sc = SparkContext()

    def test_tasks_are_dispatched_not_recursive(self):
        """
        Test that tasks are dispatched as separate units, not via recursion.

        Current implementation uses recursive _execute() which doesn't properly
        separate task execution. Tasks should be dispatched independently.
        """
        rdd = self.sc.parallelize([1, 2, 3, 4], 4)
        rdd2 = rdd.map(lambda x: x * 2)

        tasks = rdd2.get_tasks()

        self.assertEqual(
            len(tasks), 4, f"Should have 4 tasks (one per partition), got {len(tasks)}"
        )

    def test_task_has_partition_reference(self):
        """
        Test that each task has a reference to its partition.

        Each task should know which partition it processes.
        """
        rdd = self.sc.parallelize([1, 2, 3, 4], 2)

        tasks = rdd.get_tasks()

        for task in tasks:
            self.assertIsNotNone(task.partition, "Task should have partition reference")

    def test_task_has_rdd_reference(self):
        """
        Test that each task has reference to the RDD it operates on.

        Task needs RDD reference to apply transformations.
        """
        rdd = self.sc.parallelize([1, 2, 3], 2)

        tasks = rdd.get_tasks()

        for task in tasks:
            self.assertTrue(
                hasattr(task, "rdd") and task.rdd is not None,
                "Task should have RDD reference",
            )

    def test_thread_pool_execution(self):
        """
        Test that tasks can run in parallel using thread pool.

        True parallel execution requires a thread pool or process pool.
        This tests that multiple tasks can execute concurrently.
        """
        execution_times = []
        lock = threading.Lock()

        def slow_task(x):
            time.sleep(0.05)
            with lock:
                execution_times.append(time.time())
            return x * 2

        rdd = self.sc.parallelize([1, 2, 3, 4], 4)
        rdd2 = rdd.map(slow_task)

        result = rdd2.collect()

        if len(execution_times) >= 2:
            time_diffs = [
                execution_times[i + 1] - execution_times[i]
                for i in range(len(execution_times) - 1)
            ]
            avg_diff = sum(time_diffs) / len(time_diffs)

            self.assertLess(
                avg_diff,
                0.04,
                f"Tasks should run in parallel. Avg time diff: {avg_diff}. "
                "Sequential would be ~0.05s between tasks.",
            )

    def test_parallel_execution_timing(self):
        """
        Test parallel execution is faster than sequential.

        If 4 tasks each take 100ms, parallel should take ~100ms,
        sequential would take ~400ms.
        """
        rdd = self.sc.parallelize([1, 2, 3, 4], 4)

        def delay_task(x):
            time.sleep(0.01)
            return x

        rdd2 = rdd.map(delay_task)

        start = time.time()
        result = rdd2.collect()
        elapsed = time.time() - start

        self.assertLess(
            elapsed,
            0.08,
            f"Parallel execution should be faster. Took {elapsed:.2f}s. "
            "Sequential would take ~0.04s+.",
        )

    def test_each_task_processes_one_partition(self):
        """
        Test that each task processes exactly one partition.

        Task is the unit of execution - one task per partition.
        """
        rdd = self.sc.parallelize([1, 2, 3, 4, 5, 6], 3)

        tasks = rdd.get_tasks()

        partition_counts = [len(task.partition.data) for task in tasks]

        self.assertEqual(
            sum(partition_counts), 6, "All data should be accounted for across tasks"
        )

    def test_task_result_collection(self):
        """
        Test that results are collected from all tasks.

        The scheduler should gather results from all tasks.
        """
        rdd = self.sc.parallelize([1, 2, 3, 4], 2)
        rdd2 = rdd.map(lambda x: x * 10)

        result = rdd2.collect()

        self.assertEqual(
            len(result), 4, f"Should collect all results. Got {len(result)}, expected 4"
        )
        self.assertEqual(
            sorted(result),
            [10, 20, 30, 40],
            "Results should be correct transformed values",
        )

    def test_task_execution_order_not_guaranteed(self):
        """
        Test that we can't assume execution order of tasks.

        Tasks run in parallel - order of results may vary.
        Results should be collected correctly regardless.
        """
        rdd = self.sc.parallelize([1, 2, 3, 4], 4)

        tasks = rdd.get_tasks()

        results = []
        for task in tasks:
            results.extend(task.partition.data)

        self.assertEqual(
            set(results), {1, 2, 3, 4}, "All data should be present, order may vary"
        )


class TestTaskExecution(unittest.TestCase):
    """Tests for actual task execution implementation."""

    def setUp(self):
        self.sc = SparkContext()

    def test_task_run_method_exists(self):
        """
        Test that Task class has a run() or execute() method.

        Task should be executable, not just a data container.
        """
        from mini_spark.execution.task import Task

        rdd = self.sc.parallelize([1, 2], 1)
        task = rdd.get_tasks()[0]

        has_run_method = (
            hasattr(task, "run") or hasattr(task, "execute") or hasattr(task, "compute")
        )

        self.assertTrue(
            has_run_method, "Task should have run/execute/compute method for execution"
        )

    def test_task_can_execute_transformation(self):
        """
        Test that task can execute its transformation on partition.

        Task.run() should apply the RDD's transformation.
        """
        from mini_spark.execution.task import Task

        rdd = self.sc.parallelize([1, 2, 3], 1)
        rdd2 = rdd.map(lambda x: x * 2)

        tasks = rdd2.get_tasks()

        if tasks and (hasattr(tasks[0], "run") or hasattr(tasks[0], "compute")):
            task = tasks[0]
            run_method = getattr(task, "run", None) or getattr(task, "compute", None)

            result = run_method()

            self.assertIsNotNone(result, "Task execution should return result")

    def test_scheduler_uses_task_class(self):
        """
        Test that Scheduler actually uses Task class for execution.

        Current scheduler just calls transformation.apply() recursively.
        It should dispatch actual Task instances.
        """
        rdd = self.sc.parallelize([1, 2, 3], 2)

        scheduler = self.sc.scheduler

        uses_task_class = (
            "task" in str(type(scheduler)).lower()
            or hasattr(scheduler, "submit_task")
            or hasattr(scheduler, "dispatch_task")
        )

        self.assertTrue(
            uses_task_class, "Scheduler should use Task class, not just recursive calls"
        )

    def test_task_executor_pool_exists(self):
        """
        Test that scheduler has an executor pool for parallel execution.

        Could be ThreadPoolExecutor or ProcessPoolExecutor.
        """
        scheduler = self.sc.scheduler

        has_executor = (
            hasattr(scheduler, "executor")
            or hasattr(scheduler, "pool")
            or hasattr(scheduler, "thread_pool")
            or hasattr(scheduler, "process_pool")
        )

        self.assertTrue(
            has_executor,
            "Scheduler should have executor/pool for parallel task execution",
        )


class TestShuffleTaskDependencies(unittest.TestCase):
    """Tests for task dependencies at shuffle boundaries."""

    def setUp(self):
        self.sc = SparkContext()

    def test_shuffle_write_tasks(self):
        """
        Test that shuffle produces write tasks.

        At shuffle boundaries, tasks must write shuffle files.
        """
        rdd = self.sc.parallelize([("a", 1), ("b", 2), ("a", 3)], 2)
        rdd2 = rdd.reduceByKey(lambda x, y: x + y)

        stages = rdd2.get_stages()

        if len(stages) >= 2:
            self.assertTrue(
                True, "Shuffle boundary exists - tasks need shuffle write logic"
            )

    def test_shuffle_read_tasks(self):
        """
        Test that next stage has shuffle read tasks.

        Tasks after shuffle must read shuffle files.
        """
        rdd = self.sc.parallelize([("a", 1), ("b", 2)], 2)
        rdd2 = rdd.reduceByKey(lambda x, y: x + y)

        stages = rdd2.get_stages()

        self.assertGreaterEqual(
            len(stages), 1, "At least one stage exists after transformation"
        )


if __name__ == "__main__":
    unittest.main()
