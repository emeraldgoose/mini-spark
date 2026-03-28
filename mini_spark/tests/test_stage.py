import unittest

from mini_spark.context import SparkContext

sc = SparkContext()

class TestExecutionModel(unittest.TestCase):

    def test_stage_split(self):
        rdd = sc.parallelize([
            ("a", 1), ("b", 2), ("a", 3)
        ], 2)

        rdd2 = rdd.map(lambda x: x).reduceByKey(lambda x, y: x + y)

        # 내부 DAG 검사 (구현 필요)
        stages = rdd2.get_stages()

        # map은 narrow → 같은 stage
        # reduceByKey는 shuffle → stage 분리
        self.assertEqual(len(stages), 2)

    def test_task_per_partition(self):
        rdd = sc.parallelize([1, 2, 3, 4], 2)

        tasks = rdd.get_tasks()

        # partition 수 == task 수
        self.assertEqual(len(tasks), 2)

if __name__ == "__main__":
    unittest.main()