import unittest

from mini_spark.context import SparkContext

sc = SparkContext()

class TestScheduler(unittest.TestCase):
    
    def test_stage_execution(self):
        rdd = (
            sc.parallelize([1, 2, 3, 4])
                .map(lambda x: x * 2)
                .filter(lambda x: x > 4)
        )
        # collect 실행 -> scheduler 실행
        result = rdd.collect()
        self.assertEqual(result, [6, 8])

    def test_multiple_stages(self):
        rdd = (
            sc.parallelize([("a", 1), ("b", 2), ("a", 3)])
                .map(lambda x: (x[0], x[1] * 10))
                .groupByKey()
                .map(lambda x: (x[0], sum(x[1])))
        )
        result = rdd.collect()
        expected = [("a", 40), ("b", 20)]
        self.assertEqual(sorted(result), sorted(expected))

if __name__ == "__main__":
    unittest.main()