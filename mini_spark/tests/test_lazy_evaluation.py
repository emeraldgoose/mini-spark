import unittest

from mini_spark.context import SparkContext

sc = SparkContext()

class TestLazyEvaluation(unittest.TestCase):

    def test_lazy_execution(self):
        executed = {"flag": False}

        def func(x):
            executed["flag"] = True
            return x * 2

        rdd = sc.parallelize([1, 2, 3], 2)
        rdd2 = rdd.map(func)

        # 아직 실행 안됨
        self.assertFalse(executed["flag"])

        rdd2.collect()

        # action 이후 실행됨
        self.assertTrue(executed["flag"])

if __name__ == "__main__":
    unittest.main()