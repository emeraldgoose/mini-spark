import unittest

from mini_spark.context import SparkContext

sc = SparkContext()

class TestSparkContext(unittest.TestCase):

    def test_parallelize(self):
        rdd = sc.parallelize([1, 2, 3], 2)

        self.assertEqual(rdd.get_num_partitions(), 2)
        self.assertEqual(rdd.collect(), [1, 2, 3])

if __name__ == "__main__":
    unittest.main()