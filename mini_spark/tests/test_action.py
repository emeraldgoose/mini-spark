import unittest

from mini_spark.context import SparkContext

sc = SparkContext()

class TestActions(unittest.TestCase):

    def test_collect(self):
        rdd = sc.parallelize([1, 2, 3], 2)
        result = rdd.collect()
        
        self.assertEqual(result, [1, 2, 3])

    def test_count(self):
        rdd = sc.parallelize([1, 2, 3, 4], 2)
        
        self.assertEqual(rdd.count(), 4)

    def test_take(self):
        rdd = sc.parallelize([1, 2, 3, 4], 2)
        
        self.assertEqual(rdd.take(2), [1, 2])

if __name__ == "__main__":
    unittest.main()