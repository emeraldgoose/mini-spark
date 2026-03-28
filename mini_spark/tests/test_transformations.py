import unittest

from mini_spark.context import SparkContext

sc = SparkContext()

class TestTransformations(unittest.TestCase):

    def test_map(self):
        rdd = sc.parallelize([1, 2, 3], 2)
        rdd2 = rdd.map(lambda x: x * 2)
        result = rdd2.collect()
    
        self.assertEqual(result, [2, 4, 6])

    def test_filter(self):
        rdd = sc.parallelize([1, 2, 3, 4], 2)
        rdd2 = rdd.filter(lambda x: x % 2 == 0)
        result = rdd2.collect()
    
        self.assertEqual(result, [2, 4])

    def test_flatMap(self):
        rdd = sc.parallelize(["a b", "c"], 2)
        rdd2 = rdd.flatMap(lambda x: x.split())
        result = rdd2.collect()
    
        self.assertEqual(result, ["a", "b", "c"])

if __name__ == "__main__":
    unittest.main()