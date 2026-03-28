import unittest
from mini_spark.context import SparkContext

sc = SparkContext()

class TestIntegration(unittest.TestCase):
    
    def test_word_count(self):
        data = ["a b a", "b c"]
        rdd = (
            sc.parallelize(data)
                .flatMap(lambda x: x.split())
                .map(lambda x: (x, 1))
                .reduceByKey(lambda a, b: a + b)
        )
        result = sorted(rdd.collect())
        expected = [("a", 2), ("b", 2), ("c", 1)]
        
        self.assertEqual(result, expected)

if __name__ == "__main__":
    unittest.main()