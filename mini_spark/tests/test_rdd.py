import unittest

from mini_spark.context import SparkContext

sc = SparkContext()

class TestRDD(unittest.TestCase):
    
    def test_parallelize_partitions(self):
        rdd = sc.parallelize([1, 2, 3, 4, 5], num_partitions=2)
        
        self.assertEqual(len(rdd.partitions), 2)
        self.assertEqual(sum(len(p) for p in rdd.partitions), 5)

    def test_map_filter_lazy(self):
        rdd = sc.parallelize([1, 2, 3, 4])
        rdd2 = rdd.map(lambda x: x * 2).filter(lambda x: x > 4)
        
        # lazy 확인
        self.assertIsNotNone(rdd2.prev)
        
        # action collect 후 결과 확인
        result = rdd2.collect()
        self.assertEqual(result, [6, 8])

    def test_flatMap(self):
        rdd = sc.parallelize(["a b", "c"])
        rdd2 = rdd.flatMap(lambda x: x.split())
        result = rdd2.collect()
        
        self.assertEqual(sorted(result), ["a", "b", "c"])

if __name__ == "__main__":
    unittest.main()