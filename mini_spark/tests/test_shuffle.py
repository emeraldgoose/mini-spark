import unittest

from mini_spark.context import SparkContext
from mini_spark.storage.partition import Partition
from mini_spark.shuffle.shuffle_manager import ShuffleManager
from mini_spark.shuffle.partitioner import HashPartitioner

sc = SparkContext()

class TestShuffle(unittest.TestCase):
    def test_groupByKey(self):
        partitions = [
            Partition([("a", 1), ("b", 2)]),
            Partition([("a", 3)])
        ]
        sm = ShuffleManager()
        result = sm.shuffle_groupByKey(partitions)
        expected = [[("a", [1, 3]), ("b", [2])]]
        
        self.assertEqual(result, expected)

    def test_reduceByKey(self):
        partitions = [
            Partition([("a", 1), ("b", 2)]),
            Partition([("a", 3), ("b", 1)])
        ]
        sm = ShuffleManager()
        result = sm.shuffle_reduceByKey(partitions, lambda x, y: x + y)
        expected = [[("a", 4), ("b", 3)]]
        
        self.assertEqual(result, expected)

    def test_partitioner(self):
        p = HashPartitioner(2)
        keys = ["a", "b", "c", "a"]
        parts = [p.get_partition(k) for k in keys]
        
        self.assertTrue(all(0 <= pid < 2 for pid in parts))
        self.assertEqual(parts[0], parts[3], "같은 key는 같은 partition")

    def test_groupByKey(self):
        rdd = sc.parallelize([
            ("a", 1), ("b", 2), ("a", 3)
        ], 2)

        result = dict(rdd.groupByKey().collect())

        self.assertEqual(result["a"], [1, 3])
        self.assertEqual(result["b"], [2])

    def test_reduceByKey(self):
        rdd = sc.parallelize([
            ("a", 1), ("b", 2), ("a", 3)
        ], 2)

        result = dict(rdd.reduceByKey(lambda x, y: x + y).collect())

        self.assertEqual(result["a"], 4)
        self.assertEqual(result["b"], 2)


if __name__ == "__main__":
    unittest.main()