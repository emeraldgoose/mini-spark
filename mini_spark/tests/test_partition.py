import unittest

from mini_spark.context import SparkContext

sc = SparkContext()

class TestPartitioning(unittest.TestCase):

    def test_partition_preserved(self):
        rdd = sc.parallelize([1, 2, 3, 4], 2)
        rdd2 = rdd.map(lambda x: x * 2)

        self.assertEqual(rdd.get_num_partitions(), rdd2.get_num_partitions())

    def test_shuffle_changes_partition(self):
        rdd = sc.parallelize([
            ("a", 1), ("b", 2), ("a", 3)
        ], 2)

        rdd2 = rdd.groupByKey()

        # shuffle 이후 partition 변경 가능
        self.assertTrue(rdd2.get_num_partitions() >= 1)

if __name__ == "__main__":
    unittest.main()