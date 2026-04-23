"""
Stage 6: join + Wide Dependency Tests

This test file verifies join operations and wide dependency handling.

CONCEPTS TESTED:
- rdd1.join(rdd2) returns correct paired results
- join triggers shuffle (wide dependency)
- cogroup() with multiple RDDs
- join result partitioning

Spark Concept:
  join() is a wide transformation requiring shuffle:
  1. Both RDDs are partitioned by key
  2. Co-locate records with same key
  3. Join matching keys

  join is implemented using co-group:
  - Group records by key from both RDDs
  - For each key, compute cartesian product of values
"""

import unittest

from mini_spark.context import SparkContext
from mini_spark.rdd._rdd import RDD


class TestJoinBasic(unittest.TestCase):
    """Basic tests for join() operation."""

    def setUp(self):
        self.sc = SparkContext()

    def test_rdd_has_join_method(self):
        """
        Test that RDD has join() method.

        join() joins two RDDs by key.
        """
        rdd1 = self.sc.parallelize([("a", 1), ("b", 2)], 2)

        self.assertTrue(
            hasattr(rdd1, "join") and callable(rdd1.join),
            "RDD should have join() method",
        )

    def test_join_returns_rdd(self):
        """
        Test that join() returns an RDD.

        join() is a transformation (lazy).
        """
        rdd1 = self.sc.parallelize([("a", 1), ("b", 2)], 2)
        rdd2 = self.sc.parallelize([("a", 10), ("b", 20)], 2)

        joined = rdd1.join(rdd2)

        self.assertIsInstance(joined, RDD, "join() should return an RDD")

    def test_join_basic_functionality(self):
        """
        Test that join returns correct paired results.

        join(("a", 1), ("a", 10)) => ("a", (1, 10))
        """
        rdd1 = self.sc.parallelize([("a", 1), ("b", 2)], 2)
        rdd2 = self.sc.parallelize([("a", 10), ("b", 20)], 2)

        result = rdd1.join(rdd2).collect()
        result_dict = dict(result)

        self.assertIn("a", result_dict, "Key 'a' should be in join result")
        self.assertEqual(
            result_dict["a"], (1, 10), "join(('a', 1), ('a', 10)) => ('a', (1, 10))"
        )

    def test_join_only_matching_keys(self):
        """
        Test that join only returns keys present in both RDDs.

        'b' only in rdd1, 'c' only in rdd2 -> not in result
        """
        rdd1 = self.sc.parallelize([("a", 1), ("b", 2)], 2)
        rdd2 = self.sc.parallelize([("a", 10), ("c", 30)], 2)

        result = rdd1.join(rdd2).collect()
        keys = set(k for k, v in result)

        self.assertEqual(
            keys, {"a"}, "Only keys present in BOTH RDDs should be in join result"
        )


class TestJoinMultiple(unittest.TestCase):
    """Tests for join with multiple partitions."""

    def setUp(self):
        self.sc = SparkContext()

    def test_join_with_more_partitions(self):
        """
        Test join with more partitions.

        Data should be correctly partitioned.
        """
        rdd1 = self.sc.parallelize([("a", 1), ("a", 2), ("b", 3), ("c", 4)], 2)
        rdd2 = self.sc.parallelize([("a", 10), ("b", 20), ("c", 30)], 2)

        result = rdd1.join(rdd2).collect()

        self.assertEqual(len(result), 3, "Should have 3 joined results")

    def test_join_multiple_values_per_key(self):
        """
        Test join with multiple values per key.

        If key has multiple values in both RDDs,
        should produce cartesian product.
        """
        rdd1 = self.sc.parallelize([("a", 1), ("a", 2)], 1)
        rdd2 = self.sc.parallelize([("a", 10), ("a", 20), ("a", 30)], 1)

        result = rdd1.join(rdd2).collect()
        a_values = [(k, v) for k, v in result if k == "a"]

        self.assertEqual(
            len(a_values), 6, "Cartesian product: 2 * 3 = 6 values for key 'a'"
        )


class TestJoinWideDependency(unittest.TestCase):
    """Tests for wide dependency with join."""

    def setUp(self):
        self.sc = SparkContext()

    def test_join_creates_shuffle(self):
        """
        Test that join creates shuffle boundary.

        join is a wide transformation that triggers shuffle.
        """
        rdd1 = self.sc.parallelize([("a", 1), ("b", 2)], 2)
        rdd2 = self.sc.parallelize([("a", 10), ("b", 20)], 2)

        joined = rdd1.join(rdd2)

        stages = joined.get_stages()

        self.assertGreaterEqual(
            len(stages),
            2,
            "join() should create shuffle boundary, creating at least 2 stages. "
            f"Got {len(stages)} stages",
        )

    def test_join_triggers_shuffle_dependency(self):
        """
        Test that join has ShuffleDependency.

        Should have shuffle dependency for both input RDDs.
        """
        rdd1 = self.sc.parallelize([("a", 1)], 2)
        rdd2 = self.sc.parallelize([("a", 10)], 2)

        joined = rdd1.join(rdd2)

        has_shuffle_dep = (
            hasattr(joined, "dependencies")
            or hasattr(joined, "shuffle_dep")
            or hasattr(joined, "requires_shuffle")
        )

        self.assertTrue(
            has_shuffle_dep, "join() should require shuffle (wide dependency)"
        )


class TestCogroup(unittest.TestCase):
    """Tests for cogroup() operation."""

    def setUp(self):
        self.sc = SparkContext()

    def test_rdd_has_cogroup_method(self):
        """
        Test that RDD has cogroup() method.

        cogroup() groups values from multiple RDDs by key.
        """
        rdd = self.sc.parallelize([("a", 1)], 2)

        self.assertTrue(
            hasattr(rdd, "cogroup") and callable(rdd.cogroup),
            "RDD should have cogroup() method",
        )

    def test_cogroup_single_rdd(self):
        """
        Test cogroup with single RDD.

        Should group values from that RDD.
        """
        rdd1 = self.sc.parallelize([("a", 1), ("b", 2)], 2)

        result = rdd1.cogroup(rdd1).collect()
        result_dict = dict(result)

        self.assertIn("a", result_dict, "Key 'a' should be in cogroup result")

    def test_cogroup_multiple_rdds(self):
        """
        Test cogroup with multiple RDDs.

        Should group values from ALL RDDs by key.
        """
        rdd1 = self.sc.parallelize([("a", 1), ("b", 2)], 2)
        rdd2 = self.sc.parallelize([("a", 10), ("c", 30)], 2)
        rdd3 = self.sc.parallelize([("a", 100)], 2)

        joined = rdd1.cogroup(rdd2, rdd3)

        self.assertIsInstance(joined, RDD, "cogroup() should return RDD")


class TestOtherJoins(unittest.TestCase):
    """Tests for other join types."""

    def setUp(self):
        self.sc = SparkContext()

    def test_left_outer_join(self):
        """
        Test leftOuterJoin.

        All keys from left RDD should be in result.
        """
        rdd1 = self.sc.parallelize([("a", 1), ("b", 2)], 2)
        rdd2 = self.sc.parallelize([("a", 10)], 2)

        if hasattr(rdd1, "left_outer_join"):
            result = rdd1.left_outer_join(rdd2).collect()
            keys = set(k for k, v in result)

            self.assertIn(
                "b", keys, "leftOuterJoin should include all keys from left RDD"
            )
        else:
            self.fail(
                "left_outer_join() not implemented. "
                "Implement left_outer_join() for left outer join."
            )

    def test_right_outer_join(self):
        """
        Test rightOuterJoin.

        All keys from right RDD should be in result.
        """
        rdd1 = self.sc.parallelize([("a", 1)], 2)
        rdd2 = self.sc.parallelize([("a", 10), ("b", 20)], 2)

        if hasattr(rdd1, "right_outer_join"):
            result = rdd1.right_outer_join(rdd2).collect()
            keys = set(k for k, v in result)

            self.assertIn(
                "b", keys, "rightOuterJoin should include all keys from right RDD"
            )
        else:
            self.fail(
                "right_outer_join() not implemented. "
                "Implement right_outer_join() for right outer join."
            )

    def test_full_outer_join(self):
        """
        Test fullOuterJoin.

        All keys from both RDDs should be in result.
        """
        rdd1 = self.sc.parallelize([("a", 1)], 2)
        rdd2 = self.sc.parallelize([("b", 20)], 2)

        if hasattr(rdd1, "full_outer_join"):
            result = rdd1.full_outer_join(rdd2).collect()
            keys = set(k for k, v in result)

            self.assertEqual(
                keys, {"a", "b"}, "fullOuterJoin should include all keys from both RDDs"
            )
        else:
            self.fail(
                "full_outer_join() not implemented. "
                "Implement full_outer_join() for full outer join."
            )


class TestJoinResultPartitioning(unittest.TestCase):
    """Tests for join result partitioning."""

    def setUp(self):
        self.sc = SparkContext()

    def test_join_result_has_partitions(self):
        """
        Test that join result has correct number of partitions.

        Default is min of two input partition counts.
        """
        rdd1 = self.sc.parallelize([("a", 1)], 4)
        rdd2 = self.sc.parallelize([("a", 10)], 2)

        joined = rdd1.join(rdd2)

        self.assertEqual(
            joined.get_num_partitions(),
            2,
            "join() partition count should be min of inputs",
        )


if __name__ == "__main__":
    unittest.main()
