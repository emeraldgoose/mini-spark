"""
Stage 4: persist/cache Tests

This test file verifies caching functionality to store RDD data in memory
or disk for reuse across multiple actions.

CONCEPTS TESTED:
- rdd.cache() stores data in memory
- Cached RDD doesn't recompute on second action
- rdd.persist(storage_level) with different levels
- rdd.unpersist() clears cache
- Cache works across multiple actions

Spark Concept:
  Caching (persist) stores computed partitions in memory/disk.
  Useful when:
  1. Data is reused (multiple actions)
  2. Iterative algorithms (multiple passes)

  storageLevel options:
  - MEMORY_ONLY (default for cache)
  - MEMORY_AND_DISK
  - DISK_ONLY
  - etc.
"""

import unittest

from mini_spark.context import SparkContext
from mini_spark.rdd._rdd import RDD


class TestCacheBasic(unittest.TestCase):
    """Basic tests for cache() functionality."""

    def setUp(self):
        self.sc = SparkContext()

    def test_rdd_has_cache_method(self):
        """
        Test that RDD has cache() method.

        cache() is a transformation that marks RDD for caching.
        """
        rdd = self.sc.parallelize([1, 2, 3], 2)

        self.assertTrue(
            hasattr(rdd, "cache") and callable(rdd.cache),
            "RDD should have cache() method",
        )

    def test_cache_returns_rdd(self):
        """
        Test that cache() returns an RDD (for chaining).

        cache() should return the same RDD or a cached version.
        """
        rdd = self.sc.parallelize([1, 2, 3], 2)
        cached_rdd = rdd.cache()

        self.assertIsInstance(cached_rdd, RDD, "cache() should return an RDD")

    def test_cache_stores_data_in_memory(self):
        """
        Test that cache() stores computed data in memory.

        After action, cached data should be accessible.
        """
        rdd = self.sc.parallelize([1, 2, 3, 4], 2)
        rdd2 = rdd.map(lambda x: x * 2)

        cached_rdd = rdd2.cache()

        first_result = cached_rdd.collect()

        has_storage = (
            hasattr(rdd2, "_storage")
            or hasattr(rdd2, "_cached")
            or hasattr(rdd2, "partitions")
            and rdd2.partitions is not None
        )

        self.assertTrue(
            has_storage, "RDD should store computed data after cache().collect()"
        )

    def test_cached_rdd_no_recompute(self):
        """
        Test that cached RDD doesn't recompute on second action.

        Key insight: first action computes and caches, second uses cache.
        """
        computation_count = [0]

        def counting_map(x):
            computation_count[0] += 1
            return x * 2

        rdd = self.sc.parallelize([1, 2, 3, 4], 2)
        rdd2 = rdd.map(counting_map)

        cached_rdd = rdd2.cache()

        cached_rdd.collect()
        first_count = computation_count[0]

        cached_rdd.collect()
        second_count = computation_count[0]

        self.assertEqual(
            first_count,
            second_count,
            f"Second action should use cache. First: {first_count}, Second: {second_count}. "
            "Cached RDD should not recompute on subsequent actions.",
        )


class TestPersistLevels(unittest.TestCase):
    """Tests for persist() with different storage levels."""

    def setUp(self):
        self.sc = SparkContext()

    def test_rdd_has_persist_method(self):
        """
        Test that RDD has persist() method.

        persist() allows specifying storage level.
        """
        rdd = self.sc.parallelize([1, 2, 3], 2)

        self.assertTrue(
            hasattr(rdd, "persist") and callable(rdd.persist),
            "RDD should have persist() method",
        )

    def test_persist_with_storage_level(self):
        """
        Test that persist() accepts storage level parameter.

        Common levels: MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY
        """
        rdd = self.sc.parallelize([1, 2, 3], 2)

        try:
            from mini_spark.rdd._rdd import StorageLevel

            cached = rdd.persist(StorageLevel.MEMORY_ONLY)

            self.assertIsNotNone(cached)
        except ImportError:
            self.fail(
                "StorageLevel not defined. Implement in RDD module. "
                "Should define: MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY"
            )

    def test_storage_level_enum_exists(self):
        """
        Test that StorageLevel is defined.

        Should have constants like:
        - MEMORY_ONLY
        - MEMORY_AND_DISK
        - DISK_ONLY
        - etc.
        """
        try:
            from mini_spark.rdd._rdd import StorageLevel

            self.assertTrue(
                hasattr(StorageLevel, "MEMORY_ONLY")
                or hasattr(StorageLevel, "MEMORY_AND_DISK")
                or hasattr(StorageLevel, "DISK_ONLY"),
                "StorageLevel should have level constants",
            )
        except ImportError:
            self.fail(
                "StorageLevel not implemented. Create StorageLevel enum/class. "
                "Define MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY, etc."
            )


class TestUnpersist(unittest.TestCase):
    """Tests for unpersist() to clear cache."""

    def setUp(self):
        self.sc = SparkContext()

    def test_rdd_has_unpersist_method(self):
        """
        Test that RDD has unpersist() method.

        unpersist() removes cached data.
        """
        rdd = self.sc.parallelize([1, 2, 3], 2)

        self.assertTrue(
            hasattr(rdd, "unpersist") and callable(rdd.unpersist),
            "RDD should have unpersist() method",
        )

    def test_unpersist_clears_cache(self):
        """
        Test that unpersist() clears cached data.

        After unpersist, RDD should recompute.
        """
        rdd = self.sc.parallelize([1, 2, 3, 4], 2)
        rdd2 = rdd.map(lambda x: x * 2)

        cached = rdd2.cache()
        cached.collect()

        cached.unpersist()

        has_storage = (
            hasattr(rdd2, "_storage")
            and rdd2._storage
            or hasattr(rdd2, "_cached")
            and rdd2._cached
        )

        self.assertFalse(
            has_storage, "After unpersist(), cached data should be cleared"
        )


class TestCacheMultipleActions(unittest.TestCase):
    """Tests for cache working across multiple actions."""

    def setUp(self):
        self.sc = SparkContext()

    def test_cache_used_by_count(self):
        """
        Test that cache() works with count() action.

        count() should use cached data if available.
        """
        rdd = self.sc.parallelize([1, 2, 3, 4, 5], 2)
        rdd2 = rdd.map(lambda x: x * 2)

        cached = rdd2.cache()
        cached.collect()

        count_result = cached.count()

        self.assertEqual(count_result, 5, "count() should work on cached RDD")

    def test_cache_used_by_take(self):
        """
        Test that cache() works with take() action.

        take() should use cached data if available.
        """
        rdd = self.sc.parallelize([1, 2, 3, 4, 5], 2)
        rdd2 = rdd.map(lambda x: x * 2)

        cached = rdd2.cache()
        cached.collect()

        take_result = cached.take(3)

        self.assertEqual(
            len(take_result), 3, "take() should return requested number of elements"
        )

    def test_multiple_collects_use_cache(self):
        """
        Test that multiple collect() calls use cache.

        First collect triggers computation and caching.
        Subsequent collects should use cached data.
        """
        compute_count = [0]

        def track_compute(x):
            compute_count[0] += 1
            return x

        rdd = self.sc.parallelize([1, 2, 3], 2)
        rdd2 = rdd.map(track_compute)

        cached = rdd2.cache()

        cached.collect()
        after_first = compute_count[0]

        cached.collect()
        after_second = compute_count[0]

        cached.collect()
        after_third = compute_count[0]

        self.assertEqual(after_first, after_second, "Second collect should use cache")
        self.assertEqual(after_second, after_third, "Third collect should use cache")


class TestCacheWithWideTransformations(unittest.TestCase):
    """Tests for cache with shuffle operations."""

    def setUp(self):
        self.sc = SparkContext()

    def test_cache_after_reduceByKey(self):
        """
        Test that cache works after wide transformation.

        reduceByKey requires shuffle - cache should still work.
        """
        rdd = self.sc.parallelize([("a", 1), ("b", 2), ("a", 3)], 2)
        rdd2 = rdd.reduceByKey(lambda x, y: x + y)

        cached = rdd2.cache()
        
        result1 = cached.collect()
        result2 = cached.collect()

        self.assertEqual(result1, result2, "Cached reduceByKey results should match")

    def test_persist_after_groupByKey(self):
        """
        Test that persist works after groupByKey.

        groupByKey also causes shuffle - should work with cache.
        """
        rdd = self.sc.parallelize([("a", 1), ("b", 2), ("a", 3)], 2)
        rdd2 = rdd.groupByKey()

        persisted = rdd2.persist()

        result1 = persisted.collect()

        self.assertIsNotNone(result1, "persist() after groupByKey should work")


class TestCachePersistence(unittest.TestCase):
    """Tests for cache persistence behavior."""

    def setUp(self):
        self.sc = SparkContext()

    def test_cache_survives_across_rdd_operations(self):
        """
        Test that cached data survives across RDD operations.

        Cached partitions should persist for dependent RDDs.
        """
        rdd = self.sc.parallelize([1, 2, 3, 4], 2)
        rdd2 = rdd.map(lambda x: x * 2)
        rdd3 = rdd2.filter(lambda x: x > 2)

        result1 = rdd3.collect()
        rdd2.cache()

        result2 = rdd3.collect()
        
        self.assertEqual(result1, result2)

    def test_is_cached_flag(self):
        """
        Test that RDD tracks whether it's cached.

        Should have _persist_level or similar attribute.
        """
        rdd = self.sc.parallelize([1, 2, 3], 2)

        if hasattr(rdd, "cache"):
            cached_rdd = rdd.cache()

            has_persist_level = (
                hasattr(cached_rdd, "_persist_level")
                or hasattr(cached_rdd, "persist_level")
                or hasattr(cached_rdd, "_storage_level")
            )
            
            self.assertTrue(
                has_persist_level, "RDD should track storage level of cached data"
            )


if __name__ == "__main__":
    unittest.main()
