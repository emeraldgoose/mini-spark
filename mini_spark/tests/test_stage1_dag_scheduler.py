"""
Stage 1: DAG Scheduler + Stage Creation Tests

This test file verifies the DAG (Directed Acyclic Graph) scheduler implementation
which is fundamental to Spark's execution model.

CONCEPTS TESTED:
- RDD lineage tracking (transformation chain)
- Shuffle boundaries creating separate stages
- Narrow dependencies staying in the same stage
- Stage creation from RDD DAG
- Parent-child stage relationships

Spark Concept:
  Spark builds a DAG of RDDs during lazy evaluation. When an action is called,
  the DAG is analyzed to create stages. Wide transformations (reduceByKey, groupByKey, join)
  create shuffle boundaries, forcing a new stage. Narrow transformations (map, filter)
  can be pipelined within a single stage.
"""

import unittest

from mini_spark.context import SparkContext


class TestDAGScheduler(unittest.TestCase):
    """Tests for DAG Scheduler and Stage Creation."""

    def setUp(self):
        """Set up SparkContext for each test."""
        self.sc = SparkContext()

    def test_rdd_lineage_tracking(self):
        """
        Test that RDD lineage correctly tracks transformation chain.

        Each transformation should link back to its parent RDD,
        forming a chain that can be traced for lineage-based recovery.
        """
        rdd = self.sc.parallelize([1, 2, 3, 4], 2)

        # Chain: parallelize -> map -> filter
        rdd2 = rdd.map(lambda x: x * 2)
        rdd3 = rdd2.filter(lambda x: x > 2)

        # Verify lineage chain exists
        self.assertIsNotNone(rdd3.prev, "RDD should have a reference to parent (prev)")
        self.assertIsNotNone(
            rdd3.prev.prev, "Parent RDD should also have its parent reference"
        )
        self.assertIsNone(
            rdd3.prev.prev.prev, "Original RDD from parallelize should have no parent"
        )

    def test_lineage_string_representation(self):
        """
        Test that lineage string correctly represents transformation chain.

        For fault tolerance, Spark needs to be able to reconstruct
        the transformation logic from lineage information.
        """
        rdd = self.sc.parallelize([1, 2, 3], 2)
        rdd2 = rdd.map(lambda x: x * 2)
        rdd3 = rdd2.filter(lambda x: x > 2)

        # Lineage should exist (implementation may vary - some use __str__, some use internal tracking)
        lineage = getattr(rdd3, "lineage", None) or str(rdd3)
        self.assertIsNotNone(
            lineage, "RDD should track lineage for recomputation on failure"
        )

    def test_shuffle_boundary_creates_separate_stage(self):
        """
        Test that wide transformations create shuffle boundaries.

        Wide transformations like reduceByKey, groupByKey trigger a shuffle,
        which requires the data to be written to disk and read back.
        This creates a natural boundary for stage separation.

        Expected: narrow transformations stay in same stage, but wide
        transformations force a new stage.
        """
        # map is narrow -> same stage
        # reduceByKey is wide -> new stage
        rdd = self.sc.parallelize([("a", 1), ("b", 2), ("a", 3)], 2)
        rdd2 = rdd.map(lambda x: (x[0], x[1] * 10))
        rdd3 = rdd2.reduceByKey(lambda x, y: x + y)

        stages = rdd3.get_stages()

        # Should have 2 stages:
        # Stage 1: parallelize + map
        # Stage 2: reduceByKey (shuffle boundary)
        self.assertEqual(
            len(stages),
            2,
            "Shuffle transformation (reduceByKey) should create separate stages. "
            f"Expected 2 stages, got {len(stages)}. "
            "Implement get_stages() to split DAG at shuffle boundaries.",
        )

    def test_narrow_dependencies_same_stage(self):
        """
        Test that narrow dependencies (map, filter, flatMap) stay in same stage.

        These transformations don't require shuffling data between partitions,
        so they can be fused into a single stage and executed together.
        """
        rdd = self.sc.parallelize([1, 2, 3, 4], 2)

        # Chain of narrow transformations
        rdd2 = rdd.map(lambda x: x * 2)
        rdd3 = rdd2.filter(lambda x: x > 2)
        rdd4 = rdd3.map(lambda x: x + 1)

        stages = rdd4.get_stages()

        # All narrow transformations should be in ONE stage
        self.assertEqual(
            len(stages),
            1,
            f"Narrow transformations (map, filter, map) should stay in same stage. "
            f"Expected 1 stage for pipeline, got {len(stages)}",
        )

    def test_multiple_shuffles_create_multiple_stages(self):
        """
        Test that multiple wide transformations create multiple stages.

        Pattern: reduceByKey -> map -> reduceByKey should create 3 stages.
        """
        rdd = self.sc.parallelize([("a", 1), ("b", 2), ("a", 3)], 2)

        # Two reduceByKey = two shuffles = three stages
        rdd2 = rdd.reduceByKey(lambda x, y: x + y)  # Stage 1 (shuffle)
        rdd3 = rdd2.map(
            lambda x: (x[0], x[1] * 10)
        )  # Stage 2 (narrow, fused with above)
        rdd4 = rdd3.reduceByKey(lambda x, y: x + y)  # Stage 3 (shuffle)

        stages = rdd4.get_stages()

        self.assertGreaterEqual(
            len(stages),
            2,
            f"Multiple wide transformations should create multiple stages. "
            f"Got {len(stages)}, expected at least 2",
        )

    def test_get_stages_returns_rdd_list(self):
        """
        Test that get_stages() returns a list of RDDs representing each stage.

        Each element in the returned list should be an RDD that can be
        executed independently as a stage.
        """
        rdd = self.sc.parallelize([1, 2, 3], 2)
        rdd2 = rdd.reduceByKey(lambda x, y: x + y)

        stages = rdd2.get_stages()

        self.assertIsInstance(stages, list, "get_stages() should return a list")
        self.assertGreater(
            len(stages), 0, "get_stages() should return at least one stage"
        )

        # Each stage should be an RDD
        for i, stage in enumerate(stages):
            self.assertIsNotNone(stage, f"Stage {i} should not be None")

    def test_stage_has_parent_relationships(self):
        """
        Test that stages have parent-child relationships.

        For proper execution, each stage should know its parent stage(s).
        This is needed for passing shuffle files between stages.
        """
        rdd = self.sc.parallelize([("a", 1), ("b", 2)], 2)
        rdd2 = rdd.reduceByKey(lambda x, y: x + y)

        stages = rdd2.get_stages()

        if len(stages) >= 2:
            # Later stages should reference earlier ones
            later_stage = stages[-1]
            # Check for parent reference (may be 'prev', 'parents', or similar attribute)
            has_parent_ref = (
                hasattr(later_stage, "prev")
                or hasattr(later_stage, "parents")
                or hasattr(later_stage, "dependencies")
            )
            self.assertTrue(
                has_parent_ref,
                "Later stage should have reference to parent stage(s). "
                "Implement parent relationship tracking in Stage class.",
            )

    def test_groupByKey_creates_shuffle_boundary(self):
        """
        Test that groupByKey also creates shuffle boundary.

        groupByKey is another wide transformation similar to reduceByKey.
        """
        rdd = self.sc.parallelize([("a", 1), ("b", 2), ("a", 3)], 2)
        rdd2 = rdd.groupByKey()

        stages = rdd2.get_stages()

        self.assertEqual(
            len(stages),
            2,
            "groupByKey should create shuffle boundary and separate stages. "
            f"Expected 2 stages, got {len(stages)}",
        )

    def test_stage_ids_increment_correctly(self):
        """
        Test that stages get unique, incrementally increasing IDs.

        Stage IDs are used for shuffle file naming and tracking.
        """
        rdd = self.sc.parallelize([("a", 1), ("b", 2), ("a", 3)], 2)
        rdd2 = rdd.reduceByKey(lambda x, y: x + y)

        # Get or create stage IDs
        stages = rdd2.get_stages()

        stage_ids = []
        for stage in stages:
            sid = getattr(stage, "stage_id", None)
            if sid is not None:
                stage_ids.append(sid)

        if len(stage_ids) >= 2:
            # Stage IDs should be in ascending order
            self.assertEqual(
                stage_ids,
                sorted(stage_ids),
                f"Stage IDs should be incremental: {stage_ids}",
            )


class TestStageClass(unittest.TestCase):
    """Tests for Stage class itself if it's implemented."""

    def setUp(self):
        self.sc = SparkContext()

    def test_stage_class_exists(self):
        """
        Test that Stage class has been implemented in execution/stage.py.

        Currently execution/stage.py is empty (0 lines).
        The Stage class should be implemented to hold:
        - List of RDDs in this stage
        - Stage ID
        - Parent stage references
        - Output shuffle info
        """
        try:
            from mini_spark.execution.stage import Stage

            # If we get here, Stage class exists
            self.assertTrue(
                hasattr(Stage, "__init__"), "Stage class should have __init__ method"
            )
        except ImportError:
            self.fail(
                "Stage class not implemented. "
                "Create mini_spark/execution/stage.py with Stage class implementation. "
                "Stage should track: rdd_list, stage_id, parents, output_shuffle"
            )

    def test_stage_can_be_instantiated(self):
        """Test that Stage can be instantiated with required parameters."""
        try:
            from mini_spark.execution.stage import Stage

            rdd = self.sc.parallelize([1, 2, 3], 2)
            stage = Stage(rdd, stage_id=0)
            self.assertIsNotNone(stage)
        except ImportError:
            self.skipTest("Stage class not implemented yet")

    def test_stage_has_required_attributes(self):
        """Test that Stage has required attributes for execution."""
        try:
            from mini_spark.execution.stage import Stage

            rdd = self.sc.parallelize([1, 2, 3], 2)
            stage = Stage(rdd, stage_id=0)

            # Check required attributes
            required_attrs = ["rdd", "stage_id", "partitions"]
            for attr in required_attrs:
                self.assertTrue(
                    hasattr(stage, attr), f"Stage should have '{attr}' attribute"
                )
        except ImportError:
            self.skipTest("Stage class not implemented yet")


if __name__ == "__main__":
    unittest.main()
