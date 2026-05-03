from enum import Enum
from typing import List, Any, Callable, Optional, Iterator

from mini_spark.execution.task import Task
from mini_spark.rdd.transformations import (
    MapTransformation,
    FilterTransformation,
    FlatMapTransformation,
    JoinTransformation,
    GroupByKeyTransformation,
    ReduceByKeyTransformation,
    CogroupTransformation
)
from mini_spark.rdd.actions import Actions
from mini_spark.storage.partition import Partition

WIDE_TRANSFORMATIONS = (
    GroupByKeyTransformation, 
    ReduceByKeyTransformation
)
NARROW_TRANSFORMATIONS = (
    MapTransformation, 
    FilterTransformation, 
    FlatMapTransformation
)


class StorageLevel(Enum):
    MEMORY_ONLY = 1
    MEMORY_AND_DISK = 2
    DISK_ONLY = 3


class RDD:
    def __init__(
            self, 
            partitions: Optional[List[Partition]] = None,
            prev: Optional["RDD"] = None,
            parents: Optional[List["RDD"]] = None,
            transformation: Optional[MapTransformation] = None,
            num_partitions: int = 2,
            lineage: Optional[str] = None,
            context: Optional["SparkContext"] = None, 
        ):
        self.prev = prev # 초기 테스트 코드를 통과하기 위해 남겨둔 필드 (현재는 parents[0]로 대체)
        self.partitions = partitions
        self.transformation = transformation
        self.num_partitions = num_partitions
        self.lineage = lineage
        self.context = context
        self.parents = parents or []

        self.is_cached = False
        self.cached_data = {}
        self._storage_level = None
        
        self._is_checkpointed = False
        self.requires_shuffle = False

    def map(self, func: Callable[[Any], Any]) -> "RDD":
        return RDD(
            prev=self,
            parents=[self],
            partitions=self.partitions,
            transformation=MapTransformation(func), 
            num_partitions=self.num_partitions, 
            context=self.context
        )

    def filter(self, func: Callable[[Any], Any]) -> "RDD":
        return RDD(
            prev=self,
            parents=[self],
            partitions=self.partitions,
            transformation=FilterTransformation(func), 
            num_partitions=self.num_partitions, 
            context=self.context
        )

    def flatMap(self, func: Callable[[Any], Any]) -> "RDD":
        return RDD(
            prev=self,
            parents=[self],
            partitions=self.partitions,
            transformation=FlatMapTransformation(func), 
            num_partitions=self.num_partitions, 
            context=self.context
        )

    def groupByKey(self) -> "RDD":
        return RDD(
            prev=self,
            parents=[self],
            partitions=self.partitions,
            transformation=GroupByKeyTransformation(), 
            num_partitions=self.num_partitions, 
            context=self.context
        )

    def reduceByKey(self, func: Callable[[Any, Any], Any]) -> "RDD":
        new_rdd = RDD(
            prev=self,
            parents=[self],
            transformation=ReduceByKeyTransformation(func, num_partitions=self.num_partitions), 
            num_partitions=self.num_partitions, 
            context=self.context
        )
    
        parent_partitions = self.context.scheduler.execute(self)
        self.context.scheduler.shuffle_manager.write(parent_partitions)
        
        return new_rdd

    def join(self, other: "RDD") -> "RDD":
        new_rdd = RDD(
            prev=self,
            parents=[self, other],
            transformation=JoinTransformation(
                other=other,
                num_partitions=self.num_partitions
            ),
            num_partitions=min(self.num_partitions, other.num_partitions),
            context=self.context
        )

        parent_partitions = self.context.scheduler.execute(self)
        self.context.scheduler.shuffle_manager.write(parent_partitions)
        self.requires_shuffle = True

        return new_rdd

    def left_outer_join(self, other: "RDD") -> "RDD":
        new_rdd = RDD(
            prev=self,
            parents=[self, other],
            transformation=JoinTransformation(
                other=other,
                num_partitions=self.num_partitions,
                join_type="left_outer"
            ),
            num_partitions=min(self.num_partitions, other.num_partitions),
            context=self.context
        )

        parent_partitions = self.context.scheduler.execute(self)
        self.context.scheduler.shuffle_manager.write(parent_partitions)
        self.requires_shuffle = True

        return new_rdd
    
    def right_outer_join(self, other: "RDD") -> "RDD":
        new_rdd = RDD(
            prev=self,
            parents=[self, other],
            transformation=JoinTransformation(
                other=other,
                num_partitions=self.num_partitions,
                join_type="right_outer"
            ),
            num_partitions=min(self.num_partitions, other.num_partitions),
            context=self.context
        )

        parent_partitions = self.context.scheduler.execute(self)
        self.context.scheduler.shuffle_manager.write(parent_partitions)
        self.requires_shuffle = True

        return new_rdd
    
    def full_outer_join(self, other: "RDD") -> "RDD":
        new_rdd = RDD(
            prev=self,
            parents=[self, other],
            transformation=JoinTransformation(
                other=other,
                num_partitions=self.num_partitions,
                join_type="full_outer"
            ),
            num_partitions=min(self.num_partitions, other.num_partitions),
            context=self.context
        )

        parent_partitions = self.context.scheduler.execute(self)
        self.context.scheduler.shuffle_manager.write(parent_partitions)
        self.requires_shuffle = True

        return new_rdd

    def cogroup(self, *others) -> "RDD":
        return RDD(
            prev=self,
            parents=[self, *others],
            transformation=CogroupTransformation(len(others)+1),
            context=self.context,
            num_partitions=min(self.num_partitions, *(o.num_partitions for o in others))
        )

    def collect(self):
        return Actions.collect(self)

    def count(self):
        return Actions.count(self)

    def take(self, n: int):
        return Actions.take(self, n)

    def get_num_partitions(self) -> int:
        return self.num_partitions
        
    def get_stages(self) -> List["RDD"]:
        stages = []

        def visit(rdd):
            for p in rdd.parents:
                visit(p)

            if (
                rdd.transformation is None
                or isinstance(rdd.transformation, WIDE_TRANSFORMATIONS)
            ):
                stages.append(rdd)
        
        visit(self)
        return stages
    
    def get_tasks(self) -> List["RDD"]:
        # Task: Worker에서 실행할 최소 실행 단위
        tasks = []
        for partition in self.partitions:
            tasks.append(Task(rdd=self, partition=partition))
        return tasks
    
    def compute(self, split: Partition):
        if self.is_cached and split.partition_id in self.cached_data:
            return iter(self.cached_data[split.partition_id])
        
        if not self.parents:
            return iter(split.data)
        
        parent_iter = self.parents[0].compute(split)
        result_iter = self.transformation.apply(parent_iter)

        result_list = list(result_iter)

        if self.is_cached:
            self.cached_data[split.partition_id] = result_list

        return iter(result_list)
    
    def recompute(self, partition_id: int):
        part = self.partitions[partition_id]
        return list(self.compute(part))

    def cache(self) -> "RDD":
        self.is_cached = True
        self.cached_data = {}  # partition_id → data
        return self.persist(StorageLevel.MEMORY_ONLY)
    
    def persist(self, storage_level: StorageLevel = StorageLevel.MEMORY_ONLY) -> "RDD":
        self.is_cached = True
        self._storage_level = storage_level
        self.cached_data = {}
        return self
    
    def unpersist(self) -> "RDD":
        # RDD의 캐시된 데이터를 제거하여 메모리를 해제
        self.is_cached = False
        self._storage_level = None
        self.cached_data = {}
        return self

    def checkpoint(self):
        data = self.collect()

        self.partitions = [Partition(list(data))] # repartition_data()
        self.transformation = None
        self.lineage = None
        self._is_checkpointed = True
        return self
