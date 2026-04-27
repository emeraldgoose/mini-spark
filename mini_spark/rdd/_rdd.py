from enum import Enum
from typing import List, Any, Callable, Optional, Iterator

from mini_spark.execution.task import Task
from mini_spark.rdd.transformations import (
    MapTransformation,
    FilterTransformation,
    FlatMapTransformation,
    GroupByKeyTransformation,
    ReduceByKeyTransformation
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
            transformation: Optional[MapTransformation] = None,
            num_partitions: int = 2,
            lineage: Optional[str] = None,
            context: Optional["SparkContext"] = None
        ):
        self.partitions = partitions
        self.prev = prev
        self.transformation = transformation
        self.num_partitions = num_partitions
        self.lineage = lineage
        self.context = context

        self.is_cached = False
        self.cached_data = {}
        self._storage_level = None

    def map(self, func: Callable[[Any], Any]) -> "RDD":
        return RDD(
            prev=self,
            partitions=self.partitions,
            transformation=MapTransformation(func), 
            num_partitions=self.num_partitions, 
            context=self.context
        )

    def filter(self, func: Callable[[Any], Any]) -> "RDD":
        return RDD(
            prev=self, 
            partitions=self.partitions,
            transformation=FilterTransformation(func), 
            num_partitions=self.num_partitions, 
            context=self.context
        )

    def flatMap(self, func: Callable[[Any], Any]) -> "RDD":
        return RDD(
            prev=self, 
            partitions=self.partitions,
            transformation=FlatMapTransformation(func), 
            num_partitions=self.num_partitions, 
            context=self.context
        )

    def groupByKey(self) -> "RDD":
        return RDD(
            prev=self, 
            partitions=self.partitions,
            transformation=GroupByKeyTransformation(), 
            num_partitions=self.num_partitions, 
            context=self.context
        )

    def reduceByKey(self, func: Callable[[Any, Any], Any]) -> "RDD":
        new_rdd = RDD(
            prev=self, 
            transformation=ReduceByKeyTransformation(func, num_partitions=self.num_partitions), 
            num_partitions=self.num_partitions, 
            context=self.context
        )
    
        parent_partitions = self.context.scheduler.execute(self)
        self.context.scheduler.shuffle_manager.write(parent_partitions)
        
        return new_rdd

    def collect(self):
        return Actions.collect(self)

    def count(self):
        return Actions.count(self)

    def take(self, n: int):
        return Actions.take(self, n)

    def get_num_partitions(self) -> int:
        if self.partitions is not None:
            return len(self.partitions)
        
        elif self.prev is not None:
            return self.prev.get_num_partitions()
        
        else:
            return 0
        
    def get_stages(self) -> List["RDD"]:
        def is_shuffle_boundary(rdd: "RDD") -> bool:
            return rdd.transformation is not None and isinstance(rdd.transformation, WIDE_TRANSFORMATIONS)

        stages = []
        current_stage = []
        current = self

        # transformation의 WIDE_TRANSFORMATIONS는 새로운 Stage의 시작
        while current is not None:
            current_stage.insert(0, current)
            if is_shuffle_boundary(current) or current.prev is None:
                stages.insert(0, current_stage[0])
                current_stage = []
            current = current.prev
        return stages[::-1]  # 역순으로 반환하여 실행 순서대로 정렬
    
    def get_tasks(self) -> List["RDD"]:
        # Task: Worker에서 실행할 최소 실행 단위
        tasks = []
        for partition in self.partitions:
            tasks.append(Task(rdd=self, partition=partition))
        return tasks
    
    def compute(self, split: Partition):
        if self.is_cached and split.partition_id in self.cached_data:
            return iter(self.cached_data[split.partition_id])
        
        if self.prev is None:
            return iter(split.data)
        
        parent_iter = self.prev.compute(split)
        result_iter = self.transformation.apply(parent_iter)

        result_list = list(result_iter)

        if self.is_cached:
            self.cached_data[split.partition_id] = result_list

        return iter(result_list)

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