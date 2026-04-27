from collections import defaultdict
from typing import Callable, List, Any, Iterator

from mini_spark.storage.partition import Partition
from mini_spark.shuffle.partitioner import HashPartitioner

class MapTransformation:
    def __init__(self, func: Callable[[Any], Any]):
        self.func = func

    def apply(self, iterator: Iterator[Any]) -> Iterator[Any]:
        return (self.func(x) for x in iterator)

class FilterTransformation:
    def __init__(self, func: Callable[[Any], Any]):
        self.func = func

    def apply(self, iterator: Iterator[Any]) -> Iterator[Any]:
        return (x for x in iterator if self.func(x))

class FlatMapTransformation:
    def __init__(self, func: Callable[[Any], Any]):
        self.func = func

    def apply(self, iterator: Iterator[Any]) -> Iterator[Any]:
        for x in iterator:
            for y in self.func(x):
                yield y

class GroupByKeyTransformation:
    def __init__(self):
        self.func = None

    def apply(self, partitions: List[Partition]):
        shuffle_dict = defaultdict(list)

        for part in partitions:
            for k, v in part.data:
                shuffle_dict[k].append(v)

        return [Partition([(k, v) for k, v in shuffle_dict.items()])]
    
class ReduceByKeyTransformation:
    def __init__(self, func: Callable[[Any, Any], Any], num_partitions: int = 2):
        self.func = func
        self.num_partitions = num_partitions
        self.partitioner = HashPartitioner(self.num_partitions)

    def apply(self, partitions: List[Partition]) -> List[Partition]:
        buckets = [[] for _ in range(self.num_partitions)]

        for part in partitions:
            for k, v in part.data:
                pid = self.partitioner.get_partition(k)
                buckets[pid].append((k, v))

        result_partitions = []
        for bucket in buckets:
            if not bucket:
                continue
            
            grouped = defaultdict(list)
            for k, v in bucket:
                grouped[k].append(v)
            
            reduced = [(k, self.__reduce_values(v_list, self.func)) for k, v_list in grouped.items()]
            result_partitions.append(Partition(reduced))

        return result_partitions
    
    def __reduce_values(self, values: List[Any], func: Callable[[Any, Any], Any]):
        result = values[0]
        for v in values[1:]:
            result = func(result, v)
        
        return result