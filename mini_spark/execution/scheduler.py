from collections import defaultdict
from typing import Callable, List

from mini_spark.storage.partition import Partition
from mini_spark.shuffle.partitioner import HashPartitioner

class Scheduler:
    def __init__(self, num_partitions: int = 2):
        self.num_partitions = num_partitions
        self.partitioner = HashPartitioner(self.num_partitions)
    
    def run(self, rdd: "RDD"):
        results = self._execute(rdd)
        return [x for part in results for x in part.data]
    
    def _execute(self, rdd: "RDD"):
        if rdd.partitions is not None:
            return rdd.partitions
        
        parent = self._execute(rdd.prev)
        op, func = rdd.op

        if op == "map":
            return [Partition([func(x) for x in part.data]) for part in parent]
    
        elif op == "filter":
            return [Partition([x for x in part.data if func(x)]) for part in parent]
    
        elif op == "flatMap":
            return [Partition([y for x in part.data for y in func(x)]) for part in parent]
        
        elif op == "groupByKey":
            return self.__shuffle_group_by_key(parent)

        elif op == "reduceByKey":
            return self.__shuffle_reduce_by_key(parent, func)

        else:
            raise Exception("Unknown operation")
    
    def __shuffle_group_by_key(self, partitions: List[Partition]) -> List[Partition]:
        shuffle_dict = defaultdict(list)

        for part in partitions:
            for k, v in part.data:
                shuffle_dict[k].append(v)

        return [Partition([(k, v) for k, v in shuffle_dict.items()])]
    
    def __shuffle_reduce_by_key(self, partitions: List[Partition], func: Callable) -> List[Partition]:
        buckets = [[] for _ in range(self.num_partitions)]

        for part in partitions:
            for k, v in part.data:
                pid = self.__get_partition_id(k)
                buckets[pid].append((k, v))

        result_partitions = []
        for bucket in buckets:
            if not bucket:
                continue
            
            grouped = defaultdict(list)
            for k, v in bucket:
                grouped[k].append(v)
            
            reduced = [(k, self.__reduce_values(v_list, func)) for k, v_list in grouped.items()]
            result_partitions.append(Partition(reduced))

        return result_partitions
    
    def __reduce_values(self, values: List, func: Callable):
        result = values[0]
        for v in values[1:]:
            result = func(result, v)
        
        return result
    
    def __get_partition_id(self, key):
        return self.partitioner.get_partition(key)