from typing import List, Callable
from collections import defaultdict

from mini_spark.shuffle.partitioner import HashPartitioner
from mini_spark.storage.partition import Partition

class ShuffleManager:
    def __init__(self, num_partitions: int = 2):
        self.num_partitions = num_partitions
        self.buckets = {} # key: partition_id, value: list of items
        self.partitioner = HashPartitioner(self.num_partitions)

    def shuffle(self, parent_partitions: List["Partition"]):
        for pid, part in enumerate(parent_partitions):
            for k, v in part.data:
                target_pid = self.get_target_partition(k)
                if target_pid not in self.buckets:
                    self.buckets[target_pid] = []
                self.buckets[target_pid].append((k, v))
        return [Partition(items) for items in self.buckets.values()]

    def get_target_partition(self, key: str) -> int:
        return self.partitioner.get_partition(key)

    def shuffle_groupByKey(self, parent_partitions: List["Partition"]):
        # pid별 버킷 초기화
        buckets = [[] for _ in range(self.num_partitions)]
        
        # key-value 분배
        for part in parent_partitions:
            for k, v in part.data:
                pid = self.get_target_partition(k)
                buckets[pid].append((k, v))
        
        # bucket별 key grouping
        result = []
        for bucket in buckets:
            if len(bucket) == 0:
                continue
            
            group_dict = defaultdict(list)
            
            for k, v in bucket:
                group_dict[k].append(v)
            
            if len(group_dict) > 0:
                result.append(list(group_dict.items()))
        
        return result
    
    def shuffle_reduceByKey(self, parent_partitions: List["Partition"], func: Callable):
        buckets = [[] for _ in range(self.num_partitions)]
        for part in parent_partitions:
            for k, v in part.data:
                pid = self.get_target_partition(k)
                buckets[pid].append((k, v))
        
        result = []
        for bucket in buckets:
            reduce_dict = defaultdict(list)
            for k, v in bucket:
                reduce_dict[k].append(v)
            
            if len(reduce_dict) > 0:
                result.append([(k, func(*v)) for k, v in reduce_dict.items()])
        
        return result