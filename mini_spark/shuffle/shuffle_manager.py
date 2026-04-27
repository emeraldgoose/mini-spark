import os
from typing import List, Callable, Any
from collections import defaultdict

from mini_spark.shuffle.partitioner import HashPartitioner
from mini_spark.storage.partition import Partition

class ShuffleManager:
    def __init__(self, num_partitions: int = 2, output_dir=None):
        self.num_partitions = num_partitions
        self.partitioner = HashPartitioner(self.num_partitions)
        self.shuffle_files = {}  # key: shuffle_id, value: list of partitions
        
        self.output_dir = output_dir or "./shuffle_data"
        os.makedirs(self.output_dir, exist_ok=True)

        self.shuffle_id = 0

    def get_shuffle_path(self, partition_id: int):
        filename = f"shuffle_{self.shuffle_id}_part_{partition_id}.data"
        return os.path.join(self.output_dir, filename)

    def read(self, shuffle_id=None, partition_id=None) -> List[Any]:
        if shuffle_id is None:
            shuffle_id = self.shuffle_id - 1 # 가장 최근 shuffle_id 사용

        shuffle_data = self.shuffle_files.get(shuffle_id, {})

        if partition_id is None:
            return [shuffle_data.get(pid, []) for pid in sorted(shuffle_data)]
        
        return shuffle_data.get(partition_id, [])

    def write(self, partitions: List[Partition]):
        buckets = {i: [] for i in range(self.num_partitions)}

        for part in partitions:
            for k, v in part.data:
                pid = self.partitioner.get_partition(k)
                buckets[pid].append((k, v))

        self.shuffle_files[self.shuffle_id] = buckets # Save shuffled data to "disk" (in-memory dict for simulation)
        self.shuffle_id += 1
        
    def get_target_partition(self, key: str) -> int:
        return self.partitioner.get_partition(key)

    def shuffle_groupByKey(self, parent_partitions: List["Partition"]):
        # pid별 버킷 초기화
        buckets = {i: [] for i in range(self.num_partitions)}
        
        # key-value 분배
        for part in parent_partitions:
            for k, v in part.data:
                pid = self.get_target_partition(k)
                buckets[pid].append((k, v))
        
        # bucket별 key grouping
        result = []
        for bucket in buckets:
            if len(buckets[bucket]) == 0:
                continue
            
            group_dict = defaultdict(list)
            
            for k, v in buckets[bucket]:
                group_dict[k].append(v)
            
            if len(group_dict) > 0:
                result.append(list(group_dict.items()))
        
        return result
    