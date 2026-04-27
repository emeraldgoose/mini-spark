from typing import List, Any

from mini_spark.rdd._rdd import RDD
from mini_spark.execution.scheduler import Scheduler
from mini_spark.storage.partition import Partition

class SparkContext:
    def __init__(self):
        self.rdd = None
        self.scheduler = Scheduler()
    
    def parallelize(self, data: List[Any], num_partitions: int = 1) -> "RDD":
        chunk_size = len(data) // num_partitions

        partitions = []
        for i in range(num_partitions):
            start = i * chunk_size
            end = (i + 1) * chunk_size if i < num_partitions - 1 else len(data)
            partitions.append(Partition(data[start:end], partition_id=i))
        
        return RDD(partitions=partitions, num_partitions=num_partitions, context=self)

    def textFile(self, filepath: str):
        ...