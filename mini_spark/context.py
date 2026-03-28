from typing import List, Any

from mini_spark.rdd import RDD
from mini_spark.storage.partition import Partition

class SparkContext:
    def __init__(self):
        self.rdd = None
    
    def parallelize(self, data: List[Any], num_partitions: int = 1) -> "RDD":
        chunk_size = len(data) // num_partitions

        partitions = []
        for i in range(num_partitions):
            start = i * chunk_size
            end = (i + 1) * chunk_size if i < num_partitions - 1 else len(data)
            partitions.append(Partition(data[start:end]))
        
        return RDD(partitions=partitions, prev=None)

    def textFile(self, filepath: str):
        ...