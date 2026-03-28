from typing import List, Any, Union, Callable, Optional

from mini_spark.execution.scheduler import Scheduler
from mini_spark.storage.partition import Partition


class RDD:
    def __init__(
            self, 
            partitions: Optional[List[Partition]] = None,
            prev: Optional["RDD"] = None,
            op: Optional[Union[str, Callable[[Any], Any]]] = None,
            lineage: Optional[str] = None
        ):
        self.partitions = partitions
        self.prev = prev
        self.op = op
    
    def map(self, func: Callable[[Any], Any]):
        return RDD(prev=self, op=("map", func))

    def filter(self, func: Callable[[Any], Any]):
        return RDD(prev=self, op=("filter", func))
    
    def flatMap(self, func: Callable[[Any], Any]):
        return RDD(prev=self, op=("flatMap", func))
    
    def groupByKey(self):
        return RDD(prev=self, op=("groupByKey", None))
    
    def reduceByKey(self, func: Callable[[Any, Any], Any]):
        return RDD(prev=self, op=("reduceByKey", func))
    
    def collect(self):
        return Scheduler().run(self)

    def get_num_partitions(self):
        if self.partitions is not None:
            return len(self.partitions)
        
        elif self.prev is not None:
            return self.prev.get_num_partitions()
        
        else:
            return 0