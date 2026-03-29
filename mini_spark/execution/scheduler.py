class Scheduler:
    def run(self, rdd: "RDD"):
        results = self._execute(rdd)
        return [x for part in results for x in part.data]
    
    def _execute(self, rdd: "RDD"):
        if rdd.partitions is not None:
            return rdd.partitions
        
        parent_partitions = self._execute(rdd.prev)
        return rdd.transformation.apply(parent_partitions)