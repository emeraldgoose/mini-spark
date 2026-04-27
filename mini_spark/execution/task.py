from typing import Callable

from mini_spark.storage.partition import Partition

class Task:
    def __init__(self, rdd: "RDD", partition: Partition):
        self.rdd = rdd
        self.partition = partition

    def run(self):
        return list(self.rdd.compute(self.partition))

class ReduceTask(Task):
    def __init__(self, partition_id: int, shuffle_manager: "ShuffleManager", func: Callable):
        self.partition_id = partition_id
        self.shuffle_manager = shuffle_manager
        self.func = func

    def run(self):
        data = self.shuffle_manager.read(partition_id=self.partition_id)

        grouped = {}
        for k, v in data:
            grouped.setdefault(k, []).append(v)

        result = []
        for k, values in grouped.items():
            res = values[0]
            for v in values[1:]:
                res = self.func(res, v)
            result.append((k, res))

        return result