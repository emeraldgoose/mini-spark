from concurrent.futures import ThreadPoolExecutor
from typing import List, Any

from mini_spark.execution.task import ReduceTask, Task
from mini_spark.rdd.transformations import ReduceByKeyTransformation
from mini_spark.shuffle.shuffle_manager import ShuffleManager
from mini_spark.storage.partition import Partition

class Scheduler:
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=4)  # 간단한 스레드 풀로 병렬 실행 시뮬레이션
        self.shuffle_manager = ShuffleManager()

    def execute(self, rdd):
        if rdd.prev is None:
            return rdd.partitions
        return self.execute(rdd.prev)
    
    def run(self, rdd: "RDD"):
        if isinstance(rdd.transformation, ReduceByKeyTransformation):
            parent_rdd = rdd.prev
            map_tasks = parent_rdd.get_tasks()

            map_results = []
            for t in map_tasks:
                map_results.append(Partition(t.run()))

            self.shuffle_manager.write(map_results)

            reduce_tasks = [
                ReduceTask(partition_id=pid, shuffle_manager=self.shuffle_manager, func=rdd.transformation.func) 
                for pid in range(rdd.transformation.num_partitions)
            ]

            results = []
            for t in reduce_tasks:
                results.extend(t.run())
            
            return results

        tasks = rdd.get_tasks()

        if hasattr(rdd.transformation, "partitioner"):
            parent_data = []
            for t in tasks:
                parent_data.append(Partition(t.run()))  # ✔ Partition으로 감싸기

            self.shuffle_manager.write(parent_data)

        futures = [self.executor.submit(task.run) for task in tasks]  # Task의 run() 메서드를 병렬로 실행
        results = []
        for future in futures:
            results.extend(future.result())  # 각 Task의 결과를 하나의 리스트로 합치기

        return results
    
    def submit_task(self, task: Task):
        # Task를 스레드 풀에 제출하여 실행
        return self.executor.submit(task.run)