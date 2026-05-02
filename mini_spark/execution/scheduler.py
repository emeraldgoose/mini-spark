from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Any

from mini_spark.execution.task import GroupByKeyTask, ReduceTask, Task
from mini_spark.rdd.transformations import GroupByKeyTransformation, ReduceByKeyTransformation
from mini_spark.shuffle.shuffle_manager import ShuffleManager
from mini_spark.storage.partition import Partition

class Scheduler:
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=4)  # 간단한 스레드 풀로 병렬 실행 시뮬레이션
        self.shuffle_manager = ShuffleManager()
        
        self.max_retries = 4  # 기본 최대 재시도 횟수 (Spark의 기본값)
        
        self.failed_tasks = set()  # 실패한 태스크 추적
        self.task_failures = {}  # 태스크별 실패 횟수 추적

    def execute(self, rdd):
        """
        RDD lineage를 따라가며 가장 부모 RDD의 파티션을 변환

        예:
        rdd3 = rdd2.map(...)
        rdd2 = rdd1.filter(...)

        execute(rdd3) -> execute(rdd2) -> execute(rdd1) -> rdd1.partitions 변환
        """
        if rdd.prev is None:
            return rdd.partitions
        return self.execute(rdd.prev)
    
    def run(self, rdd: "RDD"):
        """
        Action(collect 등) 실행 진입점

        transformation 종류에 따라 실행 방식 분기
        1. reduceByKey: shuffle 필요
        2. groupByKey: shuffle 필요
        3. 일반 map/filter/flatMap: 일반 Task 실행
        """
        if isinstance(rdd.transformation, ReduceByKeyTransformation):
            return self._run_reduce_by_key(rdd)
        
        if isinstance(rdd.transformation, GroupByKeyTransformation):
            return self._run_group_by_key(rdd)
        
        return self._run_normal_task(rdd)
    
    def _run_normal_task(self, rdd: "RDD"):
        """
        map/filter/flatMap 등 일반 Transformation 실행
        각 partition마다 Task 생성 후 병렬 실행
        """
        tasks = rdd.get_tasks()
        return self._run_tasks_parallel(tasks)

    def _run_reduce_by_key(self, rdd: "RDD"):
        """
        reduceByKey 실행 흐름

        Stage 1: map side task 실행
        Stage 2: shuffle write
        Stage 3: reduce task 실행
        """

        # 1. parent RDD를 partition 단위로 실행
        parent_rdd = rdd.prev

        # 결과를 partiton_id 기준 dict로 받음
        map_tasks = parent_rdd.get_tasks()
        map_results = self._run_tasks_parallel_with_partition(map_tasks)

        # dict -> Partition 리스트로 변환
        partitions = [
            Partition(data, partition_id=pid)
            for pid, data in sorted(map_results.items())
        ]

        # 2. shuffle write
        # key 기준으로 재분배하여 저장
        self.shuffle_manager.write(partitions)

        # 3. reduce task 생성
        reduce_tasks = [
            ReduceTask(
                partition_id=pid,
                shuffle_manager=self.shuffle_manager,
                func=rdd.transformation.func,
            )
            for pid in range(rdd.transformation.num_partitions)
        ]

        # reduce task 병렬 실행
        return self._run_tasks_parallel(reduce_tasks)
    
    def _run_group_by_key(self, rdd: "RDD"):
        """
        groupByKey 실행 흐름

        Stage 1: map task
        Stage 2: shuffle
        Stage 3: group reduce task
        """
        parent_rdd = rdd.prev

        # 부모 RDD 실행
        map_tasks = parent_rdd.get_tasks()
        map_results = self._run_tasks_parallel_with_partition(map_tasks)

        # dict -> Partition 리스트로 변환
        partitions = [
            Partition(data, partition_id=pid)
            for pid, data in sorted(map_results.items())
        ]

        # shuffle write
        self.shuffle_manager.write(partitions)

        # reduce side task 생성
        reduce_tasks = [
            GroupByKeyTask(
                partition_id=pid, 
                shuffle_manager=self.shuffle_manager
            )
            for pid in range(rdd.transformation.num_partitions)
        ]

        return self._run_tasks_parallel(reduce_tasks)

    def _run_tasks_parallel(self, tasks: List[Task]):
        """
        Task들을 병렬 실행 후 최종 결과를 list로 반환

        내부적으로는 partition별 결과를 받은 뒤,
        partition 순서대로 합쳐 collect 순서를 보장한다.

        예:
        P0 -> [1,2]
        P1 -> [3,4]

        결과: [1,2,3,4]
        """
        partition_results = self._run_tasks_parallel_with_partition(tasks)

        results = []
        # partition_id 순서대로 결과 합치기
        for pid in sorted(partition_results):
            results.extend(partition_results[pid])
        
        return results

    def _run_tasks_parallel_with_partition(self, tasks: List[Task]):
        """
        병렬 실행 함수

        기능:
        1. 모든 Task를 스레드 풀에 제출
        2. Task 완료를 기다리며 결과 수집
        3. Task 실패 시 재시도 (최대 self.max_retries 회)
        4. partition_id 기준으로 결과 저장

        return:
        {
            0: [...],
            1: [...],
            2: [...]
        }
        """
        futures = {} # future -> task 매핑
        retry_counts = {} # partition_id -> 재시도 횟수 매핑
        partition_results = {} # partition_id -> 결과 매핑

        # 1. 모든 Task를 스레드 풀에 제출 (병렬 시작)
        for task in tasks:
            pid = self._get_partition_id(task)
            retry_counts[pid] = 0

            future = self.executor.submit(task.run)
            futures[future] = task

        # 2. Task 완료를 기다리며 결과 수집 및 재시도 처리
        while futures:
            for future in as_completed(list(futures.keys())):
                task = futures.pop(future)
                pid = self._get_partition_id(task)

                try:
                    # 성공 시 결과 저장
                    partition_results[pid] = future.result()

                except Exception as e:
                    retry_counts[pid] += 1

                    # 최대 재시도 횟수 초과 시 예외 발생
                    if retry_counts[pid] > self.max_retries:
                        raise RuntimeError(
                            f"Task for partition {pid} failed after"
                            f"{self.max_retries} retries: {e}"
                        )
                    
                    # 동일 Task 재시도 (실제 Spark는 동일 Task ID로 재시도하지만, 여기서는 간단히 같은 작업을 다시 제출)
                    new_future = self.executor.submit(task.run)
                    futures[new_future] = task

        return partition_results

    def _get_partition_id(self, task: Task):
        """
        Task 종류마다 partition_id를 얻는 방식이 다름

        일반 Task:
            task.partition.partition_id
        
        ReduceTask / GroupByKeyTask:
            task.partition_id
        """
        if hasattr(task, "partition") and task.partition is not None:
            return task.partition.partition_id
        
        if hasattr(task, "partition_id"):
            return task.partition_id
        
        return 0

    def submit_task(self, task: Task):
        """
        외부에서 Task 하나만 실행하고 싶을 때 사용
        """
        return self.executor.submit(task.run)