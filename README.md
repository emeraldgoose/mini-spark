## Mini-Spark
> Spark Study

### 디렉토리 구조
```
mini_spark/
├── rdd/
│   ├── _rdd.py             # RDD
│   ├── transformations.py  # Transformation
│   └── actions.py          # Actions
│
├── execution/
│   ├── scheduler.py        # Scheduler
│   ├── stage.py            # Stage
│   └── task.py             # Task
│
├── shuffle/
│   ├── shuffle_manager.py # ShuffleManager
│   └── partitioner.py     # HashPartitioner
│
├── storage/
│   └── partition.py       # Partition
│
├── context.py             # SparkContext
└── utils/
```

- RDD: 분산 데이터셋, partition 단위로 저장
    - cache, persist, checkpoint
- Transformations (Lazy Evaluation)
    - Narrow: map, filter, flatMap
    - Wide: groupByKey, reduceByKey (wide transformation)
- Actions
    - 실제 연산 트리거
    - collect(), count(), take()
- Scheduler: RDD DAG 실행 담당
- ShuffleManager: Shuffle 관리
- Partitioner: key 기반 파티셔닝
- Stage & Task:
    - Stage: shuffle 기준으로 작업의 논리적 단위
    - Task: stage 내 각 partition 실행 단위

### 사용예시
```
from mini_spark.context import SparkContext

sc = SparkContext()
rdd = sc.parallelize([("a", 1), ("b", 2), ("a", 3)], num_partitions=2)

# Transformation
rdd2 = rdd.map(lambda x: (x[0], x[1] * 2)).filter(lambda x: x[1] > 2)

# Wide Transformation
rdd3 = rdd2.reduceByKey(lambda x, y: x + y)

# Action
result = rdd3.collect()
print(result)  # [('a', 8), ('b', 4)]
```

### 테스트
```
python -m unittest discover -s mini_spark/tests
```
