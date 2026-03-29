## Mini-Spark
> Spark Study

### 디렉토리 구조
```
mini_spark/
├── rdd/
│   ├── rdd.py              # RDD
│   ├── transformations.py  # Transformation
│   └── actions.py          # Actions
│
├── execution/
│   ├── scheduler.py        # Scheduler
│   ├── stage.py
│   └── task.py
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
- Transformations (Lazy Evaluation)
    - map, filter, flatMap
    - groupByKey, reduceByKey (wide transformation)
- Actions (실제 실행)
    - collect(), count()
- Scheduler: RDD DAG 실행 담당
- ShuffleManager: wide transformation 처리
- Partitioner: key 기반 hash partitioning
- Stage & Task:
    - Stage: shuffle 단위로 구분
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
