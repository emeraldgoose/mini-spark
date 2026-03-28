## Mini-Spark

### Architecture
```
mini_spark/
├── rdd/
│   ├── rdd.py
│   ├── transformations.py
│   └── actions.py
│
├── execution/
│   ├── scheduler.py
│   ├── stage.py
│   └── task.py
│
├── shuffle/
│   ├── shuffle_manager.py
│   └── partitioner.py
│
├── storage/
│   └── partition.py
│
├── context.py
└── utils/mini_spark/
├── rdd/
│   ├── rdd.py
│   ├── transformations.py
│   └── actions.py
│
├── execution/
│   ├── scheduler.py
│   ├── stage.py
│   └── task.py
│
├── shuffle/
│   ├── shuffle_manager.py
│   └── partitioner.py
│
├── storage/
│   └── partition.py
│
├── context.py
└── utils/
```

### RDD
책임:
- DAG 구성 (lineage)
- transformation 정의 (lazy)

반드시 있어야 하는 필드
- partitions
- prev (부모 RDD)
- op (어떤 연산인지)
- func (함수)

스스로 생각해야 할 질문
- RDD는 immutable인가?
- op를 string으로 할지 클래스로 할지?
- lineage를 어떻게 저장할지?

### Transformations / Actions
파일 분리 이유:
- Spark도 논리적으로 분리되어 있음

**transformations.py**
- map
- filter
- flatMap
- groupByKey
- reduceByKey

특징: 새로운 RDD 반환

**actions.py**
- collect
- count
- take

특징: 실행 트리거

### Scheduler (execution/scheduler.py)
책임:
- DAG 실행
- stage 분리

필수 기능
- run(rdd)
- build_stages(rdd)
- execute_stage(stage)

스스로 고민해야 할 핵심:
- 언제 stage를 나눌 것인가?
- shuffle 기준은 무엇인가?

### Stage (executions/stage.py)

책임:
- 하나의 실행 단위
```
Stage
 ├── tasks
 └── dependency (이전 stage)
```
질문:
- stage는 RDD 단위인가? operation 단위인가?

### Task (execution/task.py)

책임:
- partition 하나 처리
- Task = (partition + operation chain)

핵심 질문:
- task는 함수 하나만 실행할까?
- 아니면 여러 transformation을 fuse할까?
- 이게 Spark의 pipelining 개념

### Partitioner (shuffle/partitioner.py)

책임:
- key → partition mapping
- hash(key) % num_partitions

질문:
- 왜 partitioner가 필요할까?
- 없으면 어떤 문제가 생길까?

### Shuffle Manager (shuffle/shuffle_manager.py)

책임:
- partition 간 데이터 재분배
```
input partitions
    ↓
(key 기준 분배)
    ↓
output partitions
```

고민해야 할 것:
- 메모리 vs 디스크 (일단 메모리로)
- groupByKey vs reduceByKey 차이

### Partition (storage/partition.py)

책임:
- 실제 데이터 컨테이너
- Partition = list 같은 것

질문:
- iterator로 할까?
- materialized list로 할까?

### Spark Context (context.py)

책임:
- entry point
- sc.parallelize(...)

최소 기능:
- RDD 생성

### 반드시 구현해야 하는 기능 순서
STEP 1: RDD + map/filter + collect

- DAG + lazy 구조 완성

STEP 2: partition 도입

- 데이터 구조를 list → list of lists로 변경

STEP 3: groupByKey (shuffle)

- 여기서 멘탈 한 번 깨짐 (정상)

STEP 4: reduceByKey

- groupByKey랑 차이 느껴야 함

STEP 5: scheduler + stage 분리

- narrow vs wide dependency 이해

STEP 6: 병렬 실행 (multiprocessing)

- executor 개념 체득

### 구현하면서 반드시 고민해야 할 질문들
Q1. 왜 lazy evaluation인가?

→ eager면 뭐가 깨지나?

Q2. lineage는 왜 필요한가?

→ checkpoint 없이 fault tolerance 가능 이유

Q3. shuffle은 왜 비싼가?

→ 네 코드에서 직접 느껴봐야 함

Q4. groupByKey vs reduceByKey 차이

→ 메모리 / 네트워크 관점에서

Q5. stage는 왜 필요한가?

→ 그냥 DAG 한 번에 실행하면 안 되나?