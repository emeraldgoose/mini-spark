from mini_spark.storage.partition import Partition

class Task:
    def __init__(self, partition: Partition, rdd: "RDD"):
        self.partition = partition
        self.rdd = rdd