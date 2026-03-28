import hashlib

def stable_hash(key: str) -> int:
    return int(hashlib.md5(key.encode()).hexdigest(), 16)

class HashPartitioner:
    def __init__(self, num_partitions):
        self.num_partitions = num_partitions

    def get_partition(self, key: str) -> int:
        return stable_hash(key) % self.num_partitions
