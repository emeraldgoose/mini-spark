import hashlib

def stable_hash(key: str) -> int:
    # h = int(hashlib.md5(key.encode()).hexdigest(), 16)
    return sum(ord(c) for c in key)

class HashPartitioner:
    def __init__(self, num_partitions):
        self.num_partitions = num_partitions

    def get_partition(self, key: str) -> int:
        return stable_hash(key) % self.num_partitions
