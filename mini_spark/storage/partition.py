from typing import List, Any

class Partition:
    def __init__(self, data: List[Any], partition_id: int = None):
        self.data = data
        self.partition_id = partition_id

    def __len__(self):
        return len(self.data)