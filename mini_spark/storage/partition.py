from typing import List, Any

class Partition:
    def __init__(self, data: List[Any]):
        self.data = data
        self.partition_id = None
        self.stage_id = None

    def __len__(self):
        return len(self.data)