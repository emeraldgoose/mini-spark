class Stage:
    def __init__(self, rdd: "RDD", stage_id: int):
        self.rdd = rdd
        self.stage_id = stage_id
        self.partitions = []

