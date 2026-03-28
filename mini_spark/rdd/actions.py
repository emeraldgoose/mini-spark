class Actions:

    @staticmethod
    def collect(rdd):
        return rdd.context.scheduler.run(rdd)

    @staticmethod
    def count(rdd):
        return len(Actions.collect(rdd))

    @staticmethod
    def take(rdd, n):
        data = Actions.collect(rdd)
        return data[:n]