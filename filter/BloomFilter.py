from redis import Redis
import math


class HashMap(object):
    def __init__(self, m, seed):
        self.m = m - 1
        self.seed = seed

    def hash(self, value):
        ret = 0
        for idx in range(len(value)):
            ret += self.seed * ret + ord(value[idx])
        return self.m & ret


class BloomFilter:
    """
    布隆过滤器
    """
    def __init__(self, redis_conn: Redis, key, size=10000000, error_rate=0.0001):
        # 计算过滤器大小
        self.m = round(-size * math.log(error_rate) / math.log(2) ** 2)
        # 计算需要多少哈希
        self.k = round(self.m / size * math.log(2))
        # 生成哈希列表
        self.maps = [HashMap(self.m, seed) for seed in range(self.k)]
        # redis
        self.redis = redis_conn
        # key
        self.key = key

    def insert(self, value):
        for func in self.maps:
            offset = func.hash(value)
            self.redis.setbit(self.key, offset, 1)

    def exist(self, value):
        for f in self.maps:
            offset = f.hash(value)
            if self.redis.getbit(self.key, offset) == 0:
                return False
        return True
    
