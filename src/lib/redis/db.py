import redis
from redis.exceptions import ConnectionError
import conf.redis as db_config
from src.lib.log import Logger

log = Logger()


class RedisClient:
    def __init__(self, db=None):
        self._connection_pool = redis.ConnectionPool(
            host=db_config.host,
            port=db_config.port,
            db=db or db_config.db_base,
            password=db_config.password,
            decode_responses=db_config.decode_responses,
        )
        self._client = redis.StrictRedis(connection_pool=self._connection_pool)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get(self, key):
        try:
            return self._client.get(key)
        except ConnectionError as e:
            log.logger.error(f"get error: {e}")
            return None

    def set(self, key, value, ex=None):
        try:
            return self._client.set(key, value, ex=ex)
        except ConnectionError as e:
            log.logger.error(f"set error: {e}")
            return False

    def delete(self, key):
        try:
            return self._client.delete(key)
        except ConnectionError as e:
            log.logger.error(f"delete error: {e}")
            return False

    def hgetall(self, key):
        try:
            return self._client.hgetall(name=key)
        except ConnectionError as e:
            log.logger.error(f"hgetall error: {e}")
            return False

    def hset(self, key, arr, value):
        try:
            return self._client.hset(name=key, key=arr, value=value)
        except ConnectionError as e:
            log.logger.error(f"hset error: {e}")
            return False

    def getMax(self, key):
        try:
            return self._client.hset(key)
        except ConnectionError as e:
            log.logger.error(f"getMax error: {e}")
            return False

    def rpush(self, key, value):
        try:
            return self._client.rpush(key, value)
        except ConnectionError as e:
            log.logger.error(f"rpush error: {e}")
            return False

    def lrange(self, key, start=0, end=-1):
        try:
            return self._client.lrange(key, start, end)
        except ConnectionError as e:
            log.logger.error(f"lrange error: {e}")
            return False

    def lrem(self, key, value, count=0):
        try:
            return self._client.lrem(key, value, count)
        except ConnectionError as e:
            log.logger.error(f"lrem error: {e}")
            return False

    def llen(self, key):
        try:
            return self._client.llen(key)
        except ConnectionError as e:
            log.logger.error(f"llen error: {e}")
            return False

    def lpop(self, key):
        try:
            return self._client.lpop(key)
        except ConnectionError as e:
            log.logger.error(f"lpop error: {e}")
            return False

    def lpush(self, key, value):
        try:
            return self._client.lpush(key, value)
        except ConnectionError as e:
            log.logger.error(f"lpush error: {e}")
            return False

    def smembers(self, key):
        try:
            return self._client.smembers(key)
        except ConnectionError as e:
            log.logger.error(f"smembers error: {e}")
            return False

    def sadd(self, key, value):
        try:
            return self._client.sadd(key, value)
        except ConnectionError as e:
            log.logger.error(f"sadd error: {e}")
            return False

    def srem(self, key, value):
        try:
            return self._client.srem(key, value)
        except ConnectionError as e:
            log.logger.error(f"srem error: {e}")
            return False

    def close(self):
        self._connection_pool.disconnect()


# # 使用示例
# redis_client = RedisClient()
# value = redis_client.get('my_key')
# if value is not None:
#     print(f"Value: {value}")
# redis_client.set('another_key', 'new_value', ex=60)  # 设置键值对，过期时间为60秒
# redis_client.delete('my_key')
# redis_client.close()

# # 使用示例，利用with语句自动管理资源
# if __name__ == "__main__":
#     with RedisClient() as redis_client:
#         value = redis_client.get('my_key')
#         if value is not None:
#             print(f"Value: {value}")
#         redis_client.set('another_key', 'new_value', ex=60)  # 设置键值对，过期时间为60秒
#         redis_client.delete('my_key')
