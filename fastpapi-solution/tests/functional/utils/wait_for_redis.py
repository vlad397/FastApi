import backoff
import redis

from functional.settings import REDIS_HOST, REDIS_PORT


@backoff.on_exception(backoff.expo,
                      redis.exceptions.ConnectionError,
                      max_time=300)
def wait_for_redis(r):
    r.ping()


if __name__ == "__main__":

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

    wait_for_redis(r)
