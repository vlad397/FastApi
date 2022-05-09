import backoff
import redis


@backoff.on_exception(backoff.expo,
                      redis.exceptions.ConnectionError,
                      max_time=300)
def wait_for_redis(r):
    r.ping()


if __name__ == "__main__":

    r = redis.Redis(host='redis', port=6379)

    wait_for_redis(r)
