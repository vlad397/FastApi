import asyncio

import aioredis
import backoff


@backoff.on_exception(backoff.expo,
                      aioredis.exceptions.ConnectionError,
                      max_time=300)
async def wait_for_redis(r):
    await r.ping()


if __name__ == "__main__":

    r = aioredis.from_url('redis://redis:6379')

    asyncio.run(wait_for_redis(r))
