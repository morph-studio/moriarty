import redis.asyncio as redis
from brq.producer import Producer

from moriarty.log import logger


class JobProducer:
    REDIS_PREFIX = "moriarty-brq"

    def __init__(self, redis_client: redis.Redis) -> None:
        self.redis_client = redis_client

    @property
    def producer(self):
        return Producer(self.redis_client, redis_prefix=self.REDIS_PREFIX)

    async def invoke(self, endpoint_name: str, params: dict) -> dict:
        logger.debug(f"Invoke endpoint: {endpoint_name}")
        return await self.producer.run_job(endpoint_name, kwargs={"payload": params})
