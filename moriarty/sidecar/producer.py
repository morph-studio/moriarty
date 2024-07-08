import redis.asyncio as redis
from brq.models import Job
from brq.producer import Producer

from moriarty.log import logger

from .vars import GROUP_NAME, REDIS_PREFIX


class JobProducer:
    """
    A warpper of brq.producer.Producer
    """

    REDIS_PREFIX = REDIS_PREFIX
    GROUP_NAME = GROUP_NAME

    def __init__(self, redis_client: redis.Redis) -> None:
        self.redis_client = redis_client

    @property
    def producer(self):
        return Producer(self.redis_client, redis_prefix=self.REDIS_PREFIX)

    async def invoke(self, endpoint_name: str, params: dict) -> Job:
        logger.debug(f"Invoke endpoint: {endpoint_name}")
        return await self.producer.run_job(endpoint_name, kwargs={"payload": params})

    async def count_unprocessed_jobs(self, endpoint_name: str) -> int:
        return await self.producer.count_unprocessed_jobs(endpoint_name, group_name=self.GROUP_NAME)

    async def count_jobs(self, endpoint_name: str) -> int:
        return await self.producer.count_stream(endpoint_name)
