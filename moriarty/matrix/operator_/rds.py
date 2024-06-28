from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

import redis.asyncio as redis
from fastapi import Depends

from moriarty.matrix.operator_.config import Config, get_config


async def get_redis_client(
    config: Config = Depends(get_config),
) -> AsyncGenerator[Any, redis.Redis | redis.RedisCluster]:
    redis_url = config.redis.get_redis_url()
    if config.redis.cluster:
        redis_client = redis.RedisCluster.from_url(redis_url, decode_responses=True)
    else:
        redis_client = redis.from_url(redis_url, decode_responses=True)
    try:
        yield redis_client
    finally:
        await redis_client.aclose()


@asynccontextmanager
async def open_redis_client(
    config: Config,
) -> AsyncGenerator[Any, redis.Redis | redis.RedisCluster]:
    redis_url = config.redis.get_redis_url()
    if config.redis.cluster:
        redis_client = redis.RedisCluster.from_url(redis_url, decode_responses=True)
    else:
        redis_client = redis.from_url(redis_url, decode_responses=True)
    try:
        yield redis_client
    finally:
        await redis_client.aclose()
