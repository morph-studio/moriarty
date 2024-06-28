from __future__ import annotations

import redis.asyncio as redis
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from moriarty.matrix.operator_.dbutils import get_db_session
from moriarty.matrix.operator_.orm import AutoscalerORM, EndpointORM
from moriarty.matrix.operator_.rds import get_redis_client
from moriarty.matrix.operator_.spawner import plugin
from moriarty.matrix.operator_.spawner.manager import get_spawner


def get_autoscaler_manager(
    spawner=Depends(get_spawner),
    redis_client=Depends(get_redis_client),
    session=Depends(get_db_session),
):
    return AutoscalerManager(redis_client=redis_client, session=session, spawner=spawner)


class AutoscalerManager:
    def __init__(
        self,
        redis_client: redis.Redis | redis.RedisCluster,
        session: AsyncSession,
        spawner: plugin.Spawner,
    ) -> None:
        self.redis_client = redis_client
        self.session = session
        self.spawner = spawner

    async def scan_and_update(self) -> None:
        pass
