from __future__ import annotations

import os
from functools import cached_property

import redis.asyncio as redis
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from moriarty.matrix.operator_.autoscaler import (
    AutoscalerManager,
    get_autoscaler_manager,
)
from moriarty.matrix.operator_.dbutils import get_db_session
from moriarty.matrix.operator_.orm import AutoscalerORM, EndpointORM
from moriarty.matrix.operator_.rds import get_redis_client
from moriarty.matrix.operator_.spawner import plugin
from moriarty.matrix.operator_.spawner.manager import (
    SpawnerManager,
    get_spawner,
    get_spawner_manager,
)
from moriarty.sidecar.params import MatrixCallback


async def get_operaotr(
    spawner: plugin.Spawner = Depends(get_spawner),
    session: AsyncSession = Depends(get_db_session),
    redis_client: redis.Redis | redis.RedisCluster = Depends(get_redis_client),
    autoscaler_manager: AutoscalerManager = Depends(get_autoscaler_manager),
) -> Operator:
    return Operator(
        spawner=spawner,
        session=session,
        redis_client=redis_client,
        autoscaler_manager=autoscaler_manager,
    )


class Operator:
    def __init__(
        self,
        spawner: plugin.Spawner,
        session: AsyncSession,
        redis_client: redis.Redis | redis.RedisCluster,
        autoscaler_manager: AutoscalerManager,
    ) -> None:
        self.spawner = spawner
        self.session = session
        self.redis_client = redis_client
        self.autoscaler_manager = autoscaler_manager

    async def handle_callback(self, callback: MatrixCallback, token: str = None) -> None: ...
