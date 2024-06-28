from __future__ import annotations

import os
from functools import cached_property

import redis.asyncio as redis
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from moriarty.matrix.connector.invoker import get_bridge_name
from moriarty.matrix.job_manager.bridge_wrapper import BridgeWrapper, get_bridge_wrapper
from moriarty.matrix.operator_.autoscaler import (
    AutoscalerManager,
    get_autoscaler_manager,
)
from moriarty.matrix.operator_.dbutils import get_db_session
from moriarty.matrix.operator_.orm import AutoscalerORM, EndpointORM
from moriarty.matrix.operator_.rds import get_redis_client
from moriarty.matrix.operator_.spawner import plugin
from moriarty.matrix.operator_.spawner.manager import get_spawner
from moriarty.sidecar.params import MatrixCallback


def get_bridger(
    bridge_name: str = Depends(get_bridge_name),
    bridge_wrapper: BridgeWrapper = Depends(get_bridge_wrapper),
    redis_client: redis.Redis | redis.RedisCluster = Depends(get_redis_client),
    session: AsyncSession = Depends(get_db_session),
) -> Bridger:
    return Bridger(
        bridge_name=bridge_name,
        bridge_wrapper=bridge_wrapper,
        redis_client=redis_client,
        session=session,
    )


async def get_operaotr(
    spawner: plugin.Spawner = Depends(get_spawner),
    bridger: Bridger = Depends(get_bridger),
    session: AsyncSession = Depends(get_db_session),
    redis_client: redis.Redis | redis.RedisCluster = Depends(get_redis_client),
    autoscaler_manager: AutoscalerManager = Depends(get_autoscaler_manager),
) -> Operator:
    return Operator(
        spawner=spawner,
        bridger=bridger,
        session=session,
        redis_client=redis_client,
        autoscaler_manager=autoscaler_manager,
    )


class Bridger:
    def __init__(
        self,
        bridge_name: str,
        bridge_wrapper: BridgeWrapper,
        redis_client: redis.Redis | redis.RedisCluster,
        session: AsyncSession,
    ) -> None:
        self.bridge_name = bridge_name
        self.bridge_wrapper = bridge_wrapper
        self.redis_client = redis_client
        self.session = session

    async def bridge_all(self) -> None:
        pass


class Operator:
    def __init__(
        self,
        spawner: plugin.Spawner,
        bridger: Bridger,
        session: AsyncSession,
        redis_client: redis.Redis | redis.RedisCluster,
        autoscaler_manager: AutoscalerManager,
    ) -> None:
        self.spawner = spawner
        self.bridger = bridger
        self.session = session
        self.redis_client = redis_client
        self.autoscaler_manager = autoscaler_manager

    async def handle_callback(self, callback: MatrixCallback) -> None: ...
