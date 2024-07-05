from __future__ import annotations

import redis.asyncio as redis
from fastapi import Depends, HTTPException, status
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from moriarty.log import logger
from moriarty.matrix.operator_.dbutils import get_db_session
from moriarty.matrix.operator_.mixin import EndpointMixin
from moriarty.matrix.operator_.orm import AutoscaleLogORM, AutoscalerORM, EndpointORM
from moriarty.matrix.operator_.params import (
    QueryEndpointAutoscaleResponse,
    SetAutoscaleParams,
)
from moriarty.matrix.operator_.rds import get_redis_client
from moriarty.matrix.operator_.spawner import plugin
from moriarty.matrix.operator_.spawner.manager import get_spawner


def get_autoscaler_manager(
    spawner=Depends(get_spawner),
    redis_client=Depends(get_redis_client),
    session=Depends(get_db_session),
):
    return AutoscalerManager(redis_client=redis_client, session=session, spawner=spawner)


class AutoscalerManager(EndpointMixin):
    def __init__(
        self,
        redis_client: redis.Redis | redis.RedisCluster,
        session: AsyncSession,
        spawner: plugin.Spawner,
    ) -> None:
        self.redis_client = redis_client
        self.session = session
        self.spawner = spawner

    async def _clean_not_exist_endpoint(self) -> None:
        await self.session.execute(
            delete(AutoscalerORM).where(
                AutoscalerORM.endpoint_name.not_in(select(EndpointORM.endpoint_name))
            )
        )
        await self.session.commit()
        logger.info("Not exist endpoint cleaned")

    async def _scale(self, endpoint_name: str) -> None:
        autoscaler_orm = await self.get_autoscaler_orm(endpoint_name)
        if autoscaler_orm is None:
            return
        # TODO: Scale accroding metrics and log it into AutoscaleLogORM

    async def scan_and_scale(self) -> None:
        await self._clean_not_exist_endpoint()
        for endpoint in await self.get_avaliable_endpoints():
            await self._scale(endpoint)

    async def get_autoscaler_orm(self, endpoint_name: str) -> AutoscalerORM | None:
        return (
            await self.session.execute(
                select(AutoscalerORM).where(AutoscalerORM.endpoint_name == endpoint_name)
            )
        ).scalar_one_or_none()

    async def get_autoscale_info(self, endpoint_name: str) -> QueryEndpointAutoscaleResponse:
        autoscaler_orm = await self.get_autoscaler_orm(endpoint_name)
        if autoscaler_orm is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Autoscaler not found")

        return QueryEndpointAutoscaleResponse(
            endpoint_name=endpoint_name,
            min_replicas=autoscaler_orm.min_replicas,
            max_replicas=autoscaler_orm.max_replicas,
            scale_in_cooldown=autoscaler_orm.scale_in_cooldown,
            scale_out_cooldown=autoscaler_orm.scale_out_cooldown,
            metrics=autoscaler_orm.metrics,
            metrics_threshold=autoscaler_orm.metrics_threshold,
        )

    async def update(
        self, endpointd_name: str, params: SetAutoscaleParams
    ) -> QueryEndpointAutoscaleResponse:
        if not await self.get_autoscaler_orm(endpointd_name):
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Autoscaler not found")
        await self.session.execute(
            update(AutoscalerORM)
            .where(AutoscalerORM.endpoint_name == endpointd_name)
            .values(
                {
                    k: v
                    for k, v in dict(
                        min_replicas=params.min_replicas,
                        max_replicas=params.max_replicas,
                        scale_in_cooldown=params.scale_in_cooldown,
                        scale_out_cooldown=params.scale_out_cooldown,
                        metrics=params.metrics,
                        metrics_threshold=params.metrics_threshold,
                    ).items()
                    if v is not None
                }
            )
        )
        return await self.get_autoscale_info(params.endpoint_name)

    async def delete(self, endpoint_name: str) -> None:
        await self.session.execute(
            delete(AutoscalerORM).where(AutoscalerORM.endpoint_name == endpoint_name)
        )
