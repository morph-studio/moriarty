from __future__ import annotations

from functools import cached_property

import redis.asyncio as redis
from fastapi import Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from moriarty.log import logger
from moriarty.matrix.operator_.dbutils import get_db_session
from moriarty.matrix.operator_.mixin import AutoscaleMixin, EndpointMixin
from moriarty.matrix.operator_.orm import AutoscaleLogORM, AutoscalerORM, EndpointORM
from moriarty.matrix.operator_.params import (
    QueryEndpointAutoscaleResponse,
    SetAutoscaleParams,
)
from moriarty.matrix.operator_.rds import get_redis_client
from moriarty.matrix.operator_.spawner import plugin
from moriarty.matrix.operator_.spawner.manager import get_spawner
from moriarty.sidecar.producer import JobProducer


def get_autoscaler_manager(
    spawner=Depends(get_spawner),
    redis_client=Depends(get_redis_client),
    session=Depends(get_db_session),
):
    return AutoscalerManager(redis_client=redis_client, session=session, spawner=spawner)


class EndpointMetrics(BaseModel):
    endpoint_name: str


class MetricsMixin:
    spawner: plugin.Spawner
    redis_client: redis.Redis | redis.RedisCluster

    @cached_property
    def job_producer(self) -> JobProducer:
        return JobProducer(redis_client=self.redis_client)

    async def get_metrics(self, endpoint_name: str) -> EndpointMetrics:
        # TODO: Imp this later
        return EndpointMetrics(
            endpoint_name=endpoint_name,
        )


class CooldownMixin(AutoscaleMixin):
    prefix = "moriarty:autoscaler:cooldown"
    separator = ":"
    redis_client: redis.Redis | redis.RedisCluster

    def get_cooldown_key(self, endpoint_name: str, is_scale_out: bool) -> str:
        return self.separator.join(
            [
                self.prefix,
                "{%s}" % endpoint_name,
                "scaleup" if is_scale_out else "scaledown",
            ]
        )

    async def set_cooldown(self, endpoint_name: str, is_scale_out: bool) -> None:
        autoscaler_orm = await self.get_autoscaler_orm(endpoint_name)

        if not autoscaler_orm:
            logger.warning(f"Autoscaler not found: {endpoint_name}, may be deleted?")
            return

        cooldown_key = self.get_cooldown_key(endpoint_name, is_scale_out)
        cooldown_ttl = (
            autoscaler_orm.scale_out_cooldown if is_scale_out else autoscaler_orm.scale_in_cooldown
        )
        await self.redis_client.set(cooldown_key, 1, ex=cooldown_ttl)

    async def _is_cooldown(self, endpoint_name: str, target_replicas: int) -> bool:
        endpoint_orm = await self.get_endpoint_orm(endpoint_name)

        if not endpoint_orm or endpoint_orm.replicas == target_replicas:
            # Not exist or already scaled, treat as cooldown to skip
            return True

        cooldown_key = self.get_cooldown_key(endpoint_name, endpoint_orm.replicas < target_replicas)
        return bool(await self.redis_client.get(cooldown_key))


class AutoscalerManager(MetricsMixin, EndpointMixin, CooldownMixin):
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
        if not autoscaler_orm:
            logger.warning(f"Autoscaler not found: {endpoint_name}, may be deleted?")
            return

        metrics = await self.get_metrics(endpoint_name)
        target_replicas = await self._calculate_target_replicas(endpoint_name, metrics)
        if await self._is_cooldown(endpoint_name, target_replicas):
            return
        try:
            await self.spawner.scale(endpoint_name, target_replicas)
        except Exception as e:
            logger.exception(e)
            await self._revoke_scale(endpoint_name, metrics, target_replicas)
        else:
            await self._commit_scale(self, endpoint_name, metrics, target_replicas)

    async def _revoke_scale(self, endpoint_name: str) -> None:
        endpoint_orm = await self.get_endpoint_orm(endpoint_name)
        if endpoint_orm is None:
            logger.warning(f"Endpoint not found: {endpoint_name}, may be deleted?")
            return
        await self.spawner.scale(endpoint_name, endpoint_orm.replicas)

    async def _commit_scale(
        self, endpoint_name: str, metrics: EndpointMetrics, target_replicas: int
    ) -> None:
        endpoint_orm = await self.get_endpoint_orm(endpoint_name)
        autoscaler_orm = await self.get_autoscaler_orm(endpoint_name)

        is_scale_out = endpoint_orm.replicas < target_replicas

        logger.info(
            f"Commit scale endpoint `{endpoint_name}` {endpoint_orm.replicas} -> {target_replicas}"
        )
        log_orm = AutoscaleLogORM(
            endpoint_name=endpoint_name,
            old_replicas=endpoint_orm.replicas,
            new_replicas=target_replicas,
            metrics=autoscaler_orm.metrics,
            metrics_threshold=autoscaler_orm.metrics_threshold,
            metrics_value=metrics.model_dump(),
        )
        self.session.add(log_orm)
        await self.session.execute(
            update(EndpointORM)
            .where(EndpointORM.endpoint_name == endpoint_name)
            .values(replicas=target_replicas)
        )
        await self.session.commit()
        await self.set_cooldown(endpoint_name, is_scale_out)

    async def _calculate_target_replicas(
        self,
        endpoint_name: str,
        current_metrics: EndpointMetrics,
    ) -> int:
        # TODO: Imp this
        return 1

    async def get_scaleable_endpoints(self) -> list[str]:
        return (
            (
                await self.session.execute(
                    select(EndpointORM.endpoint_name).where(
                        EndpointORM.endpoint_name.in_(select(AutoscalerORM.endpoint_name))
                    )
                )
            )
            .scalars()
            .all()
        )

    async def scan_and_scale(self) -> None:
        await self._clean_not_exist_endpoint()
        for endpoint in await self.get_scaleable_endpoints():
            await self._scale(endpoint)

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

    async def set_autoscale(
        self, endpoint_name: str, params: SetAutoscaleParams
    ) -> QueryEndpointAutoscaleResponse:
        params = {
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
        if not await self.get_autoscaler_orm(endpoint_name):
            autoscaler_orm = AutoscalerORM(
                endpoint_name=endpoint_name,
                **params,
            )
            self.session.add(autoscaler_orm)
        else:
            await self.session.execute(
                update(AutoscalerORM)
                .where(AutoscalerORM.endpoint_name == endpoint_name)
                .values(params)
            )

        await self.session.commit()
        return await self.get_autoscale_info(endpoint_name)

    async def delete(self, endpoint_name: str) -> None:
        await self.session.execute(
            delete(AutoscalerORM).where(AutoscalerORM.endpoint_name == endpoint_name)
        )
