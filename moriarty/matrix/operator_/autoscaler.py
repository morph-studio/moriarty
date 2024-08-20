from __future__ import annotations

import math
from functools import cached_property

import redis.asyncio as redis
from fastapi import Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from moriarty.log import logger
from moriarty.matrix.operator_.dbutils import get_db_session
from moriarty.matrix.operator_.enums_ import MetricType
from moriarty.matrix.operator_.mixin import (
    AutoscaleMixin,
    EndpointMetrics,
    EndpointMixin,
    MetricsManager,
)
from moriarty.matrix.operator_.orm import AutoscaleLogORM, AutoscalerORM, EndpointORM
from moriarty.matrix.operator_.params import (
    AutoscaleLog,
    QueryEndpointAutoscaleLogResponse,
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

    async def clear_cooldown(self, endpoint_name: str) -> None:
        scaleout_key = self.get_cooldown_key(endpoint_name, True)
        scalein_key = self.get_cooldown_key(endpoint_name, False)

        await self.redis_client.delete(scaleout_key, scalein_key)


class AutoscalerManager(EndpointMixin, CooldownMixin):
    def __init__(
        self,
        redis_client: redis.Redis | redis.RedisCluster,
        session: AsyncSession,
        spawner: plugin.Spawner,
    ) -> None:
        self.redis_client = redis_client
        self.session = session
        self.spawner = spawner
        self.metrics_manager = MetricsManager(spawner=spawner, redis_client=redis_client)

    async def _clean_not_exist_endpoint(self) -> None:
        logger.debug("Cleaning not exist endpoint...")
        await self.session.execute(
            delete(AutoscalerORM).where(
                AutoscalerORM.endpoint_name.not_in(select(EndpointORM.endpoint_name))
            )
        )
        await self.session.commit()
        logger.debug("Cleaned not exist endpoint")

    async def _scale(self, endpoint_name: str) -> None:
        autoscaler_orm = await self.get_autoscaler_orm(endpoint_name)
        if not autoscaler_orm:
            logger.warning(f"Autoscaler not found: {endpoint_name}, may be deleted?")
            return

        metrics = await self.metrics_manager.get_metrics(endpoint_name)
        logger.debug(f"Metrics Collected: {metrics}")
        target_replicas = await self._calculate_target_replicas(endpoint_name, metrics)
        if target_replicas is None or await self._is_cooldown(endpoint_name, target_replicas):
            return
        try:
            logger.info(
                f"Scale endpoint `{endpoint_name}` -> {target_replicas}, based on {metrics}"
            )
            await self.spawner.scale(endpoint_name, target_replicas)
        except Exception as e:
            logger.exception(e)
            await self._revoke_scale(endpoint_name)
        else:
            await self._commit_scale(endpoint_name, metrics, target_replicas)

    async def _revoke_scale(self, endpoint_name: str) -> None:
        logger.info(f"Revoke scale endpoint `{endpoint_name}`")
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
            metrics_log=metrics.model_dump(),
        )
        self.session.add(log_orm)
        await self.session.commit()
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
        autoscaler_orm = await self.get_autoscaler_orm(endpoint_name)

        if not autoscaler_orm:
            logger.warning(f"Autoscaler not found: {endpoint_name}, may be deleted?")
            return None

        current_metric = current_metrics.metrics.get(autoscaler_orm.metrics)
        if current_metric is None:
            logger.info(
                f"No metrics found for {autoscaler_orm.endpoint_name}.{autoscaler_orm.metrics}"
            )
            return None

        least_replicas = self.metrics_manager.calculate_least_replicas(
            metric=autoscaler_orm.metrics,
            current_metric_value=current_metric,
            metric_threshold=autoscaler_orm.metrics_threshold,
            current_instance_value=current_metrics.replicas,
        )
        logger.info(
            f"Least replicas calculated for {autoscaler_orm.endpoint_name}({autoscaler_orm.metrics}) is {least_replicas}"
        )
        if least_replicas < autoscaler_orm.min_replicas:
            logger.info(
                f"Using min replicas {autoscaler_orm.min_replicas} instead of {least_replicas}"
            )
            return autoscaler_orm.min_replicas
        if least_replicas > autoscaler_orm.max_replicas:
            logger.info(
                f"Using max replicas {autoscaler_orm.max_replicas} instead of {least_replicas}"
            )
            return autoscaler_orm.max_replicas
        return least_replicas

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

        await self.session.commit()

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
        endpoint_orm = await self.get_endpoint_orm(endpoint_name)
        if endpoint_orm is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Endpoint not found")

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
        await self.clear_cooldown(endpoint_name)

        logger.info(f"Autoscaler {endpoint_name} updated")
        return await self.get_autoscale_info(endpoint_name)

    async def delete(self, endpoint_name: str) -> None:
        await self.session.execute(
            delete(AutoscalerORM).where(AutoscalerORM.endpoint_name == endpoint_name)
        )

    async def get_autoscale_logs(
        self,
        endpoint_name: str,
        limit: int,
        cursor: int | None,
        order_by: str,
        order: str,
    ) -> QueryEndpointAutoscaleLogResponse:
        if cursor:
            cursor = await self.session.get(AutoscaleLogORM, cursor)
            if not cursor:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, detail="Cursor not found"
                )

        query = select(AutoscaleLogORM).where(AutoscaleLogORM.endpoint_name == endpoint_name)

        if cursor:
            query = query.where(AutoscaleLogORM.id_ < cursor.id_)

        if order == "desc":
            query = query.order_by(getattr(AutoscaleLogORM, order_by).desc())

        else:
            query = query.order_by(getattr(AutoscaleLogORM, order_by))

        query = query.limit(limit)
        total = (
            await self.session.execute(
                select(func.count()).select_from(
                    select(AutoscaleLogORM)
                    .where(AutoscaleLogORM.endpoint_name == endpoint_name)
                    .subquery()
                )
            )
        ).scalar()
        log_orms = (await self.session.execute(query)).scalars().all()
        next_cursor = log_orms[-1].id_ if log_orms else None

        return QueryEndpointAutoscaleLogResponse(
            logs=[
                AutoscaleLog(
                    id_=log_orm.id_,
                    endpoint_name=log_orm.endpoint_name,
                    old_replicas=log_orm.old_replicas,
                    new_replicas=log_orm.new_replicas,
                    metrics=log_orm.metrics,
                    metrics_threshold=log_orm.metrics_threshold,
                    metrics_log=log_orm.metrics_log,
                    details=log_orm.details,
                    created_at=log_orm.created_at,
                    updated_at=log_orm.updated_at,
                )
                for log_orm in log_orms
                if log_orm
            ],
            cursor=next_cursor,
            limit=limit,
            total=total,
        )
