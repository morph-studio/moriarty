from __future__ import annotations

import math
from functools import cached_property

import redis.asyncio as redis
from pydantic import BaseModel
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from moriarty.log import logger
from moriarty.matrix.operator_.enums_ import MetricType
from moriarty.matrix.operator_.orm import AutoscalerORM, EndpointORM
from moriarty.matrix.operator_.spawner import plugin
from moriarty.sidecar.producer import JobProducer


class EndpointMetrics(BaseModel):
    endpoint_name: str
    metrics: dict[MetricType, float] = dict()
    replicas: int


class MetricsManager:
    def __init__(self, spawner: plugin.Spawner, redis_client: redis.Redis) -> None:
        self.spawner = spawner
        self.redis_client = redis_client

    @cached_property
    def job_producer(self) -> JobProducer:
        return JobProducer(redis_client=self.redis_client)

    async def count_pending_jobs(self, endpoint_name: str) -> int:
        return await self.job_producer.count_unprocessed_jobs(endpoint_name=endpoint_name)

    async def get_metrics(self, endpoint_name: str) -> EndpointMetrics:
        unprocessed_count = await self.count_pending_jobs(endpoint_name)
        replicas = max(1, await self.spawner.count_avaliable_instances(endpoint_name))
        return EndpointMetrics(
            endpoint_name=endpoint_name,
            metrics={
                MetricType.pending_jobs: unprocessed_count,
                MetricType.pending_jobs_per_instance: unprocessed_count / replicas,
            },
            replicas=replicas,
        )

    def calculate_least_replicas(
        self,
        metric: MetricType,
        current_metric_value: float | int,
        metric_threshold: float,
        current_instance_value: int,
    ) -> int:
        logger.debug(
            f"Calculating least replicas:`{metric}`(current `{current_metric_value}` threshold `{metric_threshold}`)"
        )
        if metric == MetricType.pending_jobs:
            return int(current_metric_value - int(metric_threshold))
        if metric == MetricType.pending_jobs_per_instance:
            if metric_threshold == 0:
                metric_threshold = 1
            return math.ceil(current_metric_value * current_instance_value / metric_threshold)

        raise NotImplementedError

    def calculate_queue_capacity(self, autoscaler: AutoscalerORM) -> int:
        metric: MetricType = autoscaler.metrics
        if metric == MetricType.pending_jobs:
            return autoscaler.metrics_threshold + autoscaler.max_replicas + 1
        if metric == MetricType.pending_jobs_per_instance:
            return math.ceil(autoscaler.metrics_threshold) * autoscaler.max_replicas + 1

        raise NotImplementedError


class EndpointMixin:
    session: AsyncSession

    async def get_endpoint_orm(self, endpoint_name: str) -> EndpointORM | None:
        return (
            await self.session.execute(
                select(EndpointORM).where(EndpointORM.endpoint_name == endpoint_name)
            )
        ).scalar_one_or_none()

    async def get_scaleable_endpoints(self) -> list[str]:
        return (await self.session.execute(select(AutoscalerORM.endpoint_name))).scalars().all()

    async def get_avaliable_endpoints(self) -> list[str]:
        endpoint_names = (
            (
                await self.session.execute(
                    select(EndpointORM.endpoint_name).where(
                        EndpointORM.replicas > 0,
                    )
                )
            )
            .scalars()
            .all()
        )

        endpoint_names += await self.get_scaleable_endpoints()

        return list(set(endpoint_names))


class AutoscaleMixin:
    async def get_autoscaler_orm(self, endpoint_name: str) -> AutoscalerORM | None:
        return (
            await self.session.execute(
                select(AutoscalerORM).where(AutoscalerORM.endpoint_name == endpoint_name)
            )
        ).scalar_one_or_none()
