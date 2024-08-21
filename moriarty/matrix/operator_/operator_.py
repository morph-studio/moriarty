from __future__ import annotations

import asyncio
import os
from functools import cached_property

import redis.asyncio as redis
from fastapi import Depends, HTTPException, status
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from moriarty.envs import get_bridge_result_queue_url
from moriarty.log import logger
from moriarty.matrix.connector.invoker import get_bridge_name
from moriarty.matrix.job_manager.bridge_wrapper import BridgeWrapper, get_bridge_wrapper
from moriarty.matrix.job_manager.params import InferenceJob, InferenceResult
from moriarty.matrix.operator_.autoscaler import (
    AutoscalerManager,
    get_autoscaler_manager,
)
from moriarty.matrix.operator_.dbutils import get_db_session
from moriarty.matrix.operator_.mixin import (
    AutoscaleMixin,
    EndpointMixin,
    MetricsManager,
)
from moriarty.matrix.operator_.orm import AutoscalerORM, EndpointORM, InferenceLogORM
from moriarty.matrix.operator_.params import (
    ContainerScope,
    CreateEndpointParams,
    ListEndpointsResponse,
    QueryEndpointResponse,
    ResourceScope,
    ScheduleScope,
    UpdateEndpointParams,
)
from moriarty.matrix.operator_.rds import get_redis_client
from moriarty.matrix.operator_.spawner import plugin
from moriarty.matrix.operator_.spawner.manager import get_spawner
from moriarty.sidecar.params import MatrixCallback
from moriarty.sidecar.producer import JobProducer


def get_bridger(
    spawner: plugin.Spawner = Depends(get_spawner),
    bridge_name: str = Depends(get_bridge_name),
    bridge_wrapper: BridgeWrapper = Depends(get_bridge_wrapper),
    redis_client: redis.Redis | redis.RedisCluster = Depends(get_redis_client),
    session: AsyncSession = Depends(get_db_session),
    bridge_result_queue_url: str = Depends(get_bridge_result_queue_url),
) -> Bridger:
    return Bridger(
        spawner=spawner,
        bridge_name=bridge_name,
        bridge_wrapper=bridge_wrapper,
        redis_client=redis_client,
        session=session,
        bridge_result_queue_url=bridge_result_queue_url,
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


class Bridger(EndpointMixin, AutoscaleMixin):
    def __init__(
        self,
        spawner: plugin.Spawner,
        bridge_name: str,
        bridge_wrapper: BridgeWrapper,
        redis_client: redis.Redis | redis.RedisCluster,
        session: AsyncSession,
        bridge_result_queue_url: None | str = None,
    ) -> None:
        self.spawner = spawner
        self.bridge_name = bridge_name
        self.bridge_wrapper = bridge_wrapper
        self.redis_client = redis_client
        self.session = session
        self.bridge_result_queue_url = bridge_result_queue_url

        self.metrics_manager = MetricsManager(
            spawner=spawner,
            redis_client=self.redis_client,
        )

    @cached_property
    def job_producer(self) -> JobProducer:
        return JobProducer(redis_client=self.redis_client)

    async def bridge_all(self) -> None:
        avaliable_endpoints = await self.get_avaliable_endpoints()
        try:
            await asyncio.gather(
                *[self.bridge_one(endpoint_name) for endpoint_name in avaliable_endpoints]
            )
        except Exception as e:
            logger.exception(e)
            await self.session.rollback()
        else:
            await self.session.commit()

    async def has_capacity(self, endpoint_name: str) -> bool:
        endpoint_orm = await self.get_endpoint_orm(endpoint_name)
        if endpoint_orm is None:
            logger.warning(f"Endpoint not found: {endpoint_name}, may be deleted?")
            return False

        pending_count = await self.job_producer.count_unprocessed_jobs(endpoint_name)
        logger.debug(f"Pending count for {endpoint_name}: {pending_count}")

        autoscaler_orm = await self.get_autoscaler_orm(endpoint_name)
        if not autoscaler_orm:
            # No autoscaler, use default behavior
            concurrency = endpoint_orm.concurrency
            return (
                pending_count
                < endpoint_orm.queue_capacity
                + await self.spawner.count_avaliable_instances(endpoint_name) * concurrency
            )
        else:
            return pending_count < self.metrics_manager.calculate_queue_capacity(autoscaler_orm)

    async def bridge_one(self, endpoint_name: str) -> None:
        async def _warp_produce_job(job: InferenceJob) -> None:
            is_processed = (
                await self.session.execute(
                    select(InferenceLogORM).where(InferenceLogORM.inference_id == job.inference_id)
                )
            ).scalar_one_or_none() is not None
            if is_processed:
                logger.info(f"Job already processed: {job}, skip")
                return

            await self.job_producer.invoke(
                endpoint_name,
                params=job.payload,
            )
            log_orm = InferenceLogORM(
                inference_id=job.inference_id,
                endpoint_name=endpoint_name,
                inference_job=job.model_dump(),
            )
            self.session.add(log_orm)
            logger.debug(f"Inference job logged: {job}")

        logger.info(f"Bridge endpoint: {endpoint_name}")
        while await self.has_capacity(endpoint_name):
            logger.debug(f"{endpoint_name} has capacity, try to sample job")
            sampled_count = await self.bridge_wrapper.sample_job(
                bridge=self.bridge_name,
                endpoint_name=endpoint_name,
                process_func=_warp_produce_job,
            )
            if not sampled_count:
                return
            logger.debug(f"{sampled_count} job sampled -> {endpoint_name}")

    async def bridge_result(self, callback: MatrixCallback) -> None:
        await self.bridge_wrapper.enqueue_result(
            bridge=self.bridge_name,
            bridge_result_queue_url=self.bridge_result_queue_url,
            result=InferenceResult.from_proxy_callback(callback),
        )


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

    async def get_endpoint_orm(self, endpoint_name: str) -> EndpointORM | None:
        return (
            await self.session.execute(
                select(EndpointORM).where(EndpointORM.endpoint_name == endpoint_name)
            )
        ).scalar_one_or_none()

    async def get_endpoint_info(self, endpoint: str | EndpointORM) -> QueryEndpointResponse:
        if isinstance(endpoint, str):
            endpoint_orm = await self.get_endpoint_orm(endpoint)
        else:
            endpoint_orm = endpoint

        if not endpoint_orm:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Endpoint not found")

        endpoint_name = endpoint_orm.endpoint_name
        return QueryEndpointResponse(
            endpoint_name=endpoint_orm.endpoint_name,
            image=endpoint_orm.image,
            model_path=endpoint_orm.model_path,
            queue_capacity=endpoint_orm.queue_capacity,
            replicas=endpoint_orm.replicas,
            resource=ResourceScope(
                cpu_request=endpoint_orm.cpu_request,
                cpu_limit=endpoint_orm.cpu_limit,
                memory_request=endpoint_orm.memory_request,
                memory_limit=endpoint_orm.memory_limit,
                gpu_nums=endpoint_orm.gpu_nums,
                gpu_type=endpoint_orm.gpu_type,
            ),
            schedule=ScheduleScope(
                node_labels=endpoint_orm.node_labels,
                node_affinity=endpoint_orm.node_affinity,
            ),
            container=ContainerScope(
                environment_variables=endpoint_orm.environment_variables,
                environment_variables_secret_refs=endpoint_orm.environment_variables_secret_refs,
                commands=endpoint_orm.commands,
                args=endpoint_orm.args,
                invoke_port=endpoint_orm.invoke_port,
                invoke_path=endpoint_orm.invoke_path,
                health_check_path=endpoint_orm.health_check_path,
            ),
            runtime=await self.spawner.get_runtime_info(endpoint_name),
        )

    async def list_endpoints(
        self,
        limit: int,
        cursor: str | None,
        keyword: str | None,
        order_by: str,
        order: str,
    ) -> ListEndpointsResponse:
        if cursor:
            cursor = await self.get_endpoint_orm(cursor)
            if not cursor:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, detail="Cursor not found"
                )

        query = select(EndpointORM)
        if keyword:
            query = query.where(EndpointORM.endpoint_name.contains(keyword))

        if cursor:
            query = query.where(EndpointORM.id_ < cursor.id_)

        if order == "desc":
            query = query.order_by(getattr(EndpointORM, order_by).desc())

        else:
            query = query.order_by(getattr(EndpointORM, order_by))

        query = query.limit(limit)
        endpoint_orms = (await self.session.execute(query)).scalars().all()
        next_cursor = endpoint_orms[-1].endpoint_name if endpoint_orms else None

        return ListEndpointsResponse(
            endpoints=[
                await self.get_endpoint_info(endpoint_orm) for endpoint_orm in endpoint_orms
            ],
            cursor=next_cursor,
            limit=limit,
            total=(
                await self.session.execute(select(func.count(EndpointORM.endpoint_name)))
            ).scalar_one(),
        )

    async def create_endpoint(self, params: CreateEndpointParams) -> None:
        if await self.get_endpoint_orm(params.endpoint_name):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT, detail="Endpoint already exists"
            )

        endpoint_orm = EndpointORM(
            endpoint_name=params.endpoint_name,
            queue_capacity=params.queue_capacity,
            image=params.image,
            model_path=params.model_path,
            replicas=params.replicas,
            **{
                k: v
                for k, v in dict(
                    cpu_request=params.resource.cpu_request,
                    cpu_limit=params.resource.cpu_limit,
                    memory_request=params.resource.memory_request,
                    memory_limit=params.resource.memory_limit,
                    gpu_nums=params.resource.gpu_nums,
                    gpu_type=params.resource.gpu_type,
                ).items()
                if v is not None
            },
            **{
                k: v
                for k, v in dict(
                    environment_variables=params.container.environment_variables,
                    environment_variables_secret_refs=params.container.environment_variables_secret_refs,
                    commands=params.container.commands,
                    args=params.container.args,
                    invoke_port=params.container.invoke_port,
                    invoke_path=params.container.invoke_path,
                    health_check_path=params.container.health_check_path,
                ).items()
                if v is not None
            },
            **{
                k: v
                for k, v in dict(
                    node_labels=params.schedule.node_labels,
                    node_affinity=params.schedule.node_affinity,
                ).items()
                if v is not None
            },
            **{
                k: v
                for k, v in dict(
                    concurrency=params.sidecar.concurrency,
                    process_timeout=params.sidecar.process_timeout,
                    health_check_timeout=params.sidecar.health_check_timeout,
                    health_check_interval=params.sidecar.health_check_interval,
                ).items()
                if v is not None
            },
        )
        self.session.add(endpoint_orm)
        await self.session.commit()
        await self.session.refresh(endpoint_orm)
        try:
            await self.spawner.create(endpoint_orm)
        except Exception as e:
            logger.exception(e)
            logger.info(f"Rollback create endpoint: {endpoint_orm.endpoint_name}")
            await self.session.delete(endpoint_orm)
            await self.session.commit()
            raise e

    async def update_endpoint(self, endpoint_name: str, params: UpdateEndpointParams) -> None:
        endpoint_orm = await self.get_endpoint_orm(endpoint_name)
        if not endpoint_orm:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Endpoint not found")

        if (
            params.replicas
            and endpoint_orm.replicas != params.replicas
            and await self.autoscaler_manager.get_autoscaler_orm(endpoint_name)
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Replicas cannot be updated as autoscaler is enabled. Try set autoscaler.",
            )

        await self.session.execute(
            update(EndpointORM)
            .where(EndpointORM.endpoint_name == endpoint_name)
            .values(
                **{
                    k: v
                    for k, v in dict(
                        queue_capacity=params.queue_capacity,
                        image=params.image,
                        model_path=params.model_path,
                        replicas=params.replicas,
                    ).items()
                    if v is not None
                },
                **(
                    {
                        k: v
                        for k, v in dict(
                            cpu_request=params.resource.cpu_request,
                            cpu_limit=params.resource.cpu_limit,
                            memory_request=params.resource.memory_request,
                            memory_limit=params.resource.memory_limit,
                            gpu_nums=params.resource.gpu_nums,
                            gpu_type=params.resource.gpu_type,
                        ).items()
                        if v is not None
                    }
                    if params.resource is not None
                    else {}
                ),
                **(
                    {
                        k: v
                        for k, v in dict(
                            environment_variables=params.container.environment_variables,
                            environment_variables_secret_refs=params.container.environment_variables_secret_refs,
                            commands=params.container.commands,
                            args=params.container.args,
                            invoke_port=params.container.invoke_port,
                            invoke_path=params.container.invoke_path,
                            health_check_path=params.container.health_check_path,
                        ).items()
                        if v is not None
                    }
                    if params.container is not None
                    else {}
                ),
                **(
                    {
                        k: v
                        for k, v in dict(
                            node_labels=params.schedule.node_labels,
                            node_affinity=params.schedule.node_affinity,
                        ).items()
                        if v is not None
                    }
                    if params.schedule is not None
                    else {}
                ),
                **(
                    {
                        k: v
                        for k, v in dict(
                            concurrency=params.sidecar.concurrency,
                            process_timeout=params.sidecar.process_timeout,
                            health_check_timeout=params.sidecar.health_check_timeout,
                            health_check_interval=params.sidecar.health_check_interval,
                        ).items()
                        if v is not None
                    }
                    if params.schedule is not None
                    else {}
                ),
            )
        )
        await self.session.commit()
        await self.session.refresh(endpoint_orm)
        try:
            await self.spawner.update(endpoint_orm, need_restart=params.need_restart)
        except Exception as e:
            logger.exception(e)
            logger.warning(f"Failed to update deployment but already update db. Try update again.")
            raise

        return await self.get_endpoint_info(endpoint_orm)

    async def delete_endpoint(self, endpoint_name: str) -> None:
        await self.spawner.delete(endpoint_name)
        await self.session.execute(
            delete(EndpointORM).where(EndpointORM.endpoint_name == endpoint_name)
        )
        await self.session.commit()
        await self.autoscaler_manager.delete(endpoint_name)
