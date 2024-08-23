from __future__ import annotations

import os
from contextlib import asynccontextmanager

import redis.asyncio as redis
from brq.consumer import Consumer
from brq.producer import Producer
from fastapi import Depends, FastAPI, HTTPException, Query, Response
from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession

from moriarty.envs import MORIARTY_DISABLE_CALLBACK_LOG_ENV
from moriarty.log import logger
from moriarty.matrix.operator_.dbutils import get_db_session, open_db_session
from moriarty.matrix.operator_.operator_ import Bridger, get_bridger
from moriarty.matrix.operator_.orm import InferenceLogORM
from moriarty.matrix.operator_.rds import get_redis_client, open_redis_client
from moriarty.sidecar.params import MatrixCallback


def get_callback_consumer(
    redis_client=Depends(get_redis_client),
    bridger=Depends(get_bridger),
    session=Depends(get_db_session),
):
    return CallbackManager(redis_client=redis_client, bridger=bridger, session=session)


class CallbackManager:
    redis_prefix = "moriarty-callback-brq"
    register_function_name = "callback"
    disable_log = os.getenv(MORIARTY_DISABLE_CALLBACK_LOG_ENV)

    def __init__(
        self,
        redis_client: redis.Redis | redis.RedisCluster,
        bridger: Bridger,
        session: AsyncSession,
    ):
        self.redis = redis_client
        self.bridger = bridger
        self.session = session

        self.producer = Producer(
            redis=self.redis,
            redis_prefix=self.redis_prefix,
        )
        self.consumer = Consumer(
            redis=self.redis,
            awaitable_function=self.handle_callback,
            redis_prefix=self.redis_prefix,
            register_function_name=self.register_function_name,
            delete_message_after_process=True,
        )

    async def handle_callback(self, payload: str) -> None:
        callback = MatrixCallback.model_validate_json(payload)
        await self.bridger.bridge_result(callback)
        if not self.disable_log:
            try:
                await self.session.execute(
                    update(InferenceLogORM)
                    .where(InferenceLogORM.inference_id == callback.inference_id)
                    .values(
                        status=callback.status,
                        callback_response=callback.model_dump(),
                        finished_at=func.now(),
                    )
                )
                await self.session.commit()
            except Exception as e:
                logger.exception(e)
                await self.session.rollback()
                raise
        logger.info(f"Callback handled: {callback}")

    async def emit_callback(self, callback: MatrixCallback) -> None:
        await self.producer.run_job(
            function_name=self.register_function_name,
            kwargs={
                "payload": callback.model_dump_json(),
            },
        )

    async def start_consumer(self) -> None:
        await self.consumer.start()

    async def stop_consumer(self) -> None:
        await self.consumer.stop()
