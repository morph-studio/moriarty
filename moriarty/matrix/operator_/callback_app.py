import os
from contextlib import asynccontextmanager

import redis.asyncio as redis
from brq.consumer import Consumer
from brq.producer import Producer
from fastapi import Depends, FastAPI, HTTPException, Query, Response
from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession

from moriarty.log import logger
from moriarty.matrix.operator_.dbutils import get_db_session
from moriarty.matrix.operator_.operator_ import (
    Bridger,
    Operator,
    get_bridger,
    get_operaotr,
)
from moriarty.matrix.operator_.orm import InferenceLogORM
from moriarty.matrix.operator_.rds import get_redis_client
from moriarty.sidecar.params import MatrixCallback


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(lifespan=lifespan)


def get_callback_consumer(
    redis_client=Depends(get_redis_client),
    bridger=Depends(get_bridger),
    session=Depends(get_db_session),
):
    return CallbackManager(redis_client=redis_client, bridger=bridger, session=session)


class CallbackManager:
    redis_prefix = "moriarty-callback-brq"

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
            redis_client=self.redis,
            prefix=self.redis_prefix,
        )

    async def _handle_callback(self, callback: MatrixCallback) -> None:
        await self.bridger.bridge_result(callback)
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
        logger.info(f"Callback handled: {callback}")

    async def emit_callback(self, callback: MatrixCallback) -> None:
        pass


@app.post("/callback")
async def callback(
    callback: MatrixCallback,
    cm: CallbackManager = Depends(get_callback_consumer),
) -> Response:
    await cm.emit_callback(callback)
    return Response(status_code=200)


@app.get("/")
async def root():
    return {"message": "Hello World"}
