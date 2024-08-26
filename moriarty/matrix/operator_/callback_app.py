from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query, Response

from moriarty.envs import get_bridge_name, get_bridge_result_queue_url, get_spawner_name
from moriarty.log import logger
from moriarty.matrix.job_manager.bridge_wrapper import (
    get_bridge_manager,
    get_bridge_wrapper,
)
from moriarty.matrix.operator_.callback import CallbackManager, get_callback_consumer
from moriarty.matrix.operator_.config import get_config
from moriarty.matrix.operator_.dbutils import open_db_session
from moriarty.matrix.operator_.operator_ import get_bridger
from moriarty.matrix.operator_.rds import open_redis_client
from moriarty.matrix.operator_.spawner.manager import get_spawner, get_spawner_manager
from moriarty.sidecar.params import MatrixCallback


@asynccontextmanager
async def lifespan(app: FastAPI):
    get_spawner_dep = app.dependency_overrides.get(get_spawner, get_spawner)
    get_spawner_name_dep = app.dependency_overrides.get(get_spawner_name, get_spawner_name)
    get_spawner_manager_dep = app.dependency_overrides.get(get_spawner_manager, get_spawner_manager)

    spawner = await get_spawner_dep(
        spawner_name=get_spawner_name_dep(),
        spawner_manager=get_spawner_manager_dep(),
    )

    get_bridger_dep = app.dependency_overrides.get(get_bridger, get_bridger)
    get_bridge_name_dep = app.dependency_overrides.get(get_bridge_name, get_bridge_name)
    get_bridge_wrapper_dep = app.dependency_overrides.get(get_bridge_wrapper, get_bridge_wrapper)
    get_bridge_manager_dep = app.dependency_overrides.get(get_bridge_manager, get_bridge_manager)
    get_bridge_result_queue_url_dep = app.dependency_overrides.get(
        get_bridge_result_queue_url, get_bridge_result_queue_url
    )
    get_callback_consumer_dep = app.dependency_overrides.get(
        get_callback_consumer, get_callback_consumer
    )
    config = get_config()

    async with open_db_session(config, {"pool_pre_ping": True, "pool_recycle": 600}) as session:
        async with open_redis_client(config) as redis_client:
            bridger = get_bridger_dep(
                spawner=spawner,
                bridge_name=get_bridge_name_dep(),
                bridge_wrapper=get_bridge_wrapper_dep(bridge_manager=get_bridge_manager_dep()),
                redis_client=redis_client,
                session=session,
                bridge_result_queue_url=get_bridge_result_queue_url_dep(),
            )

            c: CallbackManager = get_callback_consumer_dep(
                redis_client=redis_client,
                bridger=bridger,
                session=session,
            )
            await c.start_consumer()
            yield
            await c.stop_consumer()


app = FastAPI(lifespan=lifespan)


@app.post("/callback")
async def callback(
    callback: MatrixCallback,
    cm: CallbackManager = Depends(get_callback_consumer),
) -> Response:
    logger.debug(f"Received callback: {callback}")
    await cm.emit_callback(callback)
    return Response(status_code=200)


@app.get("/")
async def root():
    return {"message": "Hello World"}
