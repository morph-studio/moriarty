import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query, Response, status

from moriarty.envs import MORIARTY_MATRIX_TOKEN_ENV
from moriarty.log import logger
from moriarty.matrix.operator_.autoscaler import (
    AutoscalerManager,
    get_autoscaler_manager,
)
from moriarty.matrix.operator_.operator_ import Operator, get_operaotr
from moriarty.matrix.operator_.params import (
    CreateEndpointParams,
    ListEndpointsResponse,
    QueryEndpointAutoscaleLogResponse,
    QueryEndpointAutoscaleResponse,
    QueryEndpointResponse,
    SetAutoscaleParams,
    UpdateEndpointParams,
)

TOKEN = os.getenv(MORIARTY_MATRIX_TOKEN_ENV)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if TOKEN:
        logger.info(f"Token: {TOKEN}")
    else:
        logger.info("Token not set, authentication is disabled")
    yield


app = FastAPI(lifespan=lifespan)


@app.middleware("http")
async def verify_token(request, call_next):
    if request.url.path == "/":
        return await call_next(request)

    if TOKEN and request.headers.get("Authorization") != f"Bearer {TOKEN}":
        return Response(
            status_code=401,
            content=f"Unauthorized. Check environment {MORIARTY_MATRIX_TOKEN_ENV} for token.",
        )
    return await call_next(request)


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/endpoint/list")
async def list_endpoints(
    limit: int = Query(100, ge=1, le=1000),
    cursor: str | None = None,
    keyword: str | None = None,
    orderBy: str = Query("created_at", pattern="^(created_at|updated_at|endpoint_name)$"),
    order: str = Query("desc", pattern="^(asc|desc)$"),
    operator: Operator = Depends(get_operaotr),
) -> ListEndpointsResponse:
    return await operator.list_endpoints(
        limit=limit,
        cursor=cursor,
        keyword=keyword,
        order_by=orderBy,
        order=order,
    )


@app.post("/endpoint/create")
async def create_endpoint(
    params: CreateEndpointParams,
    operaotr: Operator = Depends(get_operaotr),
):
    await operaotr.create_endpoint(params)
    return Response(status_code=status.HTTP_201_CREATED)


@app.get("/endpoint/{endpoint_name}/info")
async def get_endpoint_info(
    endpoint_name: str,
    operator: Operator = Depends(get_operaotr),
) -> QueryEndpointResponse:
    return await operator.get_endpoint_info(endpoint_name)


@app.post("/endpoint/{endpoint_name}/update")
async def update_endpoint(
    endpoint_name: str,
    params: UpdateEndpointParams,
    operator: Operator = Depends(get_operaotr),
) -> QueryEndpointResponse:
    return await operator.update_endpoint(endpoint_name, params)


@app.post("/endpoint/{endpoint_name}/delete")
async def delete_endpoint(
    endpoint_name: str,
    operator: Operator = Depends(get_operaotr),
):
    await operator.delete_endpoint(endpoint_name)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/autoscale/{endpoint_name}/info")
async def autoscale_endpoint_info(
    endpoint_name: str,
    autoscaler: AutoscalerManager = Depends(get_autoscaler_manager),
) -> QueryEndpointAutoscaleResponse:
    return await autoscaler.get_autoscale_info(endpoint_name)


@app.post("/autoscale/{endpoint_name}/set")
async def set_autoscale(
    endpoint_name: str,
    params: SetAutoscaleParams,
    autoscaler: AutoscalerManager = Depends(get_autoscaler_manager),
) -> QueryEndpointAutoscaleResponse:
    return await autoscaler.set_autoscale(endpoint_name, params)


@app.post("/autoscale/{endpoint_name}/delete")
async def delete_autoscale(
    endpoint_name: str,
    autoscaler: AutoscalerManager = Depends(get_autoscaler_manager),
) -> QueryEndpointAutoscaleResponse:
    await autoscaler.delete(endpoint_name)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/autoscale/{endpoint_name}/log")
async def get_autoscale_log(
    endpoint_name: str,
    limit: int = Query(100, ge=1, le=1000),
    cursor: int | None = None,
    orderBy: str = Query("created_at", pattern="^(created_at|updated_at|endpoint_name)$"),
    order: str = Query("desc", pattern="^(asc|desc)$"),
    autoscaler: AutoscalerManager = Depends(get_autoscaler_manager),
) -> QueryEndpointAutoscaleLogResponse:
    return await autoscaler.get_autoscale_logs(
        endpoint_name, limit=limit, cursor=cursor, order_by=orderBy, order=order
    )
