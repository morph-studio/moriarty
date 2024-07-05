import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query, Response, status

from moriarty.log import logger
from moriarty.matrix.envs import MORIARTY_MATRIX_TOKEN_ENV
from moriarty.matrix.operator_.autoscaler import (
    AutoscalerManager,
    get_autoscaler_manager,
)
from moriarty.matrix.operator_.operator_ import Operator, get_operaotr
from moriarty.matrix.operator_.params import (
    CreateEndpointParams,
    ListEndpointsResponse,
    QueryEndpointAutoscaleResponse,
    QueryEndpointResponse,
    UpdateEndpointParams,
)
from moriarty.sidecar.params import MatrixCallback

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


@app.post("/endpoint/create")
async def create_endpoint(
    params: CreateEndpointParams,
    operaotr: Operator = Depends(get_operaotr),
):
    await operaotr.create_endpoint(params)
    return Response(status_code=status.HTTP_201_CREATED)


@app.get("/endpoint/list")
async def list_endpoints(
    limit: int = Query(100, ge=1, le=1000),
    cursor: str | None = None,
    keyword: str | None = None,
    order_by: str = Query("created_at", regex="^(name|created_at|updated_at)$"),
    order: str = Query("desc", regex="^(asc|desc)$"),
    operator: Operator = Depends(get_operaotr),
) -> ListEndpointsResponse: ...


@app.get("/endpoint/{endpoint_name}/info")
async def get_endpoint_info(
    endpointd_name: str,
    operator: Operator = Depends(get_operaotr),
) -> QueryEndpointResponse: ...


@app.post("/endpoint/{endpoint_name}/update")
async def update_endpoint(
    endpointd_name: str,
    params: UpdateEndpointParams,
    operator: Operator = Depends(get_operaotr),
) -> QueryEndpointResponse: ...


@app.post("/endpoint/{endpointd_name}/delete")
async def delete_endpoint(
    endpointd_name: str,
    operator: Operator = Depends(get_operaotr),
):
    # await operator.delete_endpoint(endpointd_name)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/autoscale/{endpoint_name}/info")
async def autoscale_endpoint_info(
    endpointd_name: str,
    autoscaler: AutoscalerManager = Depends(get_autoscaler_manager),
) -> QueryEndpointAutoscaleResponse: ...


@app.post("/autoscale/{endpoint_name}/update")
async def autoscale_endpoint(
    endpointd_name: str,
    autoscaler: AutoscalerManager = Depends(get_autoscaler_manager),
) -> QueryEndpointAutoscaleResponse: ...


@app.post("/autoscale/{endpoint_name}/delete")
async def autoscale_endpoint(
    endpointd_name: str,
    autoscaler: AutoscalerManager = Depends(get_autoscaler_manager),
) -> QueryEndpointAutoscaleResponse:
    return Response(status_code=status.HTTP_204_NO_CONTENT)
