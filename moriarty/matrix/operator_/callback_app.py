import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query, Response

from moriarty.log import logger
from moriarty.matrix.operator_.operator_ import Operator, get_operaotr
from moriarty.sidecar.params import MatrixCallback


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(lifespan=lifespan)


@app.post("/callback")
async def callback(
    callback: MatrixCallback,
    operator: Operator = Depends(get_operaotr),
) -> Response:
    await operator.handle_callback(callback)
    return Response(status_code=200)


@app.get("/")
async def root():
    return {"message": "Hello World"}
