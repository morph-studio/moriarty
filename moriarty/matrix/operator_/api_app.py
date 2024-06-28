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


@app.get("/")
async def root():
    return {"message": "Hello World"}
