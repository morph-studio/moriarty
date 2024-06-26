import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query, Response

from moriarty.log import logger


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(lifespan=lifespan)


@app.post("/callback")
async def callback() -> Response:
    return Response(status_code=200)


@app.get("/")
async def root():
    return {"message": "Hello World"}
