import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query

from moriarty.log import logger
from moriarty.matrix.connector.invoker import Invoker, get_invoker
from moriarty.matrix.connector.params import InvokeParams, InvokeResponse

"""
TODO: Switch to oauth and other authentication
"""

TOKEN = os.getenv("MORIARTY_CONNECTOR_TOKEN")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if TOKEN:
        logger.info(f"Token: {TOKEN}")
    else:
        logger.info("Token not set, authentication is disabled")

    yield


app = FastAPI(lifespan=lifespan)


@app.post("/invoke")
async def invoke(
    params: InvokeParams,
    invoker: Invoker = Depends(get_invoker),
    token: str = Query(None),
) -> InvokeResponse:
    if TOKEN and token != TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized, check token")
    return await invoker.invoke(params)


@app.get("/")
async def root():
    return {"message": "Hello World"}
