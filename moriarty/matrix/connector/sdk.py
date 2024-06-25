import os

import httpx


async def invoke(
    endpoint: str = os.getenv("MORIARTY_CONNECTOR_ENDPOINT", "http://moriarty-connector:8901"),
    *,
    async_client=httpx.AsyncClient,
):
    pass
