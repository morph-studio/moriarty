import os
from datetime import datetime
from urllib.parse import urljoin

import httpx

from moriarty.envs import MORIARTY_CONNECTOR_ENDPOINT_ENV
from moriarty.log import logger
from moriarty.matrix.connector.params import InvokeParams


async def invoke(
    endpoint_name: str,
    invoke_params: dict,
    inference_id: str = None,
    priority: int = None,
    created_at: datetime | int = None,
    expires_at: datetime | int = None,
    endpoint: str = os.getenv(MORIARTY_CONNECTOR_ENDPOINT_ENV, "http://moriarty-connector:8901"),
    *,
    async_client=httpx.AsyncClient,
    **client_kwargs,
) -> httpx.Response:
    """
    Invoke an inference endpoint.

    Response: httpx.Response
    """
    url = urljoin(endpoint, "/invoke")
    if expires_at and isinstance(expires_at, int):
        expires_at = datetime.fromtimestamp(expires_at)

    if created_at and isinstance(created_at, int):
        created_at = datetime.fromtimestamp(created_at)

    payload = InvokeParams(
        inference_id=inference_id,
        endpoint_name=endpoint_name,
        invoke_params=invoke_params,
        priority=priority,
        created_at=created_at or datetime.now(),
        expires_at=expires_at,
    )
    async with async_client(
        follow_redirects=True,
    ) as client:
        logger.debug(f"Invoke endpoint: {url} with payload: {payload}")
        return await client.post(
            url,
            data=payload.model_dump_json(),
            timeout=60,
            **client_kwargs,
        )
