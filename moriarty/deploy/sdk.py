from urllib.parse import urljoin

import httpx

from moriarty.log import logger
from moriarty.matrix.operator_.params import (
    ListEndpointsResponse,
    QueryEndpointResponse,
)


def _make_headers(token: str | None) -> dict:
    return {"Authorization": f"Bearer {token}"} if token else dict()


def scan_endpoint(
    api_url: str,
    limit: int = None,
    cursor: str = None,
    keyword: str = None,
    order_by: str = None,
    order: str = None,
    token: str = None,
) -> ListEndpointsResponse:
    response = httpx.get(
        urljoin(api_url, "/endpoint/list"),
        params={
            "limit": limit,
            "cursor": cursor,
            "keyword": keyword,
            "order_by": order_by,
            "order": order,
        },
        headers=_make_headers(token),
        follow_redirects=True,
    )
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        logger.error(f"Request error, response text: {response.text}")
        raise
    return ListEndpointsResponse.model_validate(response.json())


def query_endpoint(
    endpoint_name: str,
    api_url: str,
    token: str = None,
) -> QueryEndpointResponse:
    response = httpx.get(
        urljoin(api_url, f"/endpoint/{endpoint_name}/info"),
        headers=_make_headers(token),
        follow_redirects=True,
    )
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        logger.error(f"Request error, response: {response.text}")
        raise
    return QueryEndpointResponse.model_validate(response.json())


def delete_endpoint(
    endpoint_name: str,
    api_url: str,
    token: str = None,
) -> None:
    response = httpx.post(
        urljoin(api_url, f"/endpoint/{endpoint_name}/delete"),
        headers=_make_headers(token),
        follow_redirects=True,
    )
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        logger.error(f"Request error, response: {response.text}")
        raise

    return None
