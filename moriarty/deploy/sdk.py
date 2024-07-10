from urllib.parse import urljoin

import httpx

from moriarty.log import logger
from moriarty.matrix.operator_.enums_ import MetricType
from moriarty.matrix.operator_.params import (
    ListEndpointsResponse,
    QueryEndpointAutoscaleLogResponse,
    QueryEndpointAutoscaleResponse,
    QueryEndpointResponse,
    SetAutoscaleParams,
)


def _make_headers(token: str | None) -> dict:
    return {"Authorization": f"Bearer {token}"} if token else dict()


def request_scan_endpoint(
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
            k: v
            for k, v in {
                "limit": limit,
                "cursor": cursor,
                "keyword": keyword,
                "order_by": order_by,
                "order": order,
            }.items()
            if v is not None
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


def request_scan_autoscale_log(
    api_url: str,
    endpoint_name: str,
    limit: int = None,
    cursor: int = None,
    keyword: str = None,
    order_by: str = None,
    order: str = None,
    token: str = None,
) -> QueryEndpointAutoscaleLogResponse:
    response = httpx.get(
        urljoin(api_url, f"/autoscale/{endpoint_name}/log"),
        params={
            k: v
            for k, v in {
                "limit": limit,
                "cursor": cursor,
                "keyword": keyword,
                "order_by": order_by,
                "order": order,
            }.items()
            if v is not None
        },
        headers=_make_headers(token),
        follow_redirects=True,
    )
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        logger.error(f"Request error, response text: {response.text}")
        raise
    return QueryEndpointAutoscaleLogResponse.model_validate(response.json())


def request_query_endpoint(
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


def request_delete_endpoint(
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
        if response.status_code == 404:
            return None

        logger.error(f"Request error, response: {response.text}")
        raise

    return None


def request_delete_autoscale(
    endpoint_name: str,
    api_url: str,
    token: str = None,
) -> None:
    response = httpx.post(
        urljoin(api_url, f"/autoscale/{endpoint_name}/delete"),
        headers=_make_headers(token),
        follow_redirects=True,
    )
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        if response.status_code == 404:
            return None

        logger.error(f"Request error, response: {response.text}")
        raise

    return None


def request_query_autoscale(
    endpoint_name: str,
    api_url: str,
    token: str = None,
) -> QueryEndpointAutoscaleResponse | None:
    response = httpx.get(
        urljoin(api_url, f"/endpoint/{endpoint_name}/autoscale"),
        headers=_make_headers(token),
        follow_redirects=True,
    )
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return None

        logger.error(f"Request error, response: {response.text}")
        raise
    return QueryEndpointAutoscaleResponse.model_validate(response.json())


def request_set_autoscale(
    endpoint_name: str,
    api_url: str,
    min_replicas: int,
    max_replicas: int,
    scale_in_cooldown: int,
    scale_out_cooldown: int,
    metrics: MetricType,
    metrics_threshold: float,
    token: str = None,
) -> None:
    response = httpx.post(
        urljoin(api_url, f"/autoscale/{endpoint_name}/set"),
        headers=_make_headers(token),
        data=SetAutoscaleParams(
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            scale_in_cooldown=scale_in_cooldown,
            scale_out_cooldown=scale_out_cooldown,
            metrics=metrics,
            metrics_threshold=metrics_threshold,
        ).model_dump_json(),
        follow_redirects=True,
    )
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        logger.error(f"Request error, response: {response.text}")
        raise
    return QueryEndpointResponse.model_validate(response.json())
