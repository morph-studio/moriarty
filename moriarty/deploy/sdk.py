from urllib.parse import urljoin

import httpx

from moriarty.log import logger
from moriarty.matrix.operator_.enums_ import MetricType
from moriarty.matrix.operator_.params import (
    ContainerScope,
    CreateEndpointParams,
    ListEndpointsResponse,
    QueryEndpointAutoscaleLogResponse,
    QueryEndpointAutoscaleResponse,
    QueryEndpointResponse,
    ResourceScope,
    ScheduleScope,
    SetAutoscaleParams,
    SidecarScope,
    UpdateEndpointParams,
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


def request_exist_endpoint(
    endpoint_name: str,
    api_url: str,
    token: str = None,
) -> bool:
    try:
        request_query_endpoint(
            endpoint_name=endpoint_name,
            api_url=api_url,
            token=token,
        )
        return True
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return False
        raise


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
        logger.info(f"Response status {response.status_code}, response: {response.text}")
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

        logger.info(f"Response status {response.status_code}, response: {response.text}")
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

        logger.info(f"Response status {response.status_code}, response: {response.text}")
        raise

    return None


def request_query_autoscale(
    endpoint_name: str,
    api_url: str,
    token: str = None,
) -> QueryEndpointAutoscaleResponse | None:
    response = httpx.get(
        urljoin(api_url, f"/autoscale/{endpoint_name}/info"),
        headers=_make_headers(token),
        follow_redirects=True,
    )
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return None

        logger.info(f"Response status {response.status_code}, response: {response.text}")
        raise
    return QueryEndpointAutoscaleResponse.model_validate(response.json())


def request_set_autoscale(
    endpoint_name: str,
    api_url: str,
    min_replicas: int,
    max_replicas: int,
    scale_in_cooldown: int | None,
    scale_out_cooldown: int | None,
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
        logger.info(f"Response status {response.status_code}, response: {response.text}")
        raise
    return QueryEndpointAutoscaleResponse.model_validate(response.json())


def request_create_endpoint_with_params(
    api_url,
    params: CreateEndpointParams,
    token: str = None,
) -> None:
    response = httpx.post(
        urljoin(api_url, "/endpoint/create"),
        headers=_make_headers(token),
        data=params.model_dump_json(),
        follow_redirects=True,
    )
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        logger.info(f"Response status {response.status_code}, response: {response.text}")
        raise
    return None


def request_update_endpoint_with_params(
    api_url,
    endpoint_name: str,
    params: UpdateEndpointParams,
    token: str = None,
) -> QueryEndpointResponse:
    response = httpx.post(
        urljoin(api_url, f"/endpoint/{endpoint_name}/update"),
        headers=_make_headers(token),
        data=params.model_dump_json(),
        follow_redirects=True,
    )
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        logger.info(f"Response status {response.status_code}, response: {response.text}")
        raise
    return QueryEndpointResponse.model_validate(response.json())


def request_create_endpoint(
    api_url: str,
    endpoint_name: str,
    image: str,
    model_path: str = None,
    queue_capacity: int = None,
    replicas: int = None,
    # ResourceScope
    cpu_request: float = None,
    cpu_limit: float = None,
    memory_request: float = None,
    memory_limit: float = None,
    gpu_nums: int = None,
    gpu_type: str = None,
    # ScheduleScope
    node_labels: dict[str, str] = None,
    node_selector: dict[str, str] = None,
    # ContainerScope
    environment_variables: dict[str, str] = None,
    environment_variables_secret_refs: list[str] = None,
    commands: list[str] = None,
    args: list[str] = None,
    invoke_port: int = None,
    invoke_path: str = None,
    health_check_path: str = None,
    # SidecarScope
    concurrency: int = None,
    process_timeout: int = None,
    health_check_timeout: int = None,
    health_check_interval: int = None,
    token: str = None,
) -> None:
    response = httpx.post(
        urljoin(api_url, f"/endpoint/create"),
        headers=_make_headers(token),
        data=CreateEndpointParams(
            **{
                k: v
                for k, v in dict(
                    endpoint_name=endpoint_name,
                    image=image,
                    model_path=model_path,
                    queue_capacity=queue_capacity,
                    replicas=replicas,
                ).items()
                if v is not None
            },
            resource=ResourceScope(
                **{
                    k: v
                    for k, v in dict(
                        cpu_request=cpu_request,
                        cpu_limit=cpu_limit,
                        memory_request=memory_request,
                        memory_limit=memory_limit,
                        gpu_nums=gpu_nums,
                        gpu_type=gpu_type,
                    ).items()
                    if v is not None
                }
            ),
            schedule=ScheduleScope(
                **{
                    k: v
                    for k, v in dict(
                        node_labels=node_labels,
                        node_selector=node_selector,
                    ).items()
                    if v is not None
                }
            ),
            container=ContainerScope(
                **{
                    k: v
                    for k, v in dict(
                        environment_variables=environment_variables,
                        environment_variables_secret_refs=environment_variables_secret_refs,
                        commands=commands,
                        args=args,
                        invoke_port=invoke_port,
                        invoke_path=invoke_path,
                        health_check_path=health_check_path,
                    ).items()
                    if v is not None
                }
            ),
            sidecar=SidecarScope(
                **{
                    k: v
                    for k, v in dict(
                        concurrency=concurrency,
                        process_timeout=process_timeout,
                        health_check_timeout=health_check_timeout,
                        health_check_interval=health_check_interval,
                    ).items()
                    if v is not None
                }
            ),
        ).model_dump_json(),
        follow_redirects=True,
    )
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        logger.info(f"Response status {response.status_code}, response: {response.text}")
        raise

    return


def request_update_endpoint(
    api_url: str,
    endpoint_name: str,
    image: str = None,
    model_path: str = None,
    queue_capacity: int = None,
    replicas: int = None,
    need_restart: bool = None,
    # ResourceScope
    cpu_request: float = None,
    cpu_limit: float = None,
    memory_request: float = None,
    memory_limit: float = None,
    gpu_nums: int = None,
    gpu_type: str = None,
    # ScheduleScope
    node_labels: dict[str, str] = None,
    node_selector: dict[str, str] = None,
    # ContainerScope
    environment_variables: dict[str, str] = None,
    environment_variables_secret_refs: list[str] = None,
    commands: list[str] = None,
    args: list[str] = None,
    invoke_port: int = None,
    invoke_path: str = None,
    health_check_path: str = None,
    # SidecarScope
    concurrency: int = None,
    process_timeout: int = None,
    health_check_timeout: int = None,
    health_check_interval: int = None,
    token: str = None,
) -> QueryEndpointResponse:

    response = httpx.post(
        urljoin(api_url, f"/endpoint/{endpoint_name}/update"),
        headers=_make_headers(token),
        data=UpdateEndpointParams(
            **{
                k: v
                for k, v in dict(
                    image=image,
                    model_path=model_path,
                    queue_capacity=queue_capacity,
                    replicas=replicas,
                    need_restart=need_restart,
                ).items()
                if v is not None
            },
            resource=ResourceScope(
                **{
                    k: v
                    for k, v in dict(
                        cpu_request=cpu_request,
                        cpu_limit=cpu_limit,
                        memory_request=memory_request,
                        memory_limit=memory_limit,
                        gpu_nums=gpu_nums,
                        gpu_type=gpu_type,
                    ).items()
                    if v is not None
                }
            ),
            schedule=ScheduleScope(
                **{
                    k: v
                    for k, v in dict(
                        node_labels=node_labels,
                        node_selector=node_selector,
                    ).items()
                    if v is not None
                }
            ),
            container=ContainerScope(
                **{
                    k: v
                    for k, v in dict(
                        environment_variables=environment_variables,
                        environment_variables_secret_refs=environment_variables_secret_refs,
                        commands=commands,
                        args=args,
                        invoke_port=invoke_port,
                        invoke_path=invoke_path,
                        health_check_path=health_check_path,
                    ).items()
                    if v is not None
                }
            ),
            sidecar=SidecarScope(
                **{
                    k: v
                    for k, v in dict(
                        concurrency=concurrency,
                        process_timeout=process_timeout,
                        health_check_timeout=health_check_timeout,
                        health_check_interval=health_check_interval,
                    ).items()
                    if v is not None
                }
            ),
        ).model_dump_json(),
        follow_redirects=True,
    )
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        logger.info(f"Response status {response.status_code}, response: {response.text}")
        raise

    return QueryEndpointResponse.model_validate(response.json())
