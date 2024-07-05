from typing import Optional

from moriarty.matrix.operator_.spawner.plugin import EndpointRuntimeInfo
from moriarty.tools import FlexibleModel


class ResourceScope(FlexibleModel):
    cpu_request: Optional[float] = None
    cpu_limit: Optional[float] = None
    memory_request: Optional[float] = None
    memory_limit: Optional[float] = None
    gpu_nums: Optional[int] = None


class ScheduleScope(FlexibleModel):
    node_labels: Optional[dict] = None
    node_affinity: Optional[dict] = None


class ContainerScope(FlexibleModel):
    environment_variables: Optional[dict] = None
    environment_variables_secret_refs: Optional[list] = None
    commands: Optional[list] = None
    args: Optional[list] = None
    invoke_port: Optional[int] = 8080
    invoke_path: Optional[str] = "/invocations"
    health_check_path: Optional[str] = "/ping"


class SidecarScope(FlexibleModel):
    concurrency: Optional[int] = 1
    process_timeout: Optional[int] = 3600
    healthy_check_timeout: Optional[int] = 1200
    healthy_check_interval: Optional[int] = 5


class CreateEndpointParams(FlexibleModel):
    endpoint_name: str
    image: str
    model_path: Optional[str] = None

    queue_capacity: Optional[int] = 5
    replicas: int = 1
    resource: Optional[ResourceScope] = ResourceScope()
    schedule: Optional[ScheduleScope] = ScheduleScope()
    container: Optional[ContainerScope] = ContainerScope()


class QueryEndpointResponse(FlexibleModel):
    endpoint_name: str
    image: str
    model_path: Optional[str]
    queue_capacity: Optional[int]
    replicas: int
    resource: ResourceScope
    schedule: ScheduleScope
    container: ContainerScope
    runtime: EndpointRuntimeInfo


class ListEndpointsResponse(FlexibleModel):
    endpoints: list[QueryEndpointResponse]
    cursor: str
    limit: int
    total: int


class UpdateEndpointParams(FlexibleModel):
    image: Optional[str] = None
    model_path: Optional[str] = None

    queue_capacity: Optional[int] = None
    replicas: Optional[int] = None
    resource: Optional[ResourceScope] = ResourceScope()
    schedule: Optional[ScheduleScope] = ScheduleScope()
    container: Optional[ContainerScope] = ContainerScope()


class QueryEndpointAutoscaleResponse(FlexibleModel):
    pass


class SetAutoscaleParams(FlexibleModel):
    pass
