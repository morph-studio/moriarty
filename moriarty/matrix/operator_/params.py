from datetime import datetime
from typing import Optional

from moriarty.matrix.operator_.enums_ import MetricType
from moriarty.tools import FlexibleModel


class EndpointRuntimeInfo(FlexibleModel):
    total_replicas_nums: int
    updated_replicas_nums: int
    avaliable_replicas_nums: int
    unavailable_replicas_nums: int


class ResourceScope(FlexibleModel):
    cpu_request: Optional[float] = None
    cpu_limit: Optional[float] = None
    memory_request: Optional[float] = None
    memory_limit: Optional[float] = None
    gpu_nums: Optional[int] = None
    gpu_type: Optional[str] = None


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
    health_check_timeout: Optional[int] = 1200
    health_check_interval: Optional[int] = 5


class CreateEndpointParams(FlexibleModel):
    endpoint_name: str
    image: str
    model_path: Optional[str] = None

    queue_capacity: Optional[int] = 5
    replicas: int = 1
    resource: Optional[ResourceScope] = ResourceScope()
    schedule: Optional[ScheduleScope] = ScheduleScope()
    container: Optional[ContainerScope] = ContainerScope()
    sidecar: Optional[SidecarScope] = SidecarScope()


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
    cursor: Optional[str] = None
    limit: int
    total: int


class UpdateEndpointParams(FlexibleModel):
    image: Optional[str] = None
    model_path: Optional[str] = None

    queue_capacity: Optional[int] = None
    replicas: Optional[int] = None

    resource: Optional[ResourceScope] = None
    schedule: Optional[ScheduleScope] = None
    container: Optional[ContainerScope] = None
    sidecar: Optional[SidecarScope] = None

    need_restart: bool = True


class QueryEndpointAutoscaleResponse(FlexibleModel):
    endpoint_name: str
    min_replicas: int
    max_replicas: int
    scale_in_cooldown: int
    scale_out_cooldown: int
    metrics: MetricType
    metrics_threshold: float


class SetAutoscaleParams(FlexibleModel):
    min_replicas: Optional[int] = None
    max_replicas: Optional[int] = None
    scale_in_cooldown: Optional[int] = None
    scale_out_cooldown: Optional[int] = None
    metrics: Optional[MetricType] = None
    metrics_threshold: Optional[float] = None


class AutoscaleLog(FlexibleModel):
    id_: int
    endpoint_name: str
    old_replicas: int
    new_replicas: int
    metrics: MetricType
    metrics_threshold: float
    metrics_log: dict
    details: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class QueryEndpointAutoscaleLogResponse(FlexibleModel):
    logs: list[AutoscaleLog]
    cursor: Optional[int] = None
    limit: int
    total: int
