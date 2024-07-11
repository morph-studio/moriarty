from typing import Optional

from pydantic import BaseModel

from moriarty.matrix.operator_.enums_ import MetricType
from moriarty.matrix.operator_.params import (
    ContainerScope,
    ResourceScope,
    ScheduleScope,
    SidecarScope,
)
from moriarty.tools import FlexibleModel


class Endpoint(FlexibleModel):
    endpoint_name: str
    image: str
    model_path: Optional[str] = None

    queue_capacity: Optional[int] = 5
    replicas: int = 1
    resource: Optional[ResourceScope] = ResourceScope()
    schedule: Optional[ScheduleScope] = ScheduleScope()
    container: Optional[ContainerScope] = ContainerScope()
    sidecar: Optional[SidecarScope] = SidecarScope()

    min_replicas: Optional[int] = None
    max_replicas: Optional[int] = None
    scale_in_cooldown: Optional[int] = None
    scale_out_cooldown: Optional[int] = None
    metrics: Optional[MetricType] = None
    metrics_threshold: Optional[float] = None

    @property
    def config_autoscale(self) -> bool:
        return any(
            [
                self.min_replicas is not None,
                self.max_replicas is not None,
                self.scale_in_cooldown is not None,
                self.scale_out_cooldown is not None,
                self.metrics is not None,
                self.metrics_threshold is not None,
            ]
        )
