import enum
import uuid
from secrets import token_urlsafe

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKeyConstraint,
    Identity,
    Integer,
    String,
    Text,
    Unicode,
    UniqueConstraint,
    func,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import declarative_base

from moriarty.matrix.operator_.enums_ import MetricType
from moriarty.sidecar.params import InferenceProxyStatus

Base = declarative_base()


class EndpointORM(Base):
    __tablename__ = "moriarty_endpoints"

    id_ = Column(Integer, autoincrement=True, primary_key=True)

    # Medatata
    endpoint_name = Column(String(255), unique=True, nullable=False, index=True)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
        comment="Updated at",
    )
    queue_capacity = Column(Integer, nullable=False, default=5)
    image = Column(String(255), nullable=True, default="wh1isper/moriarty-compute-image:latest")
    model_path = Column(
        Text,
        nullable=True,
        comment="Model path for init container to download from",
    )
    replicas = Column(
        Integer,
        nullable=False,
        default=1,
        comment="Number of replicas, autoscaler will also change it if enabled",
    )

    # Resource spec
    cpu_request = Column(Float, nullable=False, default=0.1, comment="CPU request in cores")
    cpu_limit = Column(Float, nullable=True, comment="CPU limit in milli")
    memory_request = Column(Float, nullable=False, default=128.0, comment="Memory request in MB")
    memory_limit = Column(Float, nullable=True, comment="Memory limit in MB")
    gpu_nums = Column(Integer, nullable=False, default=0)
    gpu_type = Column(String(255), nullable=False, default="nvidia.com/gpu")

    # Config for container
    environment_variables = Column(JSON, nullable=False, default={})
    environment_variables_secret_refs = Column(JSON, nullable=False, default=[])
    commands = Column(JSON, nullable=False, default=[])
    args = Column(JSON, nullable=False, default=[])
    invoke_port = Column(Integer, nullable=False, default=8080)
    invoke_path = Column(String(255), nullable=False, default="/invocations")
    health_check_path = Column(String(255), nullable=False, default="/ping")
    allow_retry = Column(Boolean, nullable=False, default=False)
    # Schedule
    node_labels = Column(JSON, nullable=False, default={})
    node_affinity = Column(JSON, nullable=False, default={})

    # Config for sidecar
    concurrency = Column(Integer, nullable=False, default=1)
    process_timeout = Column(Integer, nullable=False, default=3600)
    health_check_timeout = Column(Integer, nullable=False, default=1200)
    health_check_interval = Column(Integer, nullable=False, default=5)


class InferenceLogORM(Base):
    __tablename__ = "moriarty_inference_logs"
    id_ = Column(Integer, autoincrement=True, primary_key=True)
    inference_id = Column(String(255), nullable=False, index=True, default=lambda: uuid.uuid4().hex)
    endpoint_name = Column(String(255), nullable=False, index=True)
    inference_job = Column(JSON, nullable=False)

    status = Column(String(255), nullable=False, default=InferenceProxyStatus.RUNNING)
    callback_response = Column(JSON, nullable=True)
    finished_at = Column(DateTime, nullable=True)

    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
        comment="Updated at",
    )


class AutoscalerORM(Base):
    __tablename__ = "moriarty_autoscalers"

    id_ = Column(Integer, autoincrement=True, primary_key=True)
    endpoint_name = Column(String(255), unique=True, nullable=False, index=True)
    min_replicas = Column(Integer, nullable=False, default=1)
    max_replicas = Column(Integer, nullable=False, default=1)
    scale_in_cooldown = Column(Integer, nullable=False, default=60)
    scale_out_cooldown = Column(Integer, nullable=False, default=60)

    metrics = Column(String, nullable=False, default=MetricType.pending_jobs_per_instance)
    metrics_threshold = Column(Float, nullable=False, default=3)

    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
        comment="Updated at",
    )


class AutoscaleLogORM(Base):
    __tablename__ = "moriarty_autoscale_logs"

    id_ = Column(Integer, autoincrement=True, primary_key=True)
    endpoint_name = Column(String(255), nullable=False, index=True)
    old_replicas = Column(Integer, nullable=False)
    new_replicas = Column(Integer, nullable=False)
    metrics = Column(String, nullable=False)
    metrics_threshold = Column(Float, nullable=False)
    metrics_log = Column(JSON, nullable=False)
    details = Column(String, nullable=True)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
        comment="Updated at",
    )
