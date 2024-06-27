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

from moriarty.matrix.operator_.enums_ import EndpointMetrics
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

    # Container spec
    image = Column(String(255), nullable=True)
    replicas = Column(Integer, nullable=False, default=1)
    cpu_request = Column(Float, nullable=False, default=0.1)
    cpu_limit = Column(Float, nullable=False, default=0.1)
    memory_request = Column(Float, nullable=False, default=128.0)
    memory_limit = Column(Float, nullable=False, default=128.0)
    gpu = Column(Integer, nullable=False, default=0)
    gpu_request = Column(Float, nullable=False, default=0.1)

    # Schedule
    node_labels = Column(JSON, nullable=False, default={})
    node_affinity = Column(JSON, nullable=False, default={})

    # Config for sidecar
    invoke_port = Column(Integer, nullable=False, default=8080)
    invoke_path = Column(String(255), nullable=False, default="/invocations")
    health_check_url = Column(String(255), nullable=False, default="/ping")
    callback_token = Column(String(255), nullable=False, default=lambda: token_urlsafe(16))
    concurrency = Column(Integer, nullable=False, default=1)
    process_timeout = Column(Integer, nullable=False, default=3600)
    healthy_check_timeout = Column(Integer, nullable=False, default=1200)
    healthy_check_interval = Column(Integer, nullable=False, default=5)


class InferenceLogORM(Base):
    __tablename__ = "moriarty_inference_logs"
    id_ = Column(Integer, autoincrement=True, primary_key=True)
    inference_id = Column(String(255), nullable=False, index=True, default=lambda: uuid.uuid4().hex)
    endpoint_name = Column(String(255), nullable=False, index=True)
    payload = Column(JSON, nullable=False)

    status = Column(String(255), nullable=False, default=InferenceProxyStatus.RUNNING)
    callback_response = Column(JSON, nullable=False, default={})
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

    metrics = Column(String, nullable=False, default=EndpointMetrics.pendding_jobs_per_instance)
    metrics_threshold = Column(Float, nullable=False, default=3)

    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
        comment="Updated at",
    )
