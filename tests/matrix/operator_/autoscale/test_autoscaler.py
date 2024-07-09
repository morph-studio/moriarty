import pytest
from sqlalchemy import delete

from moriarty.matrix.operator_.autoscaler import AutoscalerManager
from moriarty.matrix.operator_.enums_ import MetricType
from moriarty.matrix.operator_.orm import AutoscaleLogORM, AutoscalerORM, EndpointORM


@pytest.fixture
async def autoscaler(
    async_redis_client,
    async_session,
    spawner,
):
    async def count_pending_jobs(endpoint_name: str) -> int:
        return 10

    autoscaler = AutoscalerManager(
        redis_client=async_redis_client, session=async_session, spawner=spawner
    )
    autoscaler.metrics_manager.count_pending_jobs = count_pending_jobs
    yield autoscaler


@pytest.fixture
def scaleable_endpoint(session):
    endpoint_name = "mock_scaleable_endpoint"

    session.add(
        EndpointORM(
            endpoint_name=endpoint_name,
            image="busybox",
        )
    )
    session.add(
        AutoscalerORM(
            endpoint_name=endpoint_name,
            min_replicas=1,
            max_replicas=10,
            scale_in_cooldown=60,
            scale_out_cooldown=60,
            metrics=MetricType.pending_jobs,
            metrics_threshold=3,
        )
    )
    session.commit()

    yield endpoint_name

    session.execute(delete(EndpointORM).where(EndpointORM.endpoint_name == endpoint_name))
    session.commit()

    session.execute(delete(AutoscalerORM).where(AutoscalerORM.endpoint_name == endpoint_name))
    session.commit()

    session.execute(delete(AutoscaleLogORM).where(AutoscaleLogORM.endpoint_name == endpoint_name))
    session.commit()


async def test_scan_and_scale(autoscaler: AutoscalerManager, scaleable_endpoint):
    await autoscaler.scan_and_scale()

    response = await autoscaler.get_autoscale_logs(
        scaleable_endpoint,
        limit=100,
        cursor=None,
        order_by="created_at",
        order="desc",
    )

    assert len(response.logs) == 1
    assert response.logs[0].old_replicas == 1
    assert response.logs[0].new_replicas == 7  # pending_jobs(10) - metrics_threshold(3) = 7


async def calculate_scale(autoscaler: AutoscalerManager):
    assert autoscaler.calculate_least_replicas(MetricType.pending_jobs, 5, 3) == 2
    assert autoscaler.calculate_least_replicas(MetricType.pending_jobs, 5, 0) == 5
    assert autoscaler.calculate_least_replicas(MetricType.pending_jobs, 5, 0.1) == 5
    assert autoscaler.calculate_least_replicas(MetricType.pending_jobs_per_instance, 10, 3) == 2
    assert autoscaler.calculate_least_replicas(MetricType.pending_jobs_per_instance, 10, 0) == 10
