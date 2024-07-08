import pytest

from moriarty.matrix.operator_.autoscaler import AutoscalerManager


@pytest.fixture
async def autoscaler(
    async_redis_client,
    async_session,
    spawner,
):
    return AutoscalerManager(
        redis_client=async_redis_client, session=async_session, spawner=spawner
    )


@pytest.fixture
def scaleable_endpoint(session):
    endpoint_name = "mock_scaleable_endpoint"

    yield endpoint_name


async def test_scan_and_scale(autoscaler, scaleable_endpoint):
    pass
