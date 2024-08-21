import pytest
from click.testing import CliRunner
from sqlalchemy import delete

from moriarty.matrix.job_manager.bridge_wrapper import BridgeWrapper
from moriarty.matrix.job_manager.params import InferenceJob
from moriarty.matrix.operator_.cli import drop
from moriarty.matrix.operator_.operator_ import Bridger
from moriarty.matrix.operator_.orm import EndpointORM


@pytest.fixture
async def mock_endpoint(async_session):
    name = "mock"
    async_session.add(
        EndpointORM(
            endpoint_name=name,
        )
    )
    await async_session.commit()
    yield name

    async_session.execute(delete(EndpointORM).where(EndpointORM.endpoint_name == name))
    await async_session.commit()


async def test_bridger(bridger: Bridger, bridge_wrapper: BridgeWrapper, mock_endpoint):
    await bridge_wrapper.enqueue_job(
        bridge=bridger.bridge_name,
        endpoint_name=mock_endpoint,
        job=InferenceJob(
            inference_id="mock",
            payload={
                "data": "mock",
            },
        ),
    )
    await bridge_wrapper.enqueue_job(
        bridge=bridger.bridge_name,
        endpoint_name=mock_endpoint,
        job=InferenceJob(
            inference_id="mock2",
            payload={
                "data": "mock2",
            },
        ),
    )
    await bridger.bridge_all()

    assert await bridger.job_producer.count_jobs(mock_endpoint) == 2
