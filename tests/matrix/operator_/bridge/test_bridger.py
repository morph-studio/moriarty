import pytest
from click.testing import CliRunner

from moriarty.matrix.job_manager.bridge_wrapper import BridgeWrapper
from moriarty.matrix.job_manager.params import InferenceJob
from moriarty.matrix.operator_.cli import drop
from moriarty.matrix.operator_.operator_ import Bridger
from moriarty.matrix.operator_.orm import EndpointORM


@pytest.fixture
async def mock_endpoint(async_session):
    runner = CliRunner()
    # Drop all before testing
    result = runner.invoke(drop, ["--yes"])
    assert result.exit_code == 0

    name = "mock"

    async_session.add(
        EndpointORM(
            endpoint_name=name,
        )
    )
    await async_session.commit()
    return name


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

    await bridger.bridge_all()

    assert await bridger.job_producer.count_jobs(mock_endpoint) == 1
