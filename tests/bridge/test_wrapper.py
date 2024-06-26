import pytest

from moriarty import mock
from moriarty.matrix.job_manager.bridge.manager import BridgeManager
from moriarty.matrix.job_manager.bridge_wrapper import BridgeWrapper
from moriarty.matrix.job_manager.params import (
    InferenceJob,
    InferenceResult,
    InferenceResultStatus,
)


@pytest.fixture
def bridge_wrapper():
    bridge_manager = BridgeManager()
    bridge_manager._load_dir(mock)
    return BridgeWrapper(bridge_manager)


async def test_bridge_wrapper(bridge_wrapper: BridgeWrapper):
    bridge = "mock"

    assert bridge_wrapper.get_bridge(bridge) is not None

    assert (
        await bridge_wrapper.enqueue_job(
            bridge,
            "test",
            InferenceJob(
                inference_id="test",
                payload={"input": "test"},
                metadata={"test": "test"},
            ),
        )
    ) == "test"

    assert (
        await bridge_wrapper.dequeue_job(
            bridge,
            "test",
            lambda x: None,
        )
        == 1
    )

    assert (
        await bridge_wrapper.enqueue_job(
            bridge,
            "test",
            InferenceJob(
                inference_id="test",
                payload={"input": "test"},
                metadata={"test": "test"},
            ),
        )
        == "test"
    )

    assert await bridge_wrapper.sample_job(bridge, "test", lambda x: None) == 1

    result = InferenceResult(
        inference_id="test",
        status=InferenceResultStatus.COMPLETED,
        message="Oh yeah",
        payload='{"output": "test"}',
    )
    assert await bridge_wrapper.enqueue_result(
        bridge,
        "test",
        result,
    )

    assert await bridge_wrapper.dequeue_result(
        bridge,
        "test",
        lambda x: None,
    ) == [result]
