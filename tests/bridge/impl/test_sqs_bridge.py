import json
import uuid

import pytest

from moriarty.matrix.job_manager.bridge.impl.sqs_bridge import SQSBridge
from moriarty.matrix.job_manager.params import (
    InferenceJob,
    InferenceResult,
    InferenceResultStatus,
)


@pytest.fixture
def endpoint_name(case_id):
    return f"test-{case_id}"


@pytest.fixture
def priority():
    return 1


@pytest.fixture
def sqs_bridge(case_id, endpoint_name, priority):
    try:
        import boto3

        sqs = boto3.client("sqs")
    except ImportError:
        pytest.skip("boto3 is not installed")
    except Exception:
        pytest.skip("boto3 is not configured")

    job_queue_name = f"moriarty-bridge-{endpoint_name}-{priority}"
    result_queue_name = f"moriarty-bridge-result-{case_id}"
    try:
        job_queue_url = sqs.create_queue(QueueName=job_queue_name)["QueueUrl"]
        result_queue_url = sqs.create_queue(QueueName=result_queue_name)["QueueUrl"]
    except sqs.exceptions.QueueAlreadyExists:
        pass
    except Exception:
        pytest.skip("boto3 is not configured")

    bridge = SQSBridge(
        bridge_result_queue_url=result_queue_url,
    )
    bridge.make_job_queue_url = lambda endpoint_name, priority: job_queue_url
    yield bridge
    try:
        sqs.delete_queue(QueueUrl=job_queue_url)
        sqs.delete_queue(QueueUrl=result_queue_url)
    except sqs.exceptions.QueueDoesNotExist:
        pass


async def test_bridge_job(sqs_bridge: SQSBridge):
    _called = False
    input_job = InferenceJob(
        inference_id="test",
        payload={"input": "test"},
    )
    job = await sqs_bridge.enqueue_job("test", job=input_job)

    assert job

    async def _process_func(job: InferenceJob):
        nonlocal _called
        _called = True
        assert job == input_job

    await sqs_bridge.dequeue_job("test", _process_func)

    assert _called


@pytest.mark.parametrize(
    "inference_result",
    [
        InferenceResult(
            inference_id="test",
            status=InferenceResultStatus.COMPLETED,
            message="Oh yeah",
            payload='{"output": "test"}',
        ),
        InferenceResult(
            inference_id="test",
            status=InferenceResultStatus.FAILED,
            message="Oh no",
            payload='{"error": "test"}',
        ),
    ],
)
async def test_bridge_result(
    sqs_bridge: SQSBridge,
    inference_result: InferenceResult,
    endpoint_name,
    priority,
):
    _called = False

    await sqs_bridge.enqueue_result(inference_result)

    async def _process_func(result: InferenceResult):
        nonlocal _called
        _called = True
        assert result == inference_result

    await sqs_bridge.list_avaliable_priorities(endpoint_name) == [priority]
    await sqs_bridge.dequeue_result(_process_func)

    assert _called

    await sqs_bridge.list_avaliable_priorities(endpoint_name) == []


@pytest.fixture
def result_bucket(monkeypatch, case_id):
    try:
        import boto3

        s3_client = boto3.client("s3")
    except ImportError:
        pytest.skip("boto3 is not installed")
    except Exception:
        pytest.skip("boto3 is not configured")

    bucket_name = f"moriarty-test"
    try:
        # Check if bucket exists
        s3_client.head_bucket(Bucket=bucket_name)
    except Exception as e:
        pytest.skip(f"Bucket not found {e}")
    monkeypatch.setattr(SQSBridge, "output_bucket", bucket_name)
    yield bucket_name


async def test_bridge_big_result(
    sqs_bridge: SQSBridge,
    endpoint_name,
    priority,
    result_bucket,
):
    inference_result = InferenceResult(
        inference_id="test",
        status=InferenceResultStatus.COMPLETED,
        message="Oh Too big",
        payload=json.dumps(
            {
                "output": "1" * 1024 * 1024,  # Bigger than 256KB
            }
        ),
    )

    _called = False

    await sqs_bridge.enqueue_result(inference_result)

    async def _process_func(result: InferenceResult):
        nonlocal _called
        _called = True
        assert result == inference_result

    await sqs_bridge.list_avaliable_priorities(endpoint_name) == [priority]
    await sqs_bridge.dequeue_result(_process_func)

    assert _called

    await sqs_bridge.list_avaliable_priorities(endpoint_name) == []
