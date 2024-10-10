import json
import uuid

import pytest

from moriarty.matrix.job_manager.bridge.impl.pubsub_bridge import PubSubBridge
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

@pytest.fixture(scope="session")
def gcp_credentials():
    import os
    credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials:
        pytest.skip("GOOGLE_APPLICATION_CREDENTIALS is not set.")
    return credentials

@pytest.fixture(scope="session")
def project_id():
    return "plasma-cascade-431107-u4"

@pytest.fixture
def pubsub_bridge(case_id, endpoint_name, priority, project_id):
    try:
        from google.cloud import pubsub_v1

        publisher = pubsub_v1.PublisherClient()
        subscriber = pubsub_v1.SubscriberClient()
    except ImportError:
        pytest.skip("google-cloud-pubsub is not installed")
    except Exception:
        pytest.skip("pubsub_v1 is not configured")

    try:
        from google.api_core.exceptions import AlreadyExists, NotFound
    except ImportError:
        pytest.skip("google-api-core is not installed")

    job_topic_name = f"moriarty-bridge-{endpoint_name}-{priority}"
    result_topic_name = f"moriarty-bridge-result-{case_id}"

    try:
        job_topic_path = publisher.topic_path(project_id, job_topic_name)
        job_subscription_path = subscriber.subscription_path(project_id, f"{job_topic_name}-sub")
        job_topic = publisher.create_topic(request={"name": job_topic_path})
        job_subscription = subscriber.create_subscription(
            request={"name": job_subscription_path, "topic": job_topic_path}
        )

        result_topic_path = publisher.topic_path(project_id, result_topic_name)
        result_subscription_path = subscriber.subscription_path(project_id, f"{result_topic_name}-sub")
        result_topic = publisher.create_topic(request={"name": result_topic_path})
        result_subscription = subscriber.create_subscription(
            request={"name": result_subscription_path, "topic": result_topic_path}
        )

    except AlreadyExists:
        pass
    except Exception:
        pytest.skip("pubsub_v1 is not configured")

    bridge = PubSubBridge(
        bridge_result_topic=result_topic_name,
        project_id=project_id,
    )
    bridge.make_job_queue_url = lambda endpoint_name, priority: job_topic_path
    yield bridge
    
    try:
        publisher.delete_topic(request={"topic": job_topic_path})
        subscriber.delete_subscription(request={"subscription": job_subscription_path})

        publisher.delete_topic(request={"topic": result_topic_path})
        subscriber.delete_subscription(request={"subscription": result_subscription_path})

    except NotFound:
        pass


async def test_bridge_job(pubsub_bridge: PubSubBridge):
    _called = False
    input_job = InferenceJob(
        inference_id="test-inference-id",
        payload={"input": "test"},
    )
    inference_id = await pubsub_bridge.enqueue_job("test", input_job)

    assert inference_id

    async def _process_func(job: InferenceJob):
        nonlocal _called
        _called = True
        assert inference_id == "test-inference-id"

    await pubsub_bridge.dequeue_job("test", _process_func)

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
    pubsub_bridge: PubSubBridge,
    inference_result: InferenceResult,
    endpoint_name,
    priority,
):
    _called = False

    await pubsub_bridge.enqueue_result(inference_result)

    async def _process_func(result: InferenceResult):
        nonlocal _called
        _called = True
        assert result == inference_result

    await pubsub_bridge.list_avaliable_priorities(endpoint_name) == [priority]
    await pubsub_bridge.dequeue_result(_process_func)

    assert _called

    await pubsub_bridge.list_avaliable_priorities(endpoint_name) == []


@pytest.fixture
def result_bucket(pubsub_bridge: PubSubBridge,):
    try:
        from google.cloud import storage

        client = storage.Client()
    except ImportError:
        pytest.skip("google-cloud-storage is not installed")
    except Exception:
        pytest.skip("storage is not configured")

    bucket_name = "morph-pubsub"
    try:
        # Check if bucket exists
        bucket = client.bucket(bucket_name)
    except Exception as e:
        pytest.skip(f"Bucket not found {e}")
    # monkeypatch.setattr(PubSubBridge(), "output_bucket", bucket_name)
    pubsub_bridge.output_bucket = bucket_name
    yield bucket_name


async def test_bridge_big_result(
    pubsub_bridge: PubSubBridge,
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

    await pubsub_bridge.enqueue_result(inference_result)

    async def _process_func(result: InferenceResult):
        nonlocal _called
        _called = True
        assert result == inference_result

    await pubsub_bridge.list_avaliable_priorities(endpoint_name) == [priority]
    await pubsub_bridge.dequeue_result(_process_func)

    assert _called

    await pubsub_bridge.list_avaliable_priorities(endpoint_name) == []
