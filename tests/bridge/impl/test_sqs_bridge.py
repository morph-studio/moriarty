import pytest

from moriarty.matrix.job_manager.bridge.impl.sqs_bridge import SQSBridge


@pytest.fixture
def sqs_bridge(case_id):
    try:
        import boto3

        sqs = boto3.client("sqs")
    except ImportError:
        pytest.skip("boto3 is not installed")
    except Exception:
        pytest.skip("boto3 is not configured")

    job_queue_name = f"moriarty-bridge-job-test-{case_id}"
    result_queue_name = f"moriarty-bridge-result-test-{case_id}"
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
    bridge.make_job_queue_url = lambda _: job_queue_url
    yield bridge
    try:
        sqs.delete_queue(QueueUrl=job_queue_url)
        sqs.delete_queue(QueueUrl=result_queue_url)
    except sqs.exceptions.QueueDoesNotExist:
        pass
