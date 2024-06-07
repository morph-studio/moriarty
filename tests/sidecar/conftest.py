import socket
import subprocess
import time
from pathlib import Path
from typing import Coroutine

import httpx
import pytest

import docker
from moriarty.sidecar.consumer import InferencerConsumer
from moriarty.sidecar.params import MatrixCallback
from moriarty.sidecar.producer import JobProducer

_HERE = Path(__file__).parent


def get_port():
    # Get an unoccupied port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def docker_client():
    try:
        client = docker.from_env()
        client.ping()
        return client
    except:
        pytest.skip("Docker is not available")


@pytest.fixture(scope="session")
def inference_port():
    inference_port = get_port()
    try:
        p = subprocess.Popen(
            [
                "uvicorn",
                "app:app",
                "--host",
                "0.0.0.0",
                "--port",
                str(inference_port),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=_HERE / "mock",
        )
        time.sleep(1)  # Wait for the server to start
        for _ in range(10):
            try:
                if p.poll() is not None:
                    raise TimeoutError(
                        f"Inference server not ready in 5s. stdout: {p.stdout} stderr: {p.stderr}"
                    )

                httpx.get(f"http://127.0.0.1:{inference_port}/ping")
            except:
                time.sleep(0.5)
            else:
                break
        else:
            p.kill()
            raise TimeoutError(
                f"Inference server not ready in 5s. stdout: {p.stdout} stderr: {p.stderr}"
            )
        yield inference_port
    finally:
        p.kill()


@pytest.fixture
def endpoint_name(case_id):
    return f"unittest-{case_id}"


class MockInferenceConsumer(InferencerConsumer):
    async def _callback(self, *args, **kwargs):
        # Disable callback
        return

    async def _proxy(self, *args, **kwargs):
        self._last_response = await super()._proxy(*args, **kwargs)
        return self._last_response


@pytest.fixture
async def inference_consumer(async_redis_client, endpoint_name, inference_port):
    return MockInferenceConsumer(
        redis_client=async_redis_client,
        endpoint_name=endpoint_name,
        invoke_port=inference_port,
    )
