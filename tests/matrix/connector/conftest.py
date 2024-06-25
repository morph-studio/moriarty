from __future__ import annotations

import socket
from typing import TYPE_CHECKING

import pytest
from fastapi.testclient import TestClient

import docker
from moriarty.matrix.connector.app import app as APP

if TYPE_CHECKING:
    from docker import DockerClient


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


@pytest.fixture
async def app():
    APP.dependency_overrides = {}
    yield APP


@pytest.fixture
def client(app):
    return TestClient(app)
