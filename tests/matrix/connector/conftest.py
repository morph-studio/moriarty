from __future__ import annotations

import socket
from functools import partial
from typing import TYPE_CHECKING

import httpx
import pytest
from fastapi.testclient import TestClient

import docker
from moriarty import mock
from moriarty.envs import get_bridge_name
from moriarty.matrix.connector.app import app as APP
from moriarty.matrix.job_manager.bridge.manager import BridgeManager
from moriarty.matrix.job_manager.bridge_wrapper import get_bridge_manager

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
def bridge_manager():
    bm = BridgeManager()
    bm._load_dir(mock)
    yield bm


@pytest.fixture
async def app(bridge_manager):
    APP.dependency_overrides = {
        get_bridge_name: lambda: "mock",
        get_bridge_manager: lambda: bridge_manager,
    }
    yield APP


@pytest.fixture
def client(app):
    return TestClient(app)


@pytest.fixture
async def async_client(app):
    return partial(httpx.AsyncClient, app=app)
