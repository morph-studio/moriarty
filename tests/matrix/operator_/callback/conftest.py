from __future__ import annotations

import socket
import time
from typing import TYPE_CHECKING

import pytest
import redis
from click.testing import CliRunner
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

import docker
from moriarty import mock
from moriarty.matrix.operator_.callback_app import app as APP
from moriarty.matrix.operator_.cli import drop, init
from moriarty.matrix.operator_.spawner.manager import (
    SpawnerManager,
    get_spawner_manager,
)

if TYPE_CHECKING:
    from docker import DockerClient


def get_port():
    # Get an unoccupied port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def spawner_manager():
    spawner_manager = SpawnerManager()
    spawner_manager._load_dir(mock)

    yield spawner_manager


@pytest.fixture(scope="session")
def docker_client():
    try:
        client = docker.from_env()
        client.ping()
        return client
    except:
        pytest.skip("Docker is not available")


@pytest.fixture(scope="session")
def redis_port(docker_client: DockerClient):
    """
    Start a redis container and return the port
    """
    redis_port = get_port()
    container = None
    try:
        container = docker_client.containers.run(
            "redis",
            detach=True,
            ports={"6379": redis_port},
            remove=True,
        )
        time.sleep(1)  # Wait for the server to start
        while True:
            try:
                # Ping redis
                redis_client = redis.Redis(host="localhost", port=redis_port)
                redis_client.ping()
            except:
                time.sleep(0.5)
            else:
                break
        yield redis_port
    finally:
        if container:
            container.stop()


@pytest.fixture(scope="session")
def pg_port(docker_client: DockerClient):
    pg_port = get_port()
    container = None
    try:
        container = docker_client.containers.run(
            "postgres:14",
            detach=True,
            ports={"5432": pg_port},
            remove=True,
            environment={
                "POSTGRES_USER": "postgres",
                "POSTGRES_PASSWORD": "Th3PAssW0rd!1.",
                "POSTGRES_DB": "moriarty-matrix",
            },
        )
        while True:
            # Execute `pg_isready -U postgres` in the container
            try:
                # Pg is ready
                r = container.exec_run("pg_isready -U postgres")
                assert r.exit_code == 0
                assert b"accepting connections" in r.output
                # Try to connect db
                engine = create_engine(
                    f"postgresql://postgres:Th3PAssW0rd!1.@localhost:{pg_port}/moriarty-matrix"
                )
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
            except Exception as e:
                time.sleep(0.5)
            else:
                break
        runner = CliRunner()
        result = runner.invoke(init, env={"DB_PORT": str(pg_port)})
        assert result.exit_code == 0
        yield pg_port
    finally:
        if container:
            container.stop()


@pytest.fixture
async def app(pg_port, redis_port, monkeypatch):
    monkeypatch.setenv("REDIS_PORT", str(redis_port))
    monkeypatch.setenv("DB_PORT", str(pg_port))

    runner = CliRunner()
    # Drop all before testing
    result = runner.invoke(drop, ["--yes"])
    assert result.exit_code == 0

    # Clear redis data
    redis_client = redis.Redis(host="localhost", port=redis_port)
    redis_client.flushall()

    APP.dependency_overrides = {get_spawner_manager: lambda: spawner_manager}
    yield APP


@pytest.fixture
def client(app):
    return TestClient(app)
