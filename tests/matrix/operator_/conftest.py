from __future__ import annotations

import socket
import time
from typing import TYPE_CHECKING

import pytest
import redis
from click.testing import CliRunner
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, exc, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

import docker
from moriarty import mock
from moriarty.matrix.job_manager.bridge.manager import BridgeManager
from moriarty.matrix.job_manager.bridge_wrapper import BridgeWrapper
from moriarty.matrix.operator_.cli import drop, init
from moriarty.matrix.operator_.config import get_config
from moriarty.matrix.operator_.dbutils import get_db_url
from moriarty.matrix.operator_.operator_ import Bridger
from moriarty.matrix.operator_.spawner.manager import SpawnerManager, get_spawner

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
def spawner_manager():
    spawner_manager = SpawnerManager()
    spawner_manager._load_dir(mock)

    yield spawner_manager


@pytest.fixture
def bridge_manager():
    bridge_manager = BridgeManager()
    bridge_manager._load_dir(mock)

    yield bridge_manager


@pytest.fixture
async def async_redis_client(redis_port, monkeypatch):
    monkeypatch.setenv("REDIS_PORT", str(redis_port))
    _config = get_config()

    redis_url = _config.redis.get_redis_url()
    if _config.redis.cluster:
        redis_client = redis.asyncio.RedisCluster.from_url(redis_url, decode_responses=True)
    else:
        redis_client = redis.asyncio.from_url(redis_url, decode_responses=True)
    try:
        yield redis_client
    finally:
        await redis_client.aclose()


@pytest.fixture
async def async_session(pg_port, monkeypatch):
    monkeypatch.setenv("DB_PORT", str(pg_port))
    _config = get_config()
    engine = create_async_engine(get_db_url(_config, async_mode=True))
    factory = async_sessionmaker(engine)
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except exc.SQLAlchemyError as error:
            await session.rollback()
            raise


@pytest.fixture
def session(pg_port, monkeypatch):
    monkeypatch.setenv("DB_PORT", str(pg_port))
    _config = get_config()
    engine = create_engine(get_db_url(_config, async_mode=False))
    factory = sessionmaker(engine)
    with factory() as session:
        try:
            yield session
            session.commit()
        except exc.SQLAlchemyError as error:
            session.rollback()
            raise


@pytest.fixture
def bridge_wrapper(bridge_manager):
    return BridgeWrapper(bridge_manager)


@pytest.fixture
async def spawner(spawner_manager):
    return await get_spawner(
        spawner_name="mock",
        spawner_manager=spawner_manager,
    )


@pytest.fixture
async def bridger(
    spawner,
    async_redis_client: redis.asyncio.Redis,
    async_session: AsyncSession,
    bridge_wrapper: BridgeWrapper,
):
    return Bridger(
        spawner=spawner,
        bridge_name="mock",
        bridge_wrapper=bridge_wrapper,
        redis_client=async_redis_client,
        session=async_session,
    )
