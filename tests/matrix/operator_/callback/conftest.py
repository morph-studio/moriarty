from __future__ import annotations

import pytest
import redis
from click.testing import CliRunner
from fastapi import Depends
from fastapi.testclient import TestClient
from sqlalchemy import delete

from moriarty import mock
from moriarty.envs import get_bridge_name, get_spawner_name
from moriarty.matrix.job_manager.bridge_wrapper import get_bridge_manager
from moriarty.matrix.operator_.callback import CallbackManager, get_callback_consumer
from moriarty.matrix.operator_.callback_app import app as APP
from moriarty.matrix.operator_.cli import drop
from moriarty.matrix.operator_.dbutils import get_db_session
from moriarty.matrix.operator_.operator_ import get_bridger
from moriarty.matrix.operator_.orm import InferenceLogORM
from moriarty.matrix.operator_.rds import get_redis_client
from moriarty.matrix.operator_.spawner.manager import get_spawner_manager


@pytest.fixture
async def app(pg_port, redis_port, monkeypatch, spawner_manager, bridge_manager):
    monkeypatch.setenv("REDIS_PORT", str(redis_port))
    monkeypatch.setenv("DB_PORT", str(pg_port))

    runner = CliRunner()
    # Drop all before testing
    result = runner.invoke(drop, ["--yes"])
    assert result.exit_code == 0

    # Clear redis data
    redis_client = redis.Redis(host="localhost", port=redis_port)
    redis_client.flushall()

    APP.dependency_overrides = {
        get_spawner_manager: lambda: spawner_manager,
        get_bridge_name: lambda: "mock",
        get_bridge_manager: lambda: bridge_manager,
        get_spawner_name: lambda: "mock",
        get_callback_consumer: get_mock_callback_consumer,
    }
    yield APP
    result = runner.invoke(drop, ["--yes"])
    assert result.exit_code == 0


@pytest.fixture
def client(app):
    return TestClient(app)


@pytest.fixture
def app_client(app):
    with TestClient(app) as client:
        yield client


@pytest.fixture
def inference_log(session):
    inference_id = "mock"
    session.add(
        InferenceLogORM(
            inference_id=inference_id,
            endpoint_name="mock",
            inference_job={},
        )
    )
    session.commit()
    yield inference_id
    session.execute(delete(InferenceLogORM).where(InferenceLogORM.inference_id == inference_id))


@pytest.fixture
def mock_callback_cls():
    MockCallbackManager.CALLED = False
    yield MockCallbackManager
    MockCallbackManager.CALLED = False


def get_mock_callback_consumer(
    redis_client=Depends(get_redis_client),
    bridger=Depends(get_bridger),
    session=Depends(get_db_session),
):
    return MockCallbackManager(redis_client=redis_client, bridger=bridger, session=session)


class MockCallbackManager(CallbackManager):
    CALLED = False

    async def handle_callback(self, payload: str) -> None:
        MockCallbackManager.CALLED = True
