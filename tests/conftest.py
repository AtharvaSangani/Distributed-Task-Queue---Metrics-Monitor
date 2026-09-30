"""Pytest fixtures providing isolated fakeredis instances and TestClient."""

import pytest
import fakeredis
from fastapi.testclient import TestClient

from src.producer import app
from src.redis_client import set_redis_client_override
from src.worker import TaskWorker


@pytest.fixture
def fake_redis():
    """Isolated in-memory Redis instance using fakeredis."""
    r = fakeredis.FakeRedis(decode_responses=True)
    yield r
    r.flushall()


@pytest.fixture
def test_client(fake_redis):
    """FastAPI TestClient with fakeredis dependency override."""
    set_redis_client_override(fake_redis)
    client = TestClient(app)
    yield client
    set_redis_client_override(None)


@pytest.fixture
def test_worker(fake_redis):
    """Worker instance backed by fakeredis with 0 backoff delay for ultra-fast tests."""
    return TaskWorker(
        worker_id="unit-test-worker",
        redis_client=fake_redis,
        backoff_multiplier=0.0,
    )
