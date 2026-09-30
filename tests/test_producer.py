"""Unit and API tests for the Producer API."""

import json
from src.config import settings


def test_health_check(test_client):
    """Test health check liveness probe."""
    response = test_client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "HEALTHY"
    assert data["redis"] == "CONNECTED"


def test_enqueue_task_image_resize(test_client, fake_redis):
    """Test successful task enqueuing with HTTP 202 and Redis verification."""
    payload = {
        "task_type": "IMAGE_RESIZE",
        "payload": {"image_url": "https://cdn.example.com/pic.jpg", "width": 1024, "height": 768},
    }
    response = test_client.post("/api/v1/tasks", json=payload)
    assert response.status_code == 202
    data = response.json()
    assert "task_id" in data
    assert data["status"] == "PENDING"

    task_id = data["task_id"]

    # Verify Redis Hash was written
    hash_data = fake_redis.hgetall(f"task:{task_id}")
    assert hash_data["task_id"] == task_id
    assert hash_data["task_type"] == "IMAGE_RESIZE"
    assert hash_data["status"] == "PENDING"
    assert int(hash_data["retries"]) == 0

    # Verify task was pushed to Redis queue list
    queue_len = fake_redis.llen(settings.TASK_QUEUE)
    assert queue_len == 1

    popped = fake_redis.rpop(settings.TASK_QUEUE)
    queued_item = json.loads(popped)
    assert queued_item["task_id"] == task_id
    assert queued_item["task_type"] == "IMAGE_RESIZE"


def test_enqueue_task_report_gen(test_client, fake_redis):
    """Test submitting REPORT_GEN task."""
    payload = {
        "task_type": "REPORT_GEN",
        "payload": {"report_name": "Quarterly_Taxes", "date_range": "2026-Q1", "format": "PDF"},
    }
    response = test_client.post("/api/v1/tasks", json=payload)
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "PENDING"


def test_enqueue_invalid_task_type(test_client):
    """Test submission with invalid task_type returns 422 validation error."""
    payload = {
        "task_type": "INVALID_TYPE_XYZ",
        "payload": {},
    }
    response = test_client.post("/api/v1/tasks", json=payload)
    assert response.status_code == 422


def test_get_task_status_existing(test_client):
    """Test polling an enqueued task returns 200 and PENDING status."""
    payload = {"task_type": "IMAGE_RESIZE", "payload": {"width": 800}}
    post_res = test_client.post("/api/v1/tasks", json=payload)
    task_id = post_res.json()["task_id"]

    get_res = test_client.get(f"/api/v1/tasks/{task_id}")
    assert get_res.status_code == 200
    data = get_res.json()
    assert data["task_id"] == task_id
    assert data["status"] == "PENDING"
    assert data["retries"] == 0
    assert data["payload"]["width"] == 800


def test_get_task_status_not_found(test_client):
    """Test polling a non-existent task returns 404."""
    response = test_client.get("/api/v1/tasks/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_metrics_endpoint(test_client, fake_redis):
    """Test metrics endpoint returns queue depth and counter stats."""
    # Pre-populate some metrics
    fake_redis.incr(settings.METRIC_COMPLETED_TASKS, 15)
    fake_redis.incr(settings.METRIC_FAILED_TASKS, 2)
    fake_redis.incr(settings.METRIC_RETRIED_TASKS, 5)

    response = test_client.get("/api/v1/metrics")
    assert response.status_code == 200
    metrics = response.json()
    assert metrics["queue_length"] == 0
    assert metrics["dead_letter_queue_length"] == 0
    assert metrics["completed_tasks"] == 15
    assert metrics["failed_tasks"] == 2
    assert metrics["retried_tasks"] == 5
    assert metrics["status"] == "healthy"


def test_dlq_endpoint(test_client, fake_redis):
    """Test DLQ inspection endpoint with empty and populated queue."""
    # Initially empty
    empty_res = test_client.get("/api/v1/dlq")
    assert empty_res.status_code == 200
    assert empty_res.json()["total_count"] == 0
    assert empty_res.json()["items"] == []

    # Insert a simulated poison pill
    dlq_item = {
        "task_id": "test-poison-pill-123",
        "task_type": "FLAKY_API",
        "payload": {"always_fail": True},
        "retries": 3,
        "error_message": "Permanent failure",
        "failed_at": "2026-09-30T12:00:00Z",
    }
    fake_redis.lpush(settings.DEAD_LETTER_QUEUE, json.dumps(dlq_item))

    populated_res = test_client.get("/api/v1/dlq")
    assert populated_res.status_code == 200
    dlq_data = populated_res.json()
    assert dlq_data["total_count"] == 1
    assert len(dlq_data["items"]) == 1
    assert dlq_data["items"][0]["task_id"] == "test-poison-pill-123"
    assert dlq_data["items"][0]["retries"] == 3


def test_enqueue_response_latency_sub_10ms(test_client):
    """Verify that task enqueuing meets the sub-10ms SLA requirement."""
    import time
    payload = {
        "task_type": "IMAGE_RESIZE",
        "payload": {"width": 1920, "height": 1080},
    }
    # Warmup
    test_client.post("/api/v1/tasks", json=payload)

    # Measure batch of requests
    durations = []
    for _ in range(20):
        start = time.perf_counter()
        res = test_client.post("/api/v1/tasks", json=payload)
        dur = (time.perf_counter() - start) * 1000  # ms
        assert res.status_code == 202
        durations.append(dur)

    avg_latency = sum(durations) / len(durations)
    # Average latency should easily be well under 10ms with in-memory Redis
    assert avg_latency < 10.0, f"Average latency {avg_latency:.2f}ms exceeded 10ms target"

