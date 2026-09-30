"""End-to-End integration tests for Producer-Consumer workflow."""

from src.models import TaskStatus


def test_end_to_end_task_lifecycle(test_client, test_worker):
    """
    Simulates complete asynchronous task journey:
    1. HTTP POST enqueues task and returns 202 Accepted.
    2. HTTP GET confirms PENDING status.
    3. Worker pulls task, processes it, and marks COMPLETED.
    4. HTTP GET returns final result and execution duration.
    5. HTTP GET /metrics reflects updated metrics.
    """
    # 1. Enqueue task via Producer API
    create_response = test_client.post(
        "/api/v1/tasks",
        json={
            "task_type": "REPORT_GEN",
            "payload": {
                "report_name": "Annual_Audit",
                "format": "PDF",
                "simulate_delay_seconds": 0,
            },
        },
    )
    assert create_response.status_code == 202
    task_id = create_response.json()["task_id"]

    # 2. Check task status before worker execution
    status_response = test_client.get(f"/api/v1/tasks/{task_id}")
    assert status_response.status_code == 200
    assert status_response.json()["status"] == TaskStatus.PENDING.value

    # 3. Worker executes the task
    processed = test_worker.process_one_task(timeout=1)
    assert processed is True

    # 4. Check task status after worker execution
    completed_response = test_client.get(f"/api/v1/tasks/{task_id}")
    assert completed_response.status_code == 200
    detail = completed_response.json()
    assert detail["status"] == TaskStatus.COMPLETED.value
    assert detail["result"]["action"] == "REPORT_GEN"
    assert "export_url" in detail["result"]
    assert detail["completed_at"] is not None

    # 5. Verify system metrics
    metrics_response = test_client.get("/api/v1/metrics")
    assert metrics_response.status_code == 200
    metrics = metrics_response.json()
    assert metrics["queue_length"] == 0
    assert metrics["completed_tasks"] == 1
    assert metrics["dead_letter_queue_length"] == 0


def test_end_to_end_poison_pill_quarantine(test_client, test_worker):
    """
    Simulates poison pill lifecycle:
    1. Producer enqueues an unprocessable/poison pill task.
    2. Worker retries until MAX_RETRIES (3) are reached.
    3. Task is quarantined to DLQ.
    4. Task status reflects FAILED.
    5. DLQ API endpoint lists the quarantined task.
    """
    # 1. Submit poison pill task
    create_res = test_client.post(
        "/api/v1/tasks",
        json={
            "task_type": "FLAKY_API",
            "payload": {"always_fail": True, "simulate_delay_seconds": 0},
        },
    )
    assert create_res.status_code == 202
    task_id = create_res.json()["task_id"]

    # 2. Worker executes 3 attempts
    for _ in range(3):
        assert test_worker.process_one_task(timeout=1) is True

    # 3. Poll task status -> should now be FAILED
    status_res = test_client.get(f"/api/v1/tasks/{task_id}")
    assert status_res.status_code == 200
    assert status_res.json()["status"] == TaskStatus.FAILED.value
    assert status_res.json()["retries"] == 3

    # 4. Check DLQ API endpoint
    dlq_res = test_client.get("/api/v1/dlq")
    assert dlq_res.status_code == 200
    dlq_data = dlq_res.json()
    assert dlq_data["total_count"] >= 1
    matching = [item for item in dlq_data["items"] if item["task_id"] == task_id]
    assert len(matching) == 1
    assert matching[0]["retries"] == 3
