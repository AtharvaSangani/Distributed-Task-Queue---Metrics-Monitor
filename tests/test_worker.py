"""Unit tests for Consumer Worker, exponential backoff, and DLQ isolation."""

import json
from src.config import settings
from src.models import TaskStatus


def test_worker_processes_successful_task(test_worker, fake_redis):
    """Test worker pops task, marks COMPLETED, writes result, and sets 1-hour TTL."""
    task_id = "test-success-task-1"
    task_key = f"task:{task_id}"

    # Setup task in Redis
    fake_redis.hset(
        task_key,
        mapping={
            "task_id": task_id,
            "task_type": "IMAGE_RESIZE",
            "status": TaskStatus.PENDING.value,
            "retries": 0,
            "payload": json.dumps({"width": 800, "height": 600, "simulate_delay_seconds": 0}),
        },
    )
    fake_redis.lpush(
        settings.TASK_QUEUE,
        json.dumps({
            "task_id": task_id,
            "task_type": "IMAGE_RESIZE",
            "payload": {"width": 800, "height": 600, "simulate_delay_seconds": 0},
            "retries": 0,
        }),
    )

    # Process task
    did_process = test_worker.process_one_task(timeout=1)
    assert did_process is True

    # Verify task state in Redis
    task_data = fake_redis.hgetall(task_key)
    assert task_data["status"] == TaskStatus.COMPLETED.value
    assert "result" in task_data
    result = json.loads(task_data["result"])
    assert result["output_resolution"] == "800x600"
    assert result["status"] == "SUCCESS"

    # Verify 1-hour TTL was set (TTL > 0)
    ttl = fake_redis.ttl(task_key)
    assert 0 < ttl <= settings.TASK_TTL_SECONDS

    # Verify metrics
    completed_metric = int(fake_redis.get(settings.METRIC_COMPLETED_TASKS) or 0)
    assert completed_metric == 1

    # Verify queue is now empty
    assert fake_redis.llen(settings.TASK_QUEUE) == 0


def test_worker_transient_error_and_exponential_backoff(test_worker, fake_redis):
    """
    Test that a transient error:
    1. Triggers exponential backoff.
    2. Increments retries count.
    3. Pushes payload back onto task_queue for subsequent retry.
    4. Eventually succeeds once downstream dependency recovers.
    """
    task_id = "test-transient-task-2"
    task_key = f"task:{task_id}"

    # Task is configured to fail on attempt 1 and 2, but succeed on attempt 3 (when retries == 2)
    payload = {"fail_until_retry": 2, "simulate_delay_seconds": 0}

    fake_redis.hset(
        task_key,
        mapping={
            "task_id": task_id,
            "task_type": "FLAKY_API",
            "status": TaskStatus.PENDING.value,
            "retries": 0,
            "payload": json.dumps(payload),
        },
    )
    fake_redis.lpush(
        settings.TASK_QUEUE,
        json.dumps({
            "task_id": task_id,
            "task_type": "FLAKY_API",
            "payload": payload,
            "retries": 0,
        }),
    )

    # --- Attempt 1 (Failure 1) ---
    res1 = test_worker.process_one_task(timeout=1)
    assert res1 is True

    # Check retried state: retries should be 1 and requeued to task_queue
    task_data = fake_redis.hgetall(task_key)
    assert task_data["status"] == TaskStatus.PENDING.value
    assert int(task_data["retries"]) == 1
    assert "transient network timeout" in task_data["error_message"].lower()
    assert fake_redis.llen(settings.TASK_QUEUE) == 1
    assert fake_redis.llen(settings.DEAD_LETTER_QUEUE) == 0

    retried_metric = int(fake_redis.get(settings.METRIC_RETRIED_TASKS) or 0)
    assert retried_metric == 1

    # --- Attempt 2 (Failure 2) ---
    res2 = test_worker.process_one_task(timeout=1)
    assert res2 is True

    task_data = fake_redis.hgetall(task_key)
    assert int(task_data["retries"]) == 2
    assert fake_redis.llen(settings.TASK_QUEUE) == 1
    assert fake_redis.llen(settings.DEAD_LETTER_QUEUE) == 0

    retried_metric = int(fake_redis.get(settings.METRIC_RETRIED_TASKS) or 0)
    assert retried_metric == 2

    # --- Attempt 3 (Recovery & Success) ---
    res3 = test_worker.process_one_task(timeout=1)
    assert res3 is True

    # Now task should be COMPLETED
    task_data = fake_redis.hgetall(task_key)
    assert task_data["status"] == TaskStatus.COMPLETED.value
    assert fake_redis.llen(settings.TASK_QUEUE) == 0
    assert fake_redis.llen(settings.DEAD_LETTER_QUEUE) == 0

    completed_metric = int(fake_redis.get(settings.METRIC_COMPLETED_TASKS) or 0)
    assert completed_metric == 1


def test_worker_poison_pill_routes_to_dlq_after_3_retries(test_worker, fake_redis):
    """
    Test poison pill task:
    After failing 3 times, worker permanently isolates it into dead_letter_queue
    and marks status as FAILED.
    """
    task_id = "test-poison-pill-3"
    task_key = f"task:{task_id}"

    payload = {"always_fail": True, "simulate_delay_seconds": 0}

    fake_redis.hset(
        task_key,
        mapping={
            "task_id": task_id,
            "task_type": "FLAKY_API",
            "status": TaskStatus.PENDING.value,
            "retries": 0,
            "payload": json.dumps(payload),
        },
    )
    fake_redis.lpush(
        settings.TASK_QUEUE,
        json.dumps({
            "task_id": task_id,
            "task_type": "FLAKY_API",
            "payload": payload,
            "retries": 0,
        }),
    )

    # Attempt 1 (retries becomes 1) -> re-queued
    test_worker.process_one_task(timeout=1)
    assert fake_redis.llen(settings.TASK_QUEUE) == 1
    assert fake_redis.llen(settings.DEAD_LETTER_QUEUE) == 0

    # Attempt 2 (retries becomes 2) -> re-queued
    test_worker.process_one_task(timeout=1)
    assert fake_redis.llen(settings.TASK_QUEUE) == 1
    assert fake_redis.llen(settings.DEAD_LETTER_QUEUE) == 0

    # Attempt 3 (retries becomes 3 == MAX_RETRIES) -> Quarantined to DLQ!
    test_worker.process_one_task(timeout=1)

    # task_queue must now be EMPTY (poison pill removed, preventing infinite loops)
    assert fake_redis.llen(settings.TASK_QUEUE) == 0

    # dead_letter_queue must contain the poison pill
    assert fake_redis.llen(settings.DEAD_LETTER_QUEUE) == 1

    # Verify DLQ payload structure
    raw_dlq = fake_redis.rpop(settings.DEAD_LETTER_QUEUE)
    dlq_item = json.loads(raw_dlq)
    assert dlq_item["task_id"] == task_id
    assert dlq_item["task_type"] == "FLAKY_API"
    assert dlq_item["retries"] == 3
    assert "error_message" in dlq_item
    assert "failed_at" in dlq_item

    # Verify task hash in Redis is marked FAILED
    task_data = fake_redis.hgetall(task_key)
    assert task_data["status"] == TaskStatus.FAILED.value
    assert int(task_data["retries"]) == 3

    # Verify metrics
    failed_metric = int(fake_redis.get(settings.METRIC_FAILED_TASKS) or 0)
    assert failed_metric == 1


def test_worker_idle_timeout_when_queue_empty(test_worker):
    """Test worker handles empty queue gracefully without crashing."""
    did_process = test_worker.process_one_task(timeout=1)
    assert did_process is False


def test_worker_handles_malformed_json(test_worker, fake_redis):
    """Test that malformed JSON in the queue is skipped without crashing the worker."""
    fake_redis.lpush(settings.TASK_QUEUE, "{not-valid-json}")
    did_process = test_worker.process_one_task(timeout=1)
    assert did_process is False
    # Malformed item popped and handled safely
    assert fake_redis.llen(settings.TASK_QUEUE) == 0


def test_multiple_workers_competing_consumers(fake_redis):
    """Test that multiple worker instances safely compete for tasks without race conditions."""
    from src.worker import TaskWorker

    worker1 = TaskWorker(worker_id="worker-node-1", redis_client=fake_redis, backoff_multiplier=0.0)
    worker2 = TaskWorker(worker_id="worker-node-2", redis_client=fake_redis, backoff_multiplier=0.0)

    # Push 4 tasks
    for i in range(4):
        task_id = f"competing-task-{i}"
        fake_redis.hset(
            f"task:{task_id}",
            mapping={
                "task_id": task_id,
                "task_type": "REPORT_GEN",
                "status": TaskStatus.PENDING.value,
                "retries": 0,
                "payload": json.dumps({"report_name": f"rep-{i}", "simulate_delay_seconds": 0}),
            },
        )
        fake_redis.lpush(
            settings.TASK_QUEUE,
            json.dumps({
                "task_id": task_id,
                "task_type": "REPORT_GEN",
                "payload": {"report_name": f"rep-{i}", "simulate_delay_seconds": 0},
                "retries": 0,
            }),
        )

    # Both workers process tasks alternately
    assert worker1.process_one_task(timeout=1) is True
    assert worker2.process_one_task(timeout=1) is True
    assert worker1.process_one_task(timeout=1) is True
    assert worker2.process_one_task(timeout=1) is True

    # Queue should now be empty and 4 tasks completed
    assert fake_redis.llen(settings.TASK_QUEUE) == 0
    completed_count = int(fake_redis.get(settings.METRIC_COMPLETED_TASKS) or 0)
    assert completed_count == 4

