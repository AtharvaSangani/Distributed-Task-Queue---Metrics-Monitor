"""FastAPI Producer API for Distributed Task Queue."""

from datetime import datetime, timezone
import json
import uuid
from typing import Optional
from fastapi import FastAPI, HTTPException, status
import redis

from src.config import settings
from src.models import (
    DLQItem,
    DLQResponse,
    MetricsResponse,
    TaskCreateRequest,
    TaskDetailResponse,
    TaskResponse,
    TaskStatus,
)
from src.redis_client import check_redis_health, get_redis_client

app = FastAPI(
    title="Distributed Task Queue & Metrics Monitor",
    description="High-throughput asynchronous task queue producer showcasing decoupled producer-consumer pattern, backoff, and DLQ isolation.",
    version="1.0.0",
)


@app.get("/health", tags=["Health"])
def health_check():
    """Liveness probe verifying API and Redis connection."""
    redis_healthy = check_redis_health()
    if not redis_healthy:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redis service unavailable",
        )
    return {"status": "HEALTHY", "redis": "CONNECTED"}


@app.post(
    "/api/v1/tasks",
    response_model=TaskResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["Tasks"],
    summary="Enqueue a new asynchronous background task",
)
def enqueue_task(request: TaskCreateRequest):
    """
    Accepts task submission, stores initial metadata in Redis Hash,
    and pushes to FIFO task queue (LPUSH) in sub-10ms.
    """
    client = get_redis_client()
    task_id = str(uuid.uuid4())
    now_iso = datetime.now(timezone.utc).isoformat()

    # 1. Initialize Task Metadata Hash in Redis
    task_key = f"task:{task_id}"
    task_data = {
        "task_id": task_id,
        "task_type": request.task_type.value,
        "status": TaskStatus.PENDING.value,
        "retries": 0,
        "payload": json.dumps(request.payload),
        "created_at": now_iso,
        "updated_at": now_iso,
    }

    # Pipeline Hash write and Queue LPUSH for atomic low-latency execution
    pipe = client.pipeline()
    pipe.hset(task_key, mapping=task_data)

    # 2. Enqueue the task payload onto Redis List (FIFO: LPUSH -> BRPOP)
    queue_item = {
        "task_id": task_id,
        "task_type": request.task_type.value,
        "payload": request.payload,
        "retries": 0,
        "enqueued_at": now_iso,
    }
    pipe.lpush(settings.TASK_QUEUE, json.dumps(queue_item))
    pipe.execute()

    return TaskResponse(task_id=task_id, status=TaskStatus.PENDING)


@app.get(
    "/api/v1/tasks/{task_id}",
    response_model=TaskDetailResponse,
    tags=["Tasks"],
    summary="Poll task execution status and results",
)
def get_task_status(task_id: str):
    """Retrieve task state, retries, and result or error from Redis."""
    client = get_redis_client()
    task_key = f"task:{task_id}"
    task_data = client.hgetall(task_key)

    if not task_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task with ID {task_id} not found",
        )

    # Deserialize payload and result if present
    payload = json.loads(task_data.get("payload", "{}"))
    raw_result = task_data.get("result")
    result = json.loads(raw_result) if raw_result else None

    return TaskDetailResponse(
        task_id=task_data["task_id"],
        task_type=task_data["task_type"],
        status=TaskStatus(task_data["status"]),
        retries=int(task_data.get("retries", 0)),
        payload=payload,
        result=result,
        error_message=task_data.get("error_message"),
        created_at=task_data.get("created_at"),
        updated_at=task_data.get("updated_at"),
        completed_at=task_data.get("completed_at"),
    )


@app.get(
    "/api/v1/metrics",
    response_model=MetricsResponse,
    tags=["Metrics"],
    summary="Query real-time queue performance and operational metrics",
)
def get_metrics():
    """Inspect queue backlog, dead-letter count, and completed counter."""
    client = get_redis_client()

    pipe = client.pipeline()
    pipe.llen(settings.TASK_QUEUE)
    pipe.llen(settings.DEAD_LETTER_QUEUE)
    pipe.get(settings.METRIC_COMPLETED_TASKS)
    pipe.get(settings.METRIC_FAILED_TASKS)
    pipe.get(settings.METRIC_RETRIED_TASKS)
    results = pipe.execute()

    queue_len = results[0] or 0
    dlq_len = results[1] or 0
    completed = int(results[2]) if results[2] else 0
    failed = int(results[3]) if results[3] else 0
    retried = int(results[4]) if results[4] else 0

    return MetricsResponse(
        queue_length=queue_len,
        dead_letter_queue_length=dlq_len,
        completed_tasks=completed,
        failed_tasks=failed,
        retried_tasks=retried,
        status="healthy",
    )


@app.get(
    "/api/v1/dlq",
    response_model=DLQResponse,
    tags=["Metrics"],
    summary="Inspect messages quarantined in Dead-Letter Queue",
)
def get_dlq_messages(limit: int = 100):
    """Retrieve dead-lettered poison pills for inspection and debugging."""
    client = get_redis_client()
    raw_items = client.lrange(settings.DEAD_LETTER_QUEUE, 0, limit - 1)

    items = []
    for raw in raw_items:
        try:
            parsed = json.loads(raw)
            items.append(
                DLQItem(
                    task_id=parsed.get("task_id", "unknown"),
                    task_type=parsed.get("task_type", "UNKNOWN"),
                    payload=parsed.get("payload", {}),
                    retries=parsed.get("retries", 0),
                    error_message=parsed.get("error_message"),
                    failed_at=parsed.get("failed_at"),
                )
            )
        except Exception:
            continue

    total_count = client.llen(settings.DEAD_LETTER_QUEUE)
    return DLQResponse(total_count=total_count, items=items)
