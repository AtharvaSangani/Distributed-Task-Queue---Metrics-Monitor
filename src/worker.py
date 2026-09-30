"""Consumer Worker implementing blocking pop, exponential backoff, and DLQ isolation."""

from datetime import datetime, timezone
import json
import logging
import signal
import sys
import time
from typing import Optional
import uuid
import redis

from src.config import settings
from src.models import TaskStatus
from src.redis_client import get_redis_client
from src.tasks import execute_task

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [Worker %(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Worker")


class TaskWorker:
    """Decoupled consumer worker processing tasks from Redis queue."""

    def __init__(
        self,
        worker_id: Optional[str] = None,
        redis_client: Optional[redis.Redis] = None,
        backoff_multiplier: Optional[float] = None,
    ):
        self.worker_id = worker_id or f"worker-{str(uuid.uuid4())[:8]}"
        self.redis_client = redis_client or get_redis_client()
        self.backoff_multiplier = (
            backoff_multiplier
            if backoff_multiplier is not None
            else settings.BASE_BACKOFF_SECONDS
        )
        self.running = True

    def setup_signal_handlers(self) -> None:
        """Register graceful shutdown handlers for SIGINT and SIGTERM."""
        def _handle_signal(signum, frame):
            logger.info("Signal %s received. Gracefully shutting down worker %s...", signum, self.worker_id)
            self.running = False

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

    def process_one_task(self, timeout: int = 5) -> bool:
        """
        Pop one task from the Redis queue using blocking right-pop (BRPOP)
        and execute it with fault-tolerance handling.
        Returns True if a task was processed, False if timeout reached.
        """
        # 1. Blocking pop from Redis FIFO list (LPUSH by producer + BRPOP by consumer)
        item = self.redis_client.brpop(settings.TASK_QUEUE, timeout=timeout)
        if not item:
            return False

        _, raw_task_data = item
        now_iso = datetime.now(timezone.utc).isoformat()

        try:
            task_dict = json.loads(raw_task_data)
        except Exception as e:
            logger.error("Failed to parse JSON task payload: %s", e)
            return False

        task_id = task_dict.get("task_id")
        task_type = task_dict.get("task_type")
        payload = task_dict.get("payload", {})
        retries = int(task_dict.get("retries", 0))

        logger.info(
            "[%s] Claimed task %s (Type: %s, Attempt: %d)",
            self.worker_id,
            task_id,
            task_type,
            retries + 1,
        )

        task_key = f"task:{task_id}"

        # 2. Update status to PROCESSING
        self.redis_client.hset(
            task_key,
            mapping={
                "status": TaskStatus.PROCESSING.value,
                "worker_id": self.worker_id,
                "started_at": now_iso,
                "updated_at": now_iso,
            },
        )

        # 3. Execute Task with Error Handling
        try:
            result = execute_task(task_type=task_type, payload=payload, current_retries=retries)

            # Task Succeeded -> Update status to COMPLETED and set 1-hour TTL
            completed_iso = datetime.now(timezone.utc).isoformat()
            pipe = self.redis_client.pipeline()
            pipe.hset(
                task_key,
                mapping={
                    "status": TaskStatus.COMPLETED.value,
                    "result": json.dumps(result),
                    "completed_at": completed_iso,
                    "updated_at": completed_iso,
                },
            )
            # 1-hour TTL for finished tasks
            pipe.expire(task_key, settings.TASK_TTL_SECONDS)
            # Increment completed metric counter
            pipe.incr(settings.METRIC_COMPLETED_TASKS)
            pipe.execute()

            logger.info("[%s] Successfully completed task %s", self.worker_id, task_id)
            return True

        except Exception as exc:
            new_retries = retries + 1
            logger.warning(
                "[%s] Task %s failed on attempt %d: %s",
                self.worker_id,
                task_id,
                new_retries,
                str(exc),
            )

            if new_retries < settings.MAX_RETRIES:
                # Calculate Exponential Backoff Delay: delay = 2 ** retries
                delay = self.backoff_multiplier * (2 ** retries)
                logger.info(
                    "[%s] Transient fault detected. Retrying task %s in %.2fs (Retry %d/%d)",
                    self.worker_id,
                    task_id,
                    delay,
                    new_retries,
                    settings.MAX_RETRIES,
                )

                if delay > 0:
                    time.sleep(delay)

                # Re-queue updated task payload onto task_queue
                task_dict["retries"] = new_retries
                retry_iso = datetime.now(timezone.utc).isoformat()

                pipe = self.redis_client.pipeline()
                pipe.hset(
                    task_key,
                    mapping={
                        "status": TaskStatus.PENDING.value,
                        "retries": new_retries,
                        "error_message": str(exc),
                        "updated_at": retry_iso,
                    },
                )
                pipe.lpush(settings.TASK_QUEUE, json.dumps(task_dict))
                pipe.incr(settings.METRIC_RETRIED_TASKS)
                pipe.execute()
                return True

            else:
                # Max retries exceeded -> Poison pill quarantined in Dead-Letter Queue (DLQ)
                logger.error(
                    "[%s] Max retries exhausted (%d) for task %s. Quarantining to DLQ (%s).",
                    self.worker_id,
                    new_retries,
                    task_id,
                    settings.DEAD_LETTER_QUEUE,
                )

                fail_iso = datetime.now(timezone.utc).isoformat()
                dlq_payload = {
                    "task_id": task_id,
                    "task_type": task_type,
                    "payload": payload,
                    "retries": new_retries,
                    "error_message": str(exc),
                    "failed_at": fail_iso,
                }

                pipe = self.redis_client.pipeline()
                pipe.hset(
                    task_key,
                    mapping={
                        "status": TaskStatus.FAILED.value,
                        "retries": new_retries,
                        "error_message": str(exc),
                        "failed_at": fail_iso,
                        "updated_at": fail_iso,
                    },
                )
                pipe.lpush(settings.DEAD_LETTER_QUEUE, json.dumps(dlq_payload))
                pipe.incr(settings.METRIC_FAILED_TASKS)
                pipe.execute()
                return True

    def run(self) -> None:
        """Main worker event loop."""
        self.setup_signal_handlers()
        logger.info(
            "Worker [%s] started. Listening for tasks on queue '%s'...",
            self.worker_id,
            settings.TASK_QUEUE,
        )

        while self.running:
            try:
                self.process_one_task(timeout=settings.WORKER_POP_TIMEOUT)
            except redis.ConnectionError as ce:
                logger.error("Redis connection error in worker [%s]: %s. Retrying in 2s...", self.worker_id, ce)
                time.sleep(2)
            except Exception as e:
                logger.error("Unexpected error in worker loop [%s]: %s", self.worker_id, e)
                time.sleep(1)

        logger.info("Worker [%s] stopped gracefully.", self.worker_id)
