"""Data models and schemas for tasks, metrics, and dead-letter queue."""

from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class TaskType(str, Enum):
    """Supported task types."""
    REPORT_GEN = "REPORT_GEN"
    IMAGE_RESIZE = "IMAGE_RESIZE"
    FLAKY_API = "FLAKY_API"


class TaskStatus(str, Enum):
    """Lifecycle states for asynchronous tasks."""
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class TaskCreateRequest(BaseModel):
    """Schema for submitting a new task to the queue."""
    task_type: TaskType = Field(..., description="The type of task to process")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Task arguments / input payload")

    model_config = {
        "json_schema_extra": {
            "example": {
                "task_type": "IMAGE_RESIZE",
                "payload": {"image_url": "https://example.com/asset.png", "width": 800, "height": 600}
            }
        }
    }


class TaskResponse(BaseModel):
    """Immediate acknowledgement returned upon enqueuing (HTTP 202)."""
    task_id: str = Field(..., description="Unique UUID identifier for the queued task")
    status: TaskStatus = Field(TaskStatus.PENDING, description="Initial task status")


class TaskDetailResponse(BaseModel):
    """Detailed task status and execution result schema."""
    task_id: str
    task_type: TaskType
    status: TaskStatus
    retries: int = 0
    payload: Dict[str, Any] = Field(default_factory=dict)
    result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    completed_at: Optional[str] = None


class MetricsResponse(BaseModel):
    """Operational metrics schema returned by /api/v1/metrics."""
    queue_length: int = Field(..., description="Current number of pending tasks in task_queue")
    dead_letter_queue_length: int = Field(..., description="Current count of poison pills in dead_letter_queue")
    completed_tasks: int = Field(..., description="Total tasks processed successfully")
    failed_tasks: int = Field(..., description="Total tasks moved to dead-letter queue after max retries")
    retried_tasks: int = Field(..., description="Total retry attempts executed across all tasks")
    status: str = Field("healthy", description="Operational health status of the task pipeline")


class DLQItem(BaseModel):
    """Structure of an isolated poison pill stored in the Dead-Letter Queue."""
    task_id: str
    task_type: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    retries: int = 0
    error_message: Optional[str] = None
    failed_at: Optional[str] = None


class DLQResponse(BaseModel):
    """Response schema for Dead-Letter Queue inspection."""
    total_count: int
    items: List[DLQItem]
