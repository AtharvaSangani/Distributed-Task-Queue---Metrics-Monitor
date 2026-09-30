"""Task execution implementations and simulation logic."""

import time
from typing import Any, Dict
from src.models import TaskType


class TransientTaskError(Exception):
    """Recoverable failure that should be retried with exponential backoff."""
    pass


class PermanentTaskError(Exception):
    """Irrecoverable poison pill error that should be routed to DLQ immediately or after max retries."""
    pass


def handle_image_resize(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Simulate CPU and image manipulation workload."""
    target_width = payload.get("width", 1024)
    target_height = payload.get("height", 768)
    image_url = payload.get("image_url", "https://assets.cdn.local/sample.png")
    quality = payload.get("quality", 85)

    # Simulate minor processing delay if specified
    sim_delay = payload.get("simulate_delay_seconds", 0.05)
    if sim_delay > 0:
        time.sleep(sim_delay)

    return {
        "action": "IMAGE_RESIZE",
        "source_url": image_url,
        "output_resolution": f"{target_width}x{target_height}",
        "quality_percentage": quality,
        "compressed_size_kb": round((target_width * target_height * 3) / 10240, 2),
        "status": "SUCCESS"
    }


def handle_report_gen(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Simulate data aggregation and PDF/CSV export workload."""
    report_name = payload.get("report_name", "Monthly_Financial_Summary")
    date_range = payload.get("date_range", "2026-Q1")
    format_type = payload.get("format", "PDF").upper()

    sim_delay = payload.get("simulate_delay_seconds", 0.05)
    if sim_delay > 0:
        time.sleep(sim_delay)

    return {
        "action": "REPORT_GEN",
        "report_name": report_name,
        "date_range": date_range,
        "format": format_type,
        "rows_processed": 14250,
        "export_url": f"https://s3.amazonaws.com/reports-bucket/{report_name}_{date_range}.{format_type.lower()}",
        "status": "SUCCESS"
    }


def handle_flaky_api(payload: Dict[str, Any], current_retries: int) -> Dict[str, Any]:
    """
    Simulate transient network dependencies or permanent poison pills.
    Used for testing exponential backoff and DLQ routing.
    """
    fail_until_retry = payload.get("fail_until_retry", 3)
    always_fail = payload.get("always_fail", False)

    if always_fail or current_retries < fail_until_retry:
        raise TransientTaskError(
            f"Simulated transient network timeout (attempt {current_retries + 1}/{fail_until_retry})"
        )

    return {
        "action": "FLAKY_API",
        "attempts_required": current_retries + 1,
        "message": "Downstream service recovered successfully",
        "status": "SUCCESS"
    }


def execute_task(task_type: str, payload: Dict[str, Any], current_retries: int = 0) -> Dict[str, Any]:
    """Route task to its respective handler."""
    if task_type == TaskType.IMAGE_RESIZE:
        return handle_image_resize(payload)
    elif task_type == TaskType.REPORT_GEN:
        return handle_report_gen(payload)
    elif task_type == TaskType.FLAKY_API:
        return handle_flaky_api(payload, current_retries)
    else:
        raise PermanentTaskError(f"Unsupported task type: {task_type}")
