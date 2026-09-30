"""Locust load testing scenario simulating high-concurrency producer and poller traffic."""

import random
from collections import deque
from locust import HttpUser, between, task, events


class TaskQueueUser(HttpUser):
    """
    Simulates concurrent users generating workload against the task queue system.
    Traffic distribution:
      - 80% Enqueue requests (POST /api/v1/tasks)
      - 20% Status polling (GET /api/v1/tasks/{task_id})
    """

    wait_time = between(0.05, 0.2)

    def on_start(self):
        """Initialize local task buffer for polling."""
        self.submitted_tasks = deque(maxlen=200)

    @task(8)
    def submit_task(self):
        """Submit a background job (80% traffic weight)."""
        task_types = ["IMAGE_RESIZE", "REPORT_GEN"]
        selected_type = random.choice(task_types)

        if selected_type == "IMAGE_RESIZE":
            payload = {
                "task_type": "IMAGE_RESIZE",
                "payload": {
                    "image_url": f"https://s3.amazonaws.com/uploads/photo_{random.randint(1000, 9999)}.png",
                    "width": random.choice([640, 800, 1080, 1920]),
                    "height": random.choice([480, 600, 720, 1080]),
                    "quality": random.randint(70, 95),
                    "simulate_delay_seconds": 0.01,
                },
            }
        else:
            payload = {
                "task_type": "REPORT_GEN",
                "payload": {
                    "report_name": f"Financial_Report_{random.randint(100, 999)}",
                    "date_range": "2026-Q1",
                    "format": random.choice(["PDF", "CSV", "JSON"]),
                    "simulate_delay_seconds": 0.01,
                },
            }

        with self.client.post(
            "/api/v1/tasks",
            json=payload,
            name="POST /api/v1/tasks",
            catch_response=True,
        ) as response:
            if response.status_code == 202:
                try:
                    data = response.json()
                    task_id = data.get("task_id")
                    if task_id:
                        self.submitted_tasks.append(task_id)
                    response.success()
                except Exception as exc:
                    response.failure(f"Invalid JSON response: {exc}")
            else:
                response.failure(f"Expected HTTP 202, got {response.status_code}")

    @task(2)
    def poll_task_status(self):
        """Poll the status of an existing task (20% traffic weight)."""
        if self.submitted_tasks:
            # Pick a previously submitted task
            task_id = random.choice(self.submitted_tasks)
            with self.client.get(
                f"/api/v1/tasks/{task_id}",
                name="GET /api/v1/tasks/[id]",
                catch_response=True,
            ) as response:
                if response.status_code in [200, 404]:
                    # 200 is expected; 404 can happen if TTL expired
                    response.success()
                else:
                    response.failure(f"Unexpected status: {response.status_code}")
        else:
            # If no tasks yet submitted by this user, poll metrics
            with self.client.get(
                "/api/v1/metrics",
                name="GET /api/v1/metrics",
                catch_response=True,
            ) as response:
                if response.status_code == 200:
                    response.success()
                else:
                    response.failure(f"Metrics endpoint failed with {response.status_code}")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Log performance summary upon test completion."""
    print("\n" + "=" * 60)
    print("LOCUST LOAD TEST COMPLETED")
    print("=" * 60)
    stats = environment.runner.stats.total
    print(f"Total Requests:      {stats.num_requests}")
    print(f"Total Failures:      {stats.num_failures} ({stats.fail_ratio * 100:.2f}%)")
    print(f"Median Latency (p50): {stats.median_response_time} ms")
    print(f"90th Percentile (p90): {stats.get_response_time_percentile(0.90)} ms")
    print(f"99th Percentile (p99): {stats.get_response_time_percentile(0.99)} ms")
    print("=" * 60 + "\n")
