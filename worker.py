"""Root entrypoint to run the Consumer Worker process."""

import argparse
import os
import socket
from src.worker import TaskWorker

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Task Queue Consumer Worker")
    parser.add_argument(
        "--worker-id",
        type=str,
        default=os.environ.get("WORKER_ID", f"{socket.gethostname()}-{os.getpid()}"),
        help="Unique identifier for this worker node",
    )
    args = parser.parse_args()

    worker = TaskWorker(worker_id=args.worker_id)
    worker.run()
