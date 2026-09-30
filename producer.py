"""Root entrypoint to run the FastAPI Producer server."""

import uvicorn
from src.config import settings
from src.producer import app

if __name__ == "__main__":
    print(f"Starting Producer API on {settings.API_HOST}:{settings.API_PORT}...")
    uvicorn.run(
        "src.producer:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=False,
        log_level="info",
    )
