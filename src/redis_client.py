"""Redis client wrapper and connection management."""

from typing import Optional
import redis
from src.config import settings

_pool: Optional[redis.ConnectionPool] = None
_client_override: Optional[redis.Redis] = None


def get_redis_pool() -> redis.ConnectionPool:
    """Initialize or return existing Redis connection pool."""
    global _pool
    if _pool is None:
        if settings.REDIS_URL:
            _pool = redis.ConnectionPool.from_url(
                settings.REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=3,
                retry_on_timeout=True
            )
        else:
            _pool = redis.ConnectionPool(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                password=settings.REDIS_PASSWORD,
                decode_responses=True,
                socket_connect_timeout=3,
                retry_on_timeout=True
            )
    return _pool


def get_redis_client() -> redis.Redis:
    """Get thread-safe Redis client instance. Allows test overriding."""
    if _client_override is not None:
        return _client_override
    return redis.Redis(connection_pool=get_redis_pool())


def set_redis_client_override(client: Optional[redis.Redis]) -> None:
    """Set or clear a Redis client override (e.g. for fakeredis in tests)."""
    global _client_override
    _client_override = client


def check_redis_health(client: Optional[redis.Redis] = None) -> bool:
    """Ping Redis to verify connectivity."""
    try:
        r = client or get_redis_client()
        return bool(r.ping())
    except (redis.ConnectionError, redis.TimeoutError, Exception):
        return False
