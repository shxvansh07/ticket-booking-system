"""asyncpg connection pool -- shared across the app so we never block the
event loop on a synchronous DB driver under concurrent load."""
import asyncpg

DSN = "postgresql:///ticketbooking"

_pool: asyncpg.Pool | None = None


async def init_pool() -> asyncpg.Pool:
    global _pool
    _pool = await asyncpg.create_pool(DSN, min_size=2, max_size=20)
    return _pool


async def close_pool() -> None:
    if _pool is not None:
        await _pool.close()


def get_pool() -> asyncpg.Pool:
    assert _pool is not None, "call init_pool() at startup first"
    return _pool
