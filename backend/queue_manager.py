"""
Redis-backed virtual waiting room -- the "novel" piece of this project.

Most DBMS-course ticket-booking projects stop at "transactions prevent double
booking." Real ticket platforms (Ticketmaster's Queue-It, BookMyShow during a
high-demand drop) have a second problem entirely: tens of thousands of people
hitting "book" in the same second would melt the database even if every
transaction is perfectly correct. Their answer is a virtual waiting room: you
queue in join-order, and are admitted into the actual booking flow at a
controlled rate.

This module implements that pattern with two Redis structures per event:
  - a sorted set `queue:{event_id}`        -- ZADD with score = join timestamp,
                                                so ZRANGE gives strict FIFO order.
  - admission keys `admitted:{event_id}:{token}` -- a plain key with a TTL;
                                                its existence *is* the admission,
                                                and it self-expires (SETEX),
                                                so an idle admitted user is
                                                automatically bumped back to
                                                needing to rejoin.

An `admit_next_batch` sweep (run on a timer by the FastAPI app, see app.py)
promotes the front of the queue into admitted status at a fixed rate --
that rate is the throttle that protects the database from a thundering herd.
"""
import time
from dataclasses import dataclass

import redis.asyncio as redis

ADMISSION_TTL_SECONDS = 180   # once admitted, you have 3 minutes to complete a booking
ADMIT_BATCH_SIZE = 5          # how many people to let in per sweep
ADMIT_SWEEP_INTERVAL = 2.0    # seconds between sweeps -- the actual admission *rate*


def _queue_key(event_id: int) -> str:
    return f"queue:{event_id}"


def _admitted_key(event_id: int, token: str) -> str:
    return f"admitted:{event_id}:{token}"


@dataclass
class QueueStatus:
    admitted: bool
    position: int | None   # 1-based position among still-waiting tokens, None if admitted or not queued


class QueueManager:
    def __init__(self, redis_client: "redis.Redis"):
        self.r = redis_client

    async def join(self, event_id: int, token: str) -> None:
        """Idempotent: joining twice just keeps the original (earliest) position."""
        await self.r.zadd(_queue_key(event_id), {token: time.time()}, nx=True)

    async def status(self, event_id: int, token: str) -> QueueStatus:
        if await self.r.exists(_admitted_key(event_id, token)):
            return QueueStatus(admitted=True, position=None)

        rank = await self.r.zrank(_queue_key(event_id), token)
        if rank is None:
            return QueueStatus(admitted=False, position=None)  # never joined
        return QueueStatus(admitted=False, position=rank + 1)

    async def admit_next_batch(self, event_id: int, batch_size: int = ADMIT_BATCH_SIZE) -> list[str]:
        """
        Promote the front of the queue to admitted. Called on a timer (see
        app.py's background sweep task), not per-request -- the fixed
        batch size + interval *is* the rate limit that protects the DB.
        """
        key = _queue_key(event_id)
        tokens = await self.r.zrange(key, 0, batch_size - 1)
        if not tokens:
            return []

        pipe = self.r.pipeline()
        for raw in tokens:
            token = raw.decode() if isinstance(raw, bytes) else raw
            pipe.setex(_admitted_key(event_id, token), ADMISSION_TTL_SECONDS, "1")
            pipe.zrem(key, token)
        await pipe.execute()
        return [t.decode() if isinstance(t, bytes) else t for t in tokens]

    async def is_admitted(self, event_id: int, token: str) -> bool:
        return bool(await self.r.exists(_admitted_key(event_id, token)))

    async def queue_length(self, event_id: int) -> int:
        return await self.r.zcard(_queue_key(event_id))
