"""
Concurrency-safe ticket booking backend.

Two independent problems, two independent solutions:

  1. "Two people must never win the same seat."
     Solved with Postgres row-level locking (SELECT ... FOR UPDATE SKIP LOCKED
     for holds, FOR UPDATE for confirms) plus a UNIQUE(seat_id) constraint on
     bookings as a hard backstop, plus idempotency keys so a retried request
     can never create a duplicate booking.

  2. "Ten thousand people must never hit the database in the same second."
     Solved with a Redis-backed virtual waiting room (queue_manager.py) that
     admits users at a controlled rate, the same pattern real ticketing
     platforms use for high-demand drops.

Run: uvicorn app:app --reload --port 8000
"""
import asyncio
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import db
from queue_manager import QueueManager, ADMIT_SWEEP_INTERVAL

HOLD_DURATION_SECONDS = 5 * 60
HOLD_SWEEP_INTERVAL = 5.0

redis_client = redis.Redis(host="localhost", port=6379, decode_responses=False)
queue_manager = QueueManager(redis_client)


# --- WebSocket fan-out: one set of live connections per event ---------------
class ConnectionManager:
    def __init__(self):
        self.connections: dict[int, set[WebSocket]] = {}

    async def connect(self, event_id: int, ws: WebSocket):
        await ws.accept()
        self.connections.setdefault(event_id, set()).add(ws)

    def disconnect(self, event_id: int, ws: WebSocket):
        self.connections.get(event_id, set()).discard(ws)

    async def broadcast(self, event_id: int, message: dict):
        dead = []
        for ws in self.connections.get(event_id, set()):
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(event_id, ws)


manager = ConnectionManager()


# --- Background sweepers -----------------------------------------------------
async def sweep_expired_holds():
    """Releases holds whose TTL has passed, so abandoned carts free up seats."""
    while True:
        await asyncio.sleep(HOLD_SWEEP_INTERVAL)
        try:
            pool = db.get_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "UPDATE seats SET status='available', held_by=NULL, held_until=NULL, version=version+1 "
                    "WHERE status='held' AND held_until < now() "
                    "RETURNING id, event_id"
                )
            for row in rows:
                await manager.broadcast(row["event_id"], {"seat_id": row["id"], "status": "available"})
        except Exception as e:
            print(f"[sweep_expired_holds] error: {e}")


async def sweep_queue_admissions():
    """Admits the next batch of waiting users for every high-demand event, on a fixed interval."""
    while True:
        await asyncio.sleep(ADMIT_SWEEP_INTERVAL)
        try:
            pool = db.get_pool()
            async with pool.acquire() as conn:
                events = await conn.fetch("SELECT id FROM events WHERE high_demand = TRUE")
            for ev in events:
                await queue_manager.admit_next_batch(ev["id"])
        except Exception as e:
            print(f"[sweep_queue_admissions] error: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_pool()
    hold_task = asyncio.create_task(sweep_expired_holds())
    queue_task = asyncio.create_task(sweep_queue_admissions())
    yield
    hold_task.cancel()
    queue_task.cancel()
    await db.close_pool()
    await redis_client.close()


app = FastAPI(title="Concurrency-Safe Ticket Booking API", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# --- Schemas ------------------------------------------------------------------
class JoinQueueRequest(BaseModel):
    token: str


class HoldRequest(BaseModel):
    user_token: str


class ConfirmRequest(BaseModel):
    seat_id: int
    user_token: str
    idempotency_key: str | None = None


# --- Events & seat map ---------------------------------------------------------
@app.get("/events")
async def list_events():
    pool = db.get_pool()
    rows = await pool.fetch("SELECT id, name, venue, event_time, high_demand FROM events ORDER BY event_time")
    return [dict(r) for r in rows]


@app.get("/events/{event_id}")
async def get_event(event_id: int):
    pool = db.get_pool()
    row = await pool.fetchrow("SELECT id, name, venue, event_time, high_demand FROM events WHERE id=$1", event_id)
    if row is None:
        raise HTTPException(404, "Event not found")
    return dict(row)


@app.get("/events/{event_id}/seats")
async def get_seat_map(event_id: int):
    pool = db.get_pool()
    rows = await pool.fetch(
        "SELECT id, section, row_label, seat_number, price_cents, status "
        "FROM seats WHERE event_id=$1 ORDER BY section, row_label, seat_number",
        event_id,
    )
    return [dict(r) for r in rows]


# --- Virtual waiting room ------------------------------------------------------
@app.post("/events/{event_id}/queue/join")
async def join_queue(event_id: int, req: JoinQueueRequest):
    await queue_manager.join(event_id, req.token)
    status = await queue_manager.status(event_id, req.token)
    return {"admitted": status.admitted, "position": status.position}


@app.get("/events/{event_id}/queue/status")
async def queue_status(event_id: int, token: str):
    status = await queue_manager.status(event_id, token)
    return {"admitted": status.admitted, "position": status.position}


# --- Booking flow: hold -> confirm --------------------------------------------
@app.post("/seats/{seat_id}/hold")
async def hold_seat(seat_id: int, req: HoldRequest):
    pool = db.get_pool()

    async with pool.acquire() as conn:
        seat_row = await conn.fetchrow("SELECT event_id FROM seats WHERE id=$1", seat_id)
        if seat_row is None:
            raise HTTPException(404, "Seat not found")
        event_id = seat_row["event_id"]

        event_row = await conn.fetchrow("SELECT high_demand FROM events WHERE id=$1", event_id)
        if event_row["high_demand"] and not await queue_manager.is_admitted(event_id, req.user_token):
            raise HTTPException(403, "Join the queue first: not yet admitted for this event.")

        async with conn.transaction():
            # SKIP LOCKED: if another request is *right now* mid-transaction on this
            # exact seat, don't wait for it -- just report unavailable immediately.
            # Waiting here would mean N concurrent clicks on one hot seat pile up
            # into N blocked DB connections, all destined to fail anyway.
            row = await conn.fetchrow(
                "SELECT id, status, held_until FROM seats WHERE id=$1 FOR UPDATE SKIP LOCKED",
                seat_id,
            )
            if row is None:
                raise HTTPException(409, "Seat is currently being processed by another request. Try again.")

            now = datetime.now(timezone.utc)
            still_held = row["status"] == "held" and row["held_until"] and row["held_until"] > now
            if row["status"] == "booked" or still_held:
                raise HTTPException(409, "Seat is no longer available.")

            held_until = now + timedelta(seconds=HOLD_DURATION_SECONDS)
            await conn.execute(
                "UPDATE seats SET status='held', held_by=$1, held_until=$2, version=version+1 WHERE id=$3",
                req.user_token, held_until, seat_id,
            )
            await conn.execute(
                "INSERT INTO seat_events_log (seat_id, event_type, actor) VALUES ($1, 'held', $2)",
                seat_id, req.user_token,
            )

    await manager.broadcast(event_id, {"seat_id": seat_id, "status": "held"})
    return {"seat_id": seat_id, "held_until": held_until.isoformat(), "hold_seconds": HOLD_DURATION_SECONDS}


@app.post("/seats/{seat_id}/release")
async def release_seat(seat_id: int, req: HoldRequest):
    pool = db.get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE seats SET status='available', held_by=NULL, held_until=NULL, version=version+1 "
            "WHERE id=$1 AND held_by=$2 AND status='held' RETURNING event_id",
            seat_id, req.user_token,
        )
    if row is None:
        raise HTTPException(409, "Seat was not held by you.")
    await manager.broadcast(row["event_id"], {"seat_id": seat_id, "status": "available"})
    return {"seat_id": seat_id, "status": "available"}


@app.post("/bookings/confirm")
async def confirm_booking(req: ConfirmRequest):
    pool = db.get_pool()
    idempotency_key = req.idempotency_key or str(uuid.uuid4())

    async with pool.acquire() as conn:
        async with conn.transaction():
            existing = await conn.fetchrow(
                "SELECT * FROM bookings WHERE idempotency_key=$1", idempotency_key
            )
            if existing:
                # Replaying the same confirm request (e.g. a client retry after a
                # dropped response) returns the original booking -- never a new one.
                return dict(existing)

            row = await conn.fetchrow(
                "SELECT id, event_id, status, held_by, held_until, price_cents "
                "FROM seats WHERE id=$1 FOR UPDATE",
                req.seat_id,
            )
            if row is None:
                raise HTTPException(404, "Seat not found")

            now = datetime.now(timezone.utc)
            valid_hold = (
                row["status"] == "held"
                and row["held_by"] == req.user_token
                and row["held_until"] and row["held_until"] > now
            )
            if not valid_hold:
                raise HTTPException(409, "Your hold on this seat has expired or is invalid.")

            try:
                booking = await conn.fetchrow(
                    "INSERT INTO bookings (event_id, seat_id, user_token, idempotency_key, price_cents) "
                    "VALUES ($1, $2, $3, $4, $5) RETURNING *",
                    row["event_id"], req.seat_id, req.user_token, idempotency_key, row["price_cents"],
                )
            except asyncpg.UniqueViolationError:
                # Should be unreachable given the FOR UPDATE above -- kept as a hard
                # backstop so a future refactor that weakens the locking can't
                # silently reintroduce double-booking.
                raise HTTPException(409, "Seat was already booked.")

            await conn.execute(
                "UPDATE seats SET status='booked', held_by=NULL, held_until=NULL, version=version+1 WHERE id=$1",
                req.seat_id,
            )
            await conn.execute(
                "INSERT INTO seat_events_log (seat_id, event_type, actor) VALUES ($1, 'booked', $2)",
                req.seat_id, req.user_token,
            )

    await manager.broadcast(row["event_id"], {"seat_id": req.seat_id, "status": "booked"})
    return dict(booking)


# --- Live seat-map updates ------------------------------------------------------
@app.websocket("/ws/events/{event_id}")
async def seat_map_ws(websocket: WebSocket, event_id: int):
    await manager.connect(event_id, websocket)
    try:
        while True:
            await websocket.receive_text()  # client doesn't need to send anything; keeps the socket open
    except WebSocketDisconnect:
        manager.disconnect(event_id, websocket)


@app.get("/health")
async def health():
    return {"status": "ok", "time": time.time()}
