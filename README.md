# Concurrency-Safe Ticket Booking System

An event-ticketing backend built around two problems most DBMS coursework treats as one:

1. **"Two people must never win the same seat."** — Solved with Postgres row-level locking
   (`SELECT ... FOR UPDATE SKIP LOCKED` for holds, `FOR UPDATE` for confirms), a `UNIQUE(seat_id)`
   constraint on bookings as a hard backstop, and idempotency keys so a retried request can never
   create a duplicate booking.
2. **"Ten thousand people must never hit the database in the same second."** — A completely
   different problem that pure transaction isolation doesn't solve. A Redis-backed virtual waiting
   room admits users at a controlled rate before they ever reach the booking endpoints — the same
   pattern Ticketmaster's Queue-It and BookMyShow use for high-demand drops.

Most versions of this project stop at problem 1 and call it "concurrency-safe." This one proves
both, empirically, with a stress-test harness that fires real concurrent HTTP requests rather than
just asserting the schema looks right.

## Why `SELECT ... FOR UPDATE SKIP LOCKED` instead of plain `FOR UPDATE`

If 100 people click the same seat at the same instant with plain `FOR UPDATE`, one transaction
wins the row lock and the other 99 **block**, waiting on a lock that will resolve to "unavailable"
for every one of them anyway — 99 held-open DB connections for nothing. `SKIP LOCKED` means any
transaction that can't immediately acquire the row lock just gets zero rows back and fails fast.
Same correctness guarantee, far better behavior under real contention. Confirms use plain
`FOR UPDATE` deliberately — that step happens once per successful hold, not during the initial
click flood, so waiting briefly for the (rare) concurrent sweep/release is the right trade-off there.

## Architecture

```
┌────────────────┐   HTTP + WebSocket   ┌───────────────────────┐
│  Web client     │◀────────────────────▶│   FastAPI backend      │
│  (live seat map)│                       │  /events  /seats       │
└────────────────┘                       │  /seats/{id}/hold      │
                                          │  /bookings/confirm     │
                                          │  /events/{id}/queue/*  │
                                          │  /ws/events/{id}       │
                                          └──────────┬────────────┘
                                                      │
                                   ┌──────────────────┼───────────────────┐
                                   ▼                                     ▼
                        PostgreSQL (asyncpg)                  Redis (virtual queue)
                        row locks, UNIQUE constraints,          sorted-set FIFO queue,
                        idempotency keys                        TTL-based admission
```

## The virtual waiting room

Two Redis structures per event:
- `queue:{event_id}` — a sorted set, `ZADD` with score = join timestamp, so `ZRANGE` gives strict
  FIFO order.
- `admitted:{event_id}:{token}` — a key with a TTL (3 minutes). Its mere existence *is* the
  admission; it self-expires, so an admitted-but-idle user is automatically bumped back to the
  back of the queue instead of squatting on a slot forever.

A background sweep (`sweep_queue_admissions` in `app.py`) promotes a fixed batch size off the front
of the queue on a fixed interval — that batch size and interval *are* the rate limit protecting the
database from a thundering herd. Booking endpoints check admission status before allowing a hold on
any event flagged `high_demand`.

## Proof, not just a claim: the stress test

`stress_test/concurrency_test.py` fires N simulated users at the *same* seat simultaneously via
`asyncio.gather` (a real race, not a simulated one) and checks how many end up with a confirmed
booking. Actual runs from this repo:

| Scenario | Concurrent users | Seats held | Bookings confirmed | Result |
|---|---|---|---|---|
| Direct booking (no queue) | 50 | 1 | 1 | ✅ zero double-booking |
| Through the virtual queue, high-demand event | 200 | 1 | 1 | ✅ zero double-booking, queue gate held |

```
$ python concurrency_test.py --seat-id 594 --event-id 2 --n 50
Fired 50 concurrent users at seat 594 in 0.08s
  Seats successfully held:     1
  Bookings successfully confirmed: 1
  PASS -- exactly one booking succeeded, as required.
  Rejected hold attempts: 49 (status codes: [409])
```

Cross-checked directly against Postgres afterward (`SELECT COUNT(*) FROM bookings WHERE seat_id = ...`)
to rule out an application-layer bug masking a real double-booking at the database level.

## Project layout

```
ticket-booking/
├── db/
│   ├── schema.sql       events, seats, bookings, seat_events_log
│   └── seed.py          seeds two sample events (one high-demand, one not)
├── backend/
│   ├── app.py            FastAPI app: booking flow, queue endpoints, WebSocket broadcast
│   ├── db.py              asyncpg connection pool
│   └── queue_manager.py   Redis virtual waiting room
├── stress_test/
│   └── concurrency_test.py   fires concurrent requests at one seat, verifies the result
└── web/
    └── index.html         self-contained live seat map (WebSocket-driven, no build step)
```

## Setup

```bash
# 1. Database
createdb ticketbooking
psql ticketbooking -f db/schema.sql
pip install -r db/requirements.txt
python db/seed.py

# 2. Redis (for the virtual queue)
redis-server &   # or: brew services start redis

# 3. Backend
pip install -r backend/requirements.txt
cd backend && uvicorn app:app --port 8000

# 4. Web client
cd web && python -m http.server 8080
# open http://127.0.0.1:8080/index.html?event_id=2   (event 2 = no queue gate)
# open http://127.0.0.1:8080/index.html?event_id=1   (event 1 = high-demand, queue gate active)

# 5. Stress test (with the backend running)
pip install -r stress_test/requirements.txt
python stress_test/concurrency_test.py --seat-id <id> --event-id 2 --n 50
```

## Stack

- **Database:** PostgreSQL, `asyncpg` (async driver, matches the async web framework so the event
  loop is never blocked under concurrent load)
- **Backend:** FastAPI, WebSockets for live seat-map broadcast
- **Queue:** Redis (sorted sets for FIFO ordering, TTL keys for self-expiring admission)
- **Client:** self-contained HTML/JS, no build step, live-updates via WebSocket
- **Testing:** `asyncio` + `httpx` concurrency stress harness

Originally a MySQL/JDBC DBMS-lab project (team of 6); rebuilt solo around PostgreSQL's more
expressive locking primitives, an async backend, a distributed-systems queueing layer, and an
empirical concurrency test in place of "the schema looks correct."
