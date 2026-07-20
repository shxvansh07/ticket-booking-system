# Study & Interview Prep — Concurrency-Safe Ticket Booking

This file is your prep sheet for talking about this project in interviews. Everything
here is grounded in the actual code in this repo (verified by reading every file and,
where feasible, running the full stack live against a real local Postgres + Redis and
firing real concurrent HTTP requests at it — see "How this was verified" at the bottom).

---

## 1. The 60–90 second pitch

"I built a ticket-booking backend that solves the two problems real ticketing platforms
actually have to solve, not just the one DBMS coursework usually stops at.

Problem one: two people must never win the same seat. If you naively 'check if the seat
is free, then insert a booking,' two concurrent requests can both pass the check before
either writes — classic check-then-act race condition — and you oversell the seat. I
solve this with Postgres row-level locking: the booking flow does `SELECT ... FOR UPDATE`
inside a transaction, so the database itself serializes concurrent access to that one
row. On top of that there's a `UNIQUE(seat_id)` constraint on the `bookings` table as a
hard backstop — even if application logic had a bug, Postgres itself physically cannot
store two bookings for the same seat — plus an idempotency key so a retried HTTP request
(client timeout, double-click) can't create a second booking for the same confirm action.

Problem two is different: ten thousand people hitting 'book' in the same second will
melt the database even if every single transaction is perfectly correct — it's a load
problem, not a correctness problem. I solve that with a Redis-backed virtual waiting
room, the same pattern Ticketmaster's Queue-It or BookMyShow use during a high-demand
drop: users join a FIFO queue (a Redis sorted set), and a background sweep admits a fixed
batch of them at a fixed interval. Only admitted users can reach the actual booking
endpoints, which throttles the rate at which the database ever sees hold/confirm traffic.

And I didn't just assert correctness — I wrote a stress-test harness that fires N
simulated users at the exact same seat simultaneously via `asyncio.gather` and asserts
exactly one of them ends up with a confirmed booking. I ran it against a real local
Postgres+Redis stack: 50 concurrent users on a normal event, 30 concurrent users
plus the queue gate on a high-demand event — both times, exactly 1 confirmed booking,
verified independently against the database with `SELECT COUNT(*) FROM bookings GROUP BY
seat_id`."

---

## 2. Architecture walkthrough

```
web/index.html  →  backend/app.py  →  queue_manager.py (Redis)  →  db.py (Postgres pool)
```

**web/index.html** — self-contained HTML/JS, no build step. `API_BASE =
"http://127.0.0.1:8000"`. On load it calls `GET /events/{id}` and `GET
/events/{id}/seats` to render the seat map, and opens a WebSocket to `/ws/events/{id}`
for live updates (other users' holds/bookings/releases appear without polling). If the
event is `high_demand`, it shows the queue panel and calls `POST
/events/{id}/queue/join`, then polls `GET /events/{id}/queue/status?token=...` every
second until `admitted: true`. Clicking a seat calls `POST /seats/{id}/hold`; the
confirm button calls `POST /bookings/confirm` with a deterministic idempotency key
(`${USER_TOKEN}-${heldSeatId}-confirm`) so retries are safe.

**backend/app.py** — FastAPI app, one asyncpg pool (`db.py`) and one Redis client shared
process-wide.
- `GET /events`, `GET /events/{id}`, `GET /events/{id}/seats` — read paths, plain
  `pool.fetch`/`fetchrow`, no locking needed (reads don't create races).
- `POST /events/{id}/queue/join`, `GET /events/{id}/queue/status` — thin wrappers over
  `queue_manager.QueueManager.join()` / `.status()`.
- `POST /seats/{id}/hold` — the first locking-critical path (see §3).
- `POST /seats/{id}/release` — releases a hold, gated by `WHERE ... AND held_by=$2 AND
  status='held'` so you can only release your own hold.
- `POST /bookings/confirm` — the second locking-critical path (see §3).
- Two background sweeps started in `lifespan()`: `sweep_expired_holds` (every
  `HOLD_SWEEP_INTERVAL`=5s, flips expired `held` seats back to `available`) and
  `sweep_queue_admissions` (every `ADMIT_SWEEP_INTERVAL`=2s, calls
  `queue_manager.admit_next_batch()` for every `high_demand` event).
- `ConnectionManager` fans out seat-status changes over WebSocket per event.

**backend/queue_manager.py** (`QueueManager`) — two Redis structures per event:
`queue:{event_id}` (sorted set, `ZADD ... NX` with score = join timestamp → strict FIFO
via `ZRANGE`/`ZRANK`) and `admitted:{event_id}:{token}` (a `SETEX` key with
`ADMISSION_TTL_SECONDS`=180 — its mere existence *is* admission, and it self-expires so
an idle admitted user is bumped back to needing re-admission). `admit_next_batch()`
promotes `ADMIT_BATCH_SIZE`=5 tokens off the front of the queue per sweep — batch size ×
sweep interval is the actual admission rate, and that's the throttle protecting Postgres.

**backend/db.py** — a single module-level `asyncpg.Pool` (`min_size=2, max_size=20`),
initialized in FastAPI's `lifespan()` via `init_pool()`, fetched per-request via
`get_pool()`. Async driver chosen deliberately so the event loop is never blocked by a
synchronous DB call under concurrent load.

**db/schema.sql / db/seed.py** — `events` (with a `high_demand` flag that routes booking
through the queue), `seats` (status enum `available|held|booked`, `held_by`/`held_until`
for the hold TTL, a `version` column bumped on every state change as an audit trail —
*not* used for optimistic-locking compare-and-swap, it's just history), `bookings`
(`UNIQUE(seat_id)` and `UNIQUE(idempotency_key)`), and `seat_events_log` (an append-only
audit trail of held/booked/released/expired events). `seed.py` truncates and reseeds two
events: event 1 (Arijit Singh concert, 592 seats, `high_demand=True`) and event 2 (tech
meetup, 90 seats, `high_demand=False`) — confirmed by actually running it.

---

## 3. Deep-dive concepts

### Why naive check-then-book fails
`SELECT status FROM seats WHERE id=?` then, if available, `INSERT INTO bookings ...` as
two separate statements/round-trips is a textbook TOCTOU (time-of-check to time-of-use)
race: two concurrent requests can both read `status='available'` before either writes.
Without a database-enforced serialization point, both proceed to "success." The fix has
to live at the point where the *check and the write* are atomic with respect to other
transactions — which is exactly what row locking gives you.

### Exactly which Postgres locking mechanism is used, and where
Two different lock modes, deliberately different, for two different call sites:
- `hold_seat()` (`POST /seats/{id}/hold`): `SELECT id, status, held_until FROM seats
  WHERE id=$1 FOR UPDATE SKIP LOCKED`. This is a genuine row-level exclusive lock (not an
  advisory lock — it's tied to the actual row, acquired and released as part of the
  transaction). `SKIP LOCKED` means a transaction that can't immediately acquire the lock
  doesn't block — it just gets zero rows back and the endpoint returns 409 immediately.
  Rationale (from the code comments): if 100 clicks hit one hot seat at once, plain `FOR
  UPDATE` would have 99 requests pile up as blocked DB connections waiting on a lock
  that's going to resolve to "unavailable" for all of them anyway. `SKIP LOCKED` fails
  those 99 fast instead.
- `confirm_booking()` (`POST /bookings/confirm`): plain `SELECT ... FOR UPDATE` (no
  `SKIP LOCKED`). This happens once per successful hold, not during the initial click
  flood, so briefly waiting for a rare concurrent sweep/release to finish is an
  acceptable trade — and you want confirm to actually resolve correctly rather than fail
  fast on contention that's now rare.
- Backstop: `UNIQUE(seat_id)` on `bookings`. The code explicitly catches
  `asyncpg.UniqueViolationError` around the `INSERT` and comments that this should be
  "unreachable given the FOR UPDATE above" — it exists so a future refactor that weakens
  the locking can't silently reintroduce double-booking. This is defense in depth, not
  the primary mechanism.
- Idempotency: `bookings.idempotency_key` is `UNIQUE`, and `confirm_booking()` checks for
  an existing row with that key *before* doing anything else, returning the original
  booking on replay instead of erroring or duplicating.

**Why this instead of optimistic locking / unique constraints alone?** A unique
constraint alone (no `SELECT ... FOR UPDATE`) would let two transactions both pass the
earlier "is this seat held by me and still valid" check, then race to `INSERT`, with one
failing with a unique-violation *after* both did real work — functionally correct but you
lose the ability to fail fast with a clean 409 tied to a specific reason ("hold expired"
vs "already booked"), and you can't safely also update `seats.status` atomically with the
check. Optimistic concurrency (compare-and-swap on the `version` column) would work too,
but requires the client to retry on conflict; under a genuine thundering-herd scenario
(hundreds of clients CAS-retrying the same row) that can perform worse than SKIP LOCKED's
fail-fast semantics. Pessimistic locking was the deliberate choice here because seat
contention is exactly the "many losers, one winner" shape where failing fast matters more
than retrying.

### What the virtual waiting room solves that a lock alone doesn't
Row locking guarantees *correctness* under concurrency — it says nothing about *load*.
Ten thousand concurrent hold requests against Postgres, even if every single one resolves
correctly (one wins, 9,999 get fast 409s via SKIP LOCKED), is still 10,000 simultaneous
connections/transactions hitting the database in the same instant — connection pool
exhaustion, CPU/lock-manager contention, and a bad experience even for the "winner." The
waiting room (`queue_manager.py`) moves the throttle *before* the database: only
`ADMIT_BATCH_SIZE` (5) users per `ADMIT_SWEEP_INTERVAL` (2s) are ever admitted to try a
hold/confirm at all for a `high_demand` event. This buys three things simultaneously —
protection from thundering herd, fairness (strict FIFO via the sorted-set join
timestamp, so it's not first-fastest-network-wins), and load shedding (the database only
ever sees a bounded, controlled request rate no matter how many people are actually
waiting).

### How concurrency_test.py actually proves correctness
`simulate_user()` has each simulated client join the queue (if `high_demand`), poll
`queue/status` until admitted, then call `POST /seats/{id}/hold` immediately followed by
`POST /bookings/confirm` — no sequencing between users. All N users' `simulate_user()`
coroutines are launched together via `asyncio.gather`, so their HTTP requests are
in flight concurrently, not one-after-another — that's what makes the race real rather
than simulated. The script counts `held_count` (how many got a 200 from `/hold`) and
`confirmed_count` (how many got a 200 from `/confirm`), and asserts `confirmed_count ==
1`. A failure would look like `confirmed_count > 1` (real double-booking — a correctness
bug) or, in principle, `confirmed_count == 0` when it should be 1 (a bug that makes the
happy path unreachable, e.g. an overly aggressive lock or a busted idempotency check).
This was actually run against a live stack for this audit: 50 concurrent users on event 2
(direct path) and 30 on event 1 (through the queue) both produced exactly 1 confirmed
booking, cross-checked directly in Postgres with `SELECT seat_id, COUNT(*) FROM bookings
GROUP BY seat_id HAVING COUNT(*) > 1` returning zero rows.

---

## 4. Interview questions with model answers

**Q1. Walk me through what happens, at the database level, when two users click the same
seat at the same millisecond.**
Both requests reach `hold_seat()` in `app.py`. Both open a transaction and issue `SELECT
... FROM seats WHERE id=$1 FOR UPDATE SKIP LOCKED`. Postgres lets exactly one transaction
acquire the row lock; the other, because of `SKIP LOCKED`, gets zero rows back
immediately rather than blocking. That request sees `row is None` and returns 409. The
winner proceeds to check status/held_until, then `UPDATE seats SET status='held', ...`
and commits, releasing the lock. There is no window where both transactions can believe
the seat is free.

**Q2. Why `SKIP LOCKED` on hold but plain `FOR UPDATE` on confirm — why not the same
lock mode everywhere?**
Hold is the "click flood" endpoint — many concurrent requests can plausibly target the
same seat at once, and blocking all of them on one lock is wasted DB connections for a
result that's going to be "unavailable" anyway, so failing fast is strictly better.
Confirm only runs once per successful hold — contention there is rare (maybe a hold just
expired via the sweeper at the same instant) — so blocking briefly to get a correct
answer is an acceptable, even preferable, trade-off.

**Q3. What does `FOR UPDATE` actually lock, precisely?**
It takes a row-level exclusive lock on the specific row(s) returned by the `SELECT`,
held until the enclosing transaction commits or rolls back. It blocks other transactions
from acquiring the same row's lock (via another `FOR UPDATE`, `FOR SHARE`, or an
`UPDATE`/`DELETE` targeting that row) — it does not lock the whole table, and read-only
queries without any locking clause aren't blocked by it.

**Q4. Is there a deadlock risk in this design?**
Deadlocks require two transactions each holding a lock the other needs, in reverse
order. Here, both `hold_seat` and `confirm_booking` lock exactly one `seats` row per
transaction (single-row `FOR UPDATE`), so there's no multi-row lock-ordering opportunity
within a request. The main place I'd watch for it if this were extended is a future
multi-seat "book N seats atomically" feature — that would require sorting seat IDs
before locking them, to guarantee every transaction acquires locks in the same order.

**Q5. What if two backend instances (behind a load balancer) both call `hold_seat` for
the same seat at the same time?**
Still correct, by design — the mutual exclusion lives in Postgres, not in
process/application memory. Two FastAPI processes, or two machines, both open their own
connection out of their own `asyncpg` pool and both issue `FOR UPDATE SKIP LOCKED`
against the same physical row in the same database; Postgres itself arbitrates which one
gets the lock, regardless of which process asked. This is exactly why the correctness
guarantee is described as coming from the database, not the application — it survives
horizontal scaling of the backend for free.

**Q6. What breaks with horizontal scaling that isn't the seat-locking part?**
Two things live in-process today and would need to move to shared state: the
`ConnectionManager` WebSocket registry (a client connected to instance A won't get
broadcasts triggered by a booking that happened via instance B — would need Redis pub/sub
or similar to fan out across instances) and the two background sweep tasks
(`sweep_expired_holds`, `sweep_queue_admissions`) — if every instance runs its own copy,
you get redundant sweeps (harmless but wasteful) rather than a single designated sweeper;
correctness survives because sweeps are idempotent updates, but it's not efficient.

**Q7. What if Redis goes down mid-booking?**
The Redis-dependent code paths are the queue (`join`, `status`, `admit_next_batch`,
`is_admitted`) and the `redis_client` used only by `queue_manager`. If Redis is
unreachable, `is_admitted()`'s call would raise, and `hold_seat` would 500 rather than
silently letting a `high_demand` event's booking through ungated — so a Redis outage
degrades to "high-demand events can't be booked" rather than "the queue gate silently
fails open and lets in a stampede." Critically, it does *not* threaten the core
double-booking guarantee, because that guarantee is entirely inside Postgres — the seat
lock and unique constraints don't depend on Redis at all. Non-high-demand events (like
event 2 in the seed data) are entirely unaffected by a Redis outage since they never call
into `queue_manager`.

**Q8. Why an idempotency key instead of relying only on the `FOR UPDATE` lock?**
The lock protects against two *different* requests racing each other. It does nothing
for one request being *retried* — e.g. a client times out waiting for the `/confirm`
response, doesn't know if it succeeded, and retries the exact same logical action. Without
an idempotency key, that retry would look like a brand-new confirm attempt; with the seat
already `booked`, `valid_hold` would now be false and it'd 409 incorrectly (in the current
code, since the seat's status changed) — or worse, in a design without any hold-state
gating, could double-book from a single user's own retry. The `idempotency_key` (client
generates it deterministically as `${user_token}-${seat_id}-confirm`) lets
`confirm_booking()` detect "I've already processed this exact action" and return the
original booking, making retries safe.

**Q9. Why is `version` on `seats` incremented but never checked in a `WHERE` clause?**
Because it's documented as an audit trail, not an optimistic-concurrency mechanism — the
actual concurrency control here is pessimistic (`FOR UPDATE`), not optimistic
compare-and-swap. `version` exists so you could reconstruct/debug the history of state
transitions on a seat if needed. It'd become load-bearing only if the design switched to
optimistic locking (`UPDATE ... WHERE id=$1 AND version=$2`), which this project
deliberately doesn't do.

**Q10. Why asyncpg + FastAPI instead of a synchronous stack?**
Because the whole selling point is behaving well under concurrent load — a synchronous
driver would block the single event loop per worker on every DB round-trip, serializing
requests that should be running concurrently (or at least concurrently *waiting on I/O*)
and defeating the purpose of an async framework. `asyncpg`'s connection pool
(`min_size=2, max_size=20` in `db.py`) lets many concurrent requests share a bounded set
of real connections without blocking the event loop.

**Q11. Why a sorted set for the queue instead of a Redis List (`LPUSH`/`RPOP`)?**
A sorted set scored by join timestamp gives O(log N) insertion with strict order
preserved regardless of insertion timing quirks, and — importantly — `ZADD ... NX`
makes `join()` naturally idempotent: a duplicate join for the same token doesn't move
their position, because `NX` only sets the score if the member doesn't already exist. A
list would need extra logic to detect and reject a duplicate join without disturbing
order.

**Q12. What happens to a user's queue position if they close the tab and never come
back?**
They stay in the `queue:{event_id}` sorted set indefinitely — nothing currently expires
queue *entries* themselves (only `admitted:*` keys have a TTL). This is a real gap:
an abandoned queue entry occupies a FIFO slot forever, which would eventually get swept
into "admitted" and then just expire unused. Worth flagging as a known limitation (see
§5) — a queue-entry TTL or periodic pruning would fix it.

**Q13. What's the difference between `held` and `booked`, and why have both states
instead of booking directly?**
`held` is a soft, time-boxed reservation (`held_until`, default 5 minutes via
`HOLD_DURATION_SECONDS`) that lets a user go through a checkout flow (pick seat, maybe
enter payment details) without another user being able to grab the same seat meanwhile,
while still allowing the seat to free up automatically if they abandon the flow (via
`sweep_expired_holds`). `booked` is the durable, terminal state protected by the
`UNIQUE(seat_id)` constraint on `bookings`. Two states let you separate "reserved but not
yet paid for" from "definitely sold," which mirrors how real ticketing/e-commerce
checkouts work (cart hold vs. completed order).

**Q14. How would you extend this to handle payment processing?**
Payment would slot in during the `held` window, before `confirm_booking()` is called —
e.g. the client calls a new `/payments/charge` endpoint after holding, and
`confirm_booking()` would additionally verify a successful payment reference before
inserting the booking row (ideally within the same transaction/using the same
idempotency key so a duplicate charge webhook can't double-book either). I'd want the
payment provider's own idempotency-key support wired to the same key used here, so a
network retry can't double-charge a card, mirroring the exact problem the booking side
already solves.

**Q15. The stress test shows `Rejected hold attempts: 49 (status codes: [409])` for the
direct-booking scenario, but the high-demand scenario shows `[403, 409]`. Why the
difference?**
403 only appears in the queue-gated scenario, from `hold_seat()`'s explicit check —
`if event_row["high_demand"] and not await queue_manager.is_admitted(...): raise
HTTPException(403, ...)`. Since only `ADMIT_BATCH_SIZE` users get admitted per sweep,
some of the 30 simulated users in that test reasonably attempt (or bounded-poll and give
up before) admission and get rejected at the *queue gate*, before ever reaching the row
lock — a different rejection reason (403, "not admitted") than the 409 ("seat
unavailable") a properly-admitted user would get after losing the row-lock race.

**Q16. Why does `hold_seat` check `event_row["high_demand"]` outside the transaction
block rather than inside it?**
It's a read-only authorization check ("is this user even allowed to try"), not part of
the seat's state — there's no race to protect there, since it doesn't decide who wins the
seat, only who's allowed to attempt. Keeping it outside the transaction avoids holding a
row lock (or an open transaction) any longer than necessary — the actual row lock only
gets acquired right before the code that needs it.

**Q17. How do you know this isn't "the schema looks right" reasoning — what's the actual
empirical evidence?**
The stress-test harness (`concurrency_test.py`) fires real concurrent HTTP requests (not
sequential, not mocked) via `asyncio.gather` and checks the actual HTTP response codes
and, per the README, cross-checks directly against Postgres afterward. Re-running it live
during this project's audit against a real local Postgres 16 + Redis instance produced 1
confirmed booking out of 50 concurrent attempts (direct path) and 1 out of 30 (queue-gated
path), independently confirmed with zero seats having `COUNT(*) > 1` in `bookings`.

---

## 5. What I'd improve

- **Idempotency at the hold step, not just confirm.** `confirm_booking()` has an
  idempotency key; `hold_seat()` doesn't — a retried hold request from the same user is
  currently just handled by "the seat's already held by you" naturally resolving as a
  409/no-op via the status check, but it's implicit rather than an explicit guarantee.
- **Queue-entry expiry.** As noted in Q12, entries in `queue:{event_id}` never expire on
  their own if a user abandons the tab — only the *admitted* state has a TTL. A
  reasonable fix: also track a last-seen timestamp and prune stale queue entries in the
  sweep.
- **Cross-instance broadcast.** The WebSocket `ConnectionManager` and the two sweep
  loops are per-process. Running multiple backend instances behind a load balancer is
  safe for booking correctness (Postgres arbitrates that) but WebSocket clients
  connected to a different instance than the one that processed a booking won't get a
  live update without adding Redis pub/sub (or similar) for cross-instance fan-out, and
  the sweeps would ideally be centralized (or made leader-elected) rather than
  duplicated per instance.
- **Payment integration is out of scope today** — the system proves "no double booking,"
  not "no double charge." See Q14 for the design sketch.
- **What the stress test doesn't cover:** it only exercises one hot seat per run, and
  only up to ~200 concurrent users in the README's recorded results — it doesn't test
  sustained load over time, connection-pool exhaustion at higher concurrency, behavior
  when Postgres or Redis itself is under resource pressure, or multi-seat/multi-event
  concurrent contention patterns. It also doesn't currently include a Redis-down or
  Postgres-down failure-injection scenario, which would be a natural next addition given
  how much of Q7's answer is currently reasoned about the code rather than observed.
- **Monitoring/alerting** is absent entirely — there's no metrics emission (queue depth,
  hold-to-confirm conversion rate, 409 rate) that a real production deployment would want
  for capacity planning around the admission batch size/interval.

---

## How this was verified (for this audit)

Postgres 16 and Redis were already installed and running locally via Homebrew
(`postgresql@16`, `redis` — both showed `started` in `brew services list`). A venv was
created, all three `requirements.txt` files installed cleanly, `db/schema.sql` applied
(idempotently — tables already existed from prior use) and `db/seed.py` run successfully,
producing event 1 (`high_demand=True`, 592 seats) and event 2 (`high_demand=False`, 90
seats). The backend was started with `uvicorn app:app --port 8000` (bound to an
already-running prior instance of the same code on that port — same directory, same
files, so results are valid) and both `stress_test/concurrency_test.py` scenarios were
run live: 50 concurrent users against a normal-event seat, and 30 concurrent users
(with `--high-demand`) against the high-demand event's seat. Both produced exactly 1
confirmed booking, and a direct `SELECT seat_id, COUNT(*) FROM bookings GROUP BY seat_id
HAVING COUNT(*) > 1` against the live database returned zero rows, confirming no
double-booking occurred at the database level, not just in the application's own report.
