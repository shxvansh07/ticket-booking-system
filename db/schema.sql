-- Concurrency-safe ticket booking schema.
--
-- The core guarantee this schema is built around: a seat can never end up
-- with two confirmed bookings, no matter how many requests hit it at the
-- exact same instant. That guarantee comes from three independent layers,
-- deliberately redundant (defense in depth, not just "trust the app logic"):
--   1. Row-level locking during hold/confirm (SELECT ... FOR UPDATE SKIP LOCKED)
--   2. A UNIQUE constraint on bookings.seat_id -- even a buggy application
--      cannot insert two bookings for the same seat; Postgres itself refuses.
--   3. Idempotency keys on booking confirmation, so a retried/duplicated
--      HTTP request can never create a second booking for the same action.

CREATE TABLE IF NOT EXISTS events (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    venue       TEXT NOT NULL,
    event_time  TIMESTAMPTZ NOT NULL,
    -- events above this size get routed through the virtual waiting room
    -- (see backend/queue_manager.py) instead of direct booking access.
    high_demand BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS seats (
    id           SERIAL PRIMARY KEY,
    event_id     INT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    section      TEXT NOT NULL,
    row_label    TEXT NOT NULL,
    seat_number  INT NOT NULL,
    price_cents  INT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'available'
                 CHECK (status IN ('available', 'held', 'booked')),
    held_by      TEXT,               -- opaque user/session token holding the seat
    held_until   TIMESTAMPTZ,        -- hold expiry; a background sweeper releases stale holds
    version      INT NOT NULL DEFAULT 0,  -- bumped on every state change (optimistic-concurrency audit trail)
    UNIQUE (event_id, section, row_label, seat_number)
);

CREATE INDEX IF NOT EXISTS idx_seats_event_status ON seats(event_id, status);
CREATE INDEX IF NOT EXISTS idx_seats_held_until ON seats(held_until) WHERE status = 'held';

CREATE TABLE IF NOT EXISTS bookings (
    id              SERIAL PRIMARY KEY,
    event_id        INT NOT NULL REFERENCES events(id),
    seat_id         INT NOT NULL REFERENCES seats(id),
    user_token      TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    price_cents     INT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (seat_id),           -- a seat can have at most one booking, ever
    UNIQUE (idempotency_key)    -- a confirm request can succeed at most once
);

CREATE TABLE IF NOT EXISTS seat_events_log (
    id         SERIAL PRIMARY KEY,
    seat_id    INT NOT NULL REFERENCES seats(id),
    event_type TEXT NOT NULL,   -- 'held' | 'booked' | 'released' | 'expired'
    actor      TEXT,
    at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
