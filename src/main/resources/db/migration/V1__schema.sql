-- Endpoints that receive webhooks.
CREATE TABLE endpoints (
    id              uuid PRIMARY KEY,
    url             text        NOT NULL,
    secret          text        NOT NULL,
    event_types     text[]      NOT NULL DEFAULT '{}',   -- empty = subscribe to all
    max_concurrency int         NOT NULL CHECK (max_concurrency BETWEEN 1 AND 1000),
    enabled         boolean     NOT NULL DEFAULT true,
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- Events. `body` is the exact JSON sent on every attempt and replay, so the
-- bytes a receiver verifies never change (only the signature timestamp does).
CREATE TABLE events (
    id         uuid PRIMARY KEY,
    type       text        NOT NULL,
    body       text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- The outbox: one row per (event, endpoint) delivery, written in the same
-- transaction as the event.
CREATE TABLE deliveries (
    id               uuid PRIMARY KEY,
    event_id         uuid        NOT NULL REFERENCES events (id),
    endpoint_id      uuid        NOT NULL REFERENCES endpoints (id),
    status           text        NOT NULL CHECK (status IN ('pending', 'in_flight', 'succeeded', 'dead')),
    attempts         int         NOT NULL DEFAULT 0,
    next_attempt_at  timestamptz NOT NULL DEFAULT now(),
    lease_token      uuid,
    locked_until     timestamptz,
    last_status_code int,
    last_error       text,
    replay_of        uuid REFERENCES deliveries (id),
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    completed_at     timestamptz,
    CHECK ((status = 'in_flight') = (lease_token IS NOT NULL AND locked_until IS NOT NULL))
);

CREATE INDEX deliveries_due ON deliveries (endpoint_id, next_attempt_at) WHERE status = 'pending';
CREATE INDEX deliveries_in_flight ON deliveries (endpoint_id, locked_until) WHERE status = 'in_flight';
CREATE INDEX deliveries_dead ON deliveries (endpoint_id, completed_at) WHERE status = 'dead';
CREATE INDEX deliveries_event ON deliveries (event_id);
CREATE UNIQUE INDEX deliveries_one_replay ON deliveries (replay_of) WHERE replay_of IS NOT NULL;

-- Audit trail of every attempt (only attempts whose lease was still valid).
CREATE TABLE delivery_attempts (
    delivery_id uuid        NOT NULL REFERENCES deliveries (id),
    attempt     int         NOT NULL,
    started_at  timestamptz NOT NULL,
    duration_ms int         NOT NULL,
    status_code int,
    error       text,
    PRIMARY KEY (delivery_id, attempt)
);
