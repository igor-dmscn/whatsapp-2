-- +goose Up

-- One outbox for every context. The rows are opaque here — a topic, a key and a
-- payload — so nothing about this table couples the contexts to each other, and one
-- relay can drain all of them.
CREATE TABLE outbox (
    -- bigserial rather than a uuid: the relay publishes in this order, and a
    -- monotonic integer is the cheapest thing to order and to resume from. UUIDv7
    -- is time-ordered but only to the millisecond, which is not enough to order two
    -- events written in the same transaction.
    id          bigserial   PRIMARY KEY,
    topic       text        NOT NULL,
    key         text        NOT NULL,
    event_name  text        NOT NULL,
    payload     jsonb       NOT NULL,
    occurred_at timestamptz NOT NULL,
    -- NULL until published. The relay's claim on a row is the transaction that
    -- sets this, so a crash mid-publish leaves the row unpublished and it is
    -- retried — at-least-once, which consumers are required to tolerate (NF-8).
    published_at timestamptz,
    -- Counted so a row that cannot be published is visible as such rather than
    -- being retried silently forever.
    attempts    integer     NOT NULL DEFAULT 0
);

-- The relay's only query: unpublished rows, oldest first. Partial, so it stays the
-- size of the backlog rather than the size of the history — which for a table that
-- is almost entirely published rows is the difference between kilobytes and
-- gigabytes.
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- +goose Down
DROP TABLE outbox;
