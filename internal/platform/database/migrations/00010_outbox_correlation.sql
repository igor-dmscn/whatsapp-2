-- +goose Up
-- NF-16: every request and every event carries a correlation identifier.
--
-- Requests already did. Events did not, and the gap was exactly where it hurts: a send is an
-- HTTP request, a Redis publish, an outbox row, a Kafka event and a worker projection, and the
-- worker's half of that had no identifier tying it to the request that caused it. Five log lines
-- in five places, which is the situation the identifier exists to prevent.
--
-- On the row rather than only in the payload, because the relay reads rows and publishes headers:
-- putting it in the encoded event would mean the relay decoding every payload to find it.
--
-- Nullable, and it stays nullable. Rows written before this column existed have no identifier and
-- never will, and an event published by something with no request behind it — a scheduled sweep,
-- a backfill — legitimately has none either. A NOT NULL default of '' would say "correlated with
-- nothing", which is a claim rather than an absence.
ALTER TABLE outbox ADD COLUMN correlation_id text;

-- +goose Down
ALTER TABLE outbox DROP COLUMN correlation_id;
