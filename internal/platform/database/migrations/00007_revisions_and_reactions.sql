-- +goose Up

-- Edits and retractions are new entries referencing an earlier one (ADR-0008).
-- Nothing in the log is ever modified in place, so these are columns on the *new*
-- entry, never on the one it amends.
ALTER TABLE entries
    -- The position this entry amends or retracts. NULL for an ordinary message.
    ADD COLUMN target_sequence bigint,
    -- The position this entry is a reply to. A reference field, which is all a reply
    -- needs — no mechanism, no separate storage, no threading.
    ADD COLUMN reply_to        bigint;

-- Finding what amends a given entry. Partial, because the overwhelming majority of
-- entries amend nothing and have no business in this index.
CREATE INDEX entries_by_target
    ON entries (conversation_id, target_sequence, sequence)
    WHERE target_sequence IS NOT NULL;

-- Reactions are deliberately *not* in the log (ADR-0008). They are high-churn: a
-- popular message in a large group would burn a sequence number per tap and wake
-- every connected client each time.
--
-- So they are separate state with no sequence number of their own (MS-10), keyed by
-- what they are about. The primary key is the whole rule: one account may hold one of
-- each emoji on one entry, and reacting twice is idempotent rather than cumulative.
CREATE TABLE reactions (
    conversation_id uuid        NOT NULL,
    sequence        bigint      NOT NULL,
    account_id      uuid        NOT NULL,
    emoji           text        NOT NULL,
    created_at      timestamptz NOT NULL,

    PRIMARY KEY (conversation_id, sequence, account_id, emoji)
);

-- The read path a client uses: reactions for the stretch of entries on screen. The
-- primary key already leads with (conversation_id, sequence), so this index would be
-- redundant — the key serves range scans over sequence directly.

-- +goose Down
DROP TABLE reactions;
DROP INDEX entries_by_target;
ALTER TABLE entries DROP COLUMN target_sequence, DROP COLUMN reply_to;
