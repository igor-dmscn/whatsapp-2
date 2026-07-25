-- +goose Up

CREATE TABLE conversations (
    id         uuid        PRIMARY KEY,
    kind       text        NOT NULL,
    head       bigint      NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL,

    -- Canonical identity of a direct conversation's pair, sorted so that
    -- (ana, bruno) and (bruno, ana) collide. NULL for groups and channels.
    direct_key text
);

-- One direct conversation per pair. Without this, both accounts messaging each
-- other simultaneously creates two conversations and each sees half the history.
CREATE UNIQUE INDEX conversations_direct_key
    ON conversations (direct_key)
    WHERE direct_key IS NOT NULL;

CREATE TABLE memberships (
    conversation_id uuid        NOT NULL REFERENCES conversations (id) ON DELETE CASCADE,
    account_id      uuid        NOT NULL,
    role            text        NOT NULL,

    -- The whole of the history policy, in one column. Set to the head on join for
    -- groups, to 1 for channels.
    visible_from    bigint      NOT NULL,

    joined_at       timestamptz NOT NULL,

    -- Left rather than deleted: an entry's author must stay resolvable after they
    -- go, or a conversation's history cannot be rendered.
    left_at         timestamptz,

    PRIMARY KEY (conversation_id, account_id)
);

-- Serves "which conversations does this account belong to", which is the query
-- behind both the conversation list and socket resume.
CREATE INDEX memberships_account_id_idx ON memberships (account_id);

CREATE TABLE entries (
    id              uuid        PRIMARY KEY,
    conversation_id uuid        NOT NULL REFERENCES conversations (id) ON DELETE CASCADE,
    sequence        bigint      NOT NULL,
    author_id       uuid        NOT NULL,
    client_entry_id text        NOT NULL,
    kind            text        NOT NULL,

    -- The payload. content_type is recorded, never interpreted; body is bytes the
    -- server does not parse (ADR-0001). There is deliberately no index over body
    -- and no full-text column — search is the client's job.
    content_type    text        NOT NULL,
    body            bytea       NOT NULL,

    created_at      timestamptz NOT NULL
);

-- Gaplessness, enforced by the database as well as by the aggregate. Appends hold
-- the conversation's row lock so two writers cannot pick the same position; this
-- index is what would turn a bug in that reasoning into a failed write rather than
-- silent corruption.
CREATE UNIQUE INDEX entries_conversation_sequence_key ON entries (conversation_id, sequence);

-- Idempotent send (MS-2). Scoped to author as well as conversation so one client's
-- identifier cannot collide with another's.
CREATE UNIQUE INDEX entries_client_entry_id_key
    ON entries (conversation_id, author_id, client_entry_id);

-- +goose Down

DROP TABLE entries;
DROP TABLE memberships;
DROP TABLE conversations;
