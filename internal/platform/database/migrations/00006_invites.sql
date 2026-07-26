-- +goose Up

-- Shareable permission to join a conversation.
--
-- Its own table rather than columns on conversations: an invite has an independent
-- lifetime and a conversation may have many, including many withdrawn ones.
CREATE TABLE invites (
    id              uuid        PRIMARY KEY,
    conversation_id uuid        NOT NULL REFERENCES conversations (id) ON DELETE CASCADE,

    -- The token is stored as given, not digested. Unlike a session token this is a
    -- capability the server must be able to *show* its creator again — "copy the
    -- link" is the whole feature — and a digest cannot be shown. What limits the
    -- damage instead is that an invite grants one specific conversation at one
    -- specific role, is revocable, and may expire.
    token           text        NOT NULL UNIQUE,

    created_by      uuid        NOT NULL,
    role            text        NOT NULL,

    -- Zero means unlimited. A shared group link is the ordinary case and single-use
    -- the special one, so counting uses is the general shape and being consumed by
    -- the first redeemer is max_uses = 1.
    max_uses        integer     NOT NULL DEFAULT 0,
    uses            integer     NOT NULL DEFAULT 0,
    revoked         boolean     NOT NULL DEFAULT false,

    created_at      timestamptz NOT NULL,
    expires_at      timestamptz
);

-- Listing a conversation's invites, for the screen that shows what has been handed
-- out. The token lookup is served by its unique index.
CREATE INDEX invites_by_conversation ON invites (conversation_id, created_at DESC);

-- +goose Down
DROP TABLE invites;
