-- +goose Up

-- The projection of ADR-0002: per-member state derived from the conversation log,
-- never written by the request that causes it. A send writes one row to entries
-- regardless of member count; this table catches up afterwards.
--
-- Consequently every column here is eventually consistent, and clients are required
-- to render correctly while it is behind (NF-7).
CREATE TABLE conversation_member_state (
    conversation_id uuid        NOT NULL,
    account_id      uuid        NOT NULL,

    -- Read and delivered high-water marks. Both move forward only, enforced by the
    -- projection taking GREATEST rather than by the writer checking first — which is
    -- what makes redelivery and out-of-order arrival harmless (MS-12, NF-8).
    read_sequence      bigint   NOT NULL DEFAULT 0,
    delivered_sequence bigint   NOT NULL DEFAULT 0,

    unread_count       bigint   NOT NULL DEFAULT 0,

    -- The highest entry_appended already counted. Without it, redelivering an entry
    -- event would increment the badge a second time — the exact double-count NF-8
    -- forbids. Sequences are gapless per conversation and events are keyed by
    -- conversation, so this arrives in order and the comparison is sufficient.
    projected_sequence bigint   NOT NULL DEFAULT 0,

    updated_at      timestamptz NOT NULL,

    PRIMARY KEY (conversation_id, account_id)
);

-- The conversation-list query: every conversation one account belongs to, most
-- recently active first. ADR-0002 rejected computing this on read precisely so that
-- this screen is one indexed scan.
CREATE INDEX conversation_member_state_by_account
    ON conversation_member_state (account_id, updated_at DESC);

-- +goose Down
DROP TABLE conversation_member_state;
