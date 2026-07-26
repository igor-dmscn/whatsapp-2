-- +goose Up

-- Calls belong to Calling. Nothing about media is stored: the SFU holds transports and
-- forwards packets, and none of that survives a restart or is worth persisting. What is
-- here is the lifecycle — who was in a call, when, and which node held it.
CREATE TABLE calls (
    id              uuid        PRIMARY KEY,

    -- The conversation this call belongs to. Not a foreign key: entitlement to join
    -- derives from membership (CL-1) and is asked of Messaging through a port, so
    -- Calling references the conversation by ID and depends on none of its schema.
    conversation_id uuid        NOT NULL,

    -- ringing, active or ended. Not an enum type: a state machine that gains a state
    -- should not need a migration that rewrites a type, and the aggregate is what
    -- decides which transitions are legal.
    state           text        NOT NULL,

    -- The SFU holding this call's media. Recorded because every participant must reach
    -- the same one (CL-4), and a call that was allocated to one node and then answered
    -- by another would forward nothing while looking entirely healthy.
    node            text        NOT NULL,

    started_at      timestamptz NOT NULL,
    ended_at        timestamptz
);

-- CL-2, enforced by the database rather than by a check in a handler: a conversation has
-- at most one call that has not ended.
--
-- A partial unique index is the whole rule. Two people pressing call simultaneously race
-- here, one loses on this constraint, and the loser's remedy is to join the call that
-- won — which is what "starting a second returns the existing one" means. The alternative,
-- a read followed by a write, is a race dressed up as a check.
CREATE UNIQUE INDEX calls_one_live_per_conversation
    ON calls (conversation_id)
    WHERE state <> 'ended';

-- A call log is what these rows are for once a call is over, so a conversation's history
-- reads in order.
CREATE INDEX calls_by_conversation ON calls (conversation_id, started_at DESC);

-- One row per presence, not per device: a device that leaves and rejoins was present
-- twice, and collapsing that would make "who was in this call" wrong in the one case
-- where somebody's connection dropped.
CREATE TABLE call_participants (
    call_id    uuid        NOT NULL REFERENCES calls (id) ON DELETE CASCADE,
    device_id  uuid        NOT NULL,
    account_id uuid        NOT NULL,
    joined_at  timestamptz NOT NULL,
    left_at    timestamptz,

    PRIMARY KEY (call_id, device_id, joined_at)
);

-- +goose Down

DROP TABLE call_participants;
DROP INDEX calls_by_conversation;
DROP INDEX calls_one_live_per_conversation;
DROP TABLE calls;
