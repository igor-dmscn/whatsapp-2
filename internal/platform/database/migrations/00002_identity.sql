-- +goose Up

CREATE TABLE accounts (
    id         uuid        PRIMARY KEY,
    handle     text        NOT NULL,
    email      text        NOT NULL,
    created_at timestamptz NOT NULL
);

-- Handles and emails are compared case-insensitively: nobody should be able to
-- register "Ana" because "ana" is taken.
CREATE UNIQUE INDEX accounts_handle_key ON accounts (lower(handle));
CREATE UNIQUE INDEX accounts_email_key ON accounts (lower(email));

-- Credentials are entities inside the Account aggregate, never loaded alone.
CREATE TABLE credentials (
    id         uuid        PRIMARY KEY,
    account_id uuid        NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
    kind       text        NOT NULL,
    material   text        NOT NULL,
    created_at timestamptz NOT NULL
);

CREATE INDEX credentials_account_id_idx ON credentials (account_id);

-- An account may hold many credentials but only one password. The aggregate
-- enforces this too; the index is what catches two concurrent requests, which the
-- aggregate cannot see. Passkeys are deliberately left unconstrained — an account
-- will legitimately register several, and a blanket unique index on
-- (account_id, kind) would have to be dropped to allow that, which is exactly the
-- migration ID-2 exists to avoid.
CREATE UNIQUE INDEX credentials_one_password_per_account
    ON credentials (account_id)
    WHERE kind = 'password';

CREATE TABLE devices (
    id         uuid        PRIMARY KEY,
    account_id uuid        NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
    name       text        NOT NULL,
    created_at timestamptz NOT NULL,
    revoked_at timestamptz
);

CREATE INDEX devices_account_id_idx ON devices (account_id);

-- One row per session holding both digests, so rotation is a single UPDATE.
-- Modelled as independent token rows, rotation would be a delete plus two inserts
-- and a crash between them would destroy the session.
CREATE TABLE sessions (
    id                 uuid        PRIMARY KEY,
    device_id          uuid        NOT NULL REFERENCES devices (id) ON DELETE CASCADE,
    access_digest      bytea       NOT NULL,
    access_expires_at  timestamptz NOT NULL,
    refresh_digest     bytea       NOT NULL,
    refresh_expires_at timestamptz NOT NULL
);

-- Unique rather than plain: two sessions sharing a digest would make
-- authentication ambiguous, and a digest collision means a repeated secret.
CREATE UNIQUE INDEX sessions_access_digest_key ON sessions (access_digest);
CREATE UNIQUE INDEX sessions_refresh_digest_key ON sessions (refresh_digest);
CREATE INDEX sessions_device_id_idx ON sessions (device_id);

-- +goose Down

DROP TABLE sessions;
DROP TABLE devices;
DROP TABLE credentials;
DROP TABLE accounts;
