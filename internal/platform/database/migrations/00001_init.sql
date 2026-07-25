-- Intentionally does nothing. Phase 0 exists to prove that the migration
-- pipeline runs, is recorded, and is idempotent on a second invocation.
-- Phase 1 adds the first real schema, for Identity.

-- +goose Up
SELECT 1;

-- +goose Down
SELECT 1;
