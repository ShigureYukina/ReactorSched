-- ReactorSched V9: instances remember where they came from.
--
-- The instance table gains the columns that separate "one planned firing of a job" from "one direct
-- submission" (PRD 11.2):
--   identity_key    - the idempotency key of the plan, unique; NULL for legacy direct submissions
--   job_id          - the definition the instance was created from
--   shard_index/total - which part of the firing this instance is
--   scheduled_at    - the planned fire time; created_at is when the row was actually written
--   executor_group  - which executors may claim it
--
-- identity_key is the unique constraint that makes instance creation idempotent: a second insert of
-- the same planned fire time and shard cannot create a second row (AGENTS.md 3).
--
-- It is NULL for direct submissions (the compatibility endpoint, which has its own idempotency key).
-- MySQL allows multiple NULLs in a unique index, so direct submissions stay unlimited. The column has
-- no default on purpose: every writer states explicitly whether it is creating a planned instance or
-- a direct submission, and a forgotten value fails loudly instead of colliding on an empty string.

ALTER TABLE task
    ADD COLUMN identity_key   VARCHAR(191) NULL COMMENT 'job:{id}:{fireEpochMs}:{shard} or manual:{id}:{key}; NULL for direct submissions' AFTER id,
    ADD COLUMN job_id         BIGINT       NULL COMMENT 'Definition the instance was created from' AFTER identity_key,
    ADD COLUMN shard_index    INT          NOT NULL DEFAULT 0 AFTER job_id,
    ADD COLUMN shard_total    INT          NOT NULL DEFAULT 1 AFTER shard_index,
    ADD COLUMN scheduled_at   DATETIME(3)  NULL COMMENT 'Planned fire time; NULL for direct submissions' AFTER shard_total,
    ADD COLUMN executor_group VARCHAR(64)  NOT NULL DEFAULT 'DEFAULT' AFTER scheduled_at,
    ADD UNIQUE KEY uk_task_identity (identity_key),
    ADD KEY idx_task_job_schedule (job_id, scheduled_at, shard_index);
