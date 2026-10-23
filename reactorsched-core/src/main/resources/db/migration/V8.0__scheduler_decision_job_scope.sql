-- ReactorSched V8: job level decisions.
--
-- Once instances are created by a scheduler rather than submitted by a client, the decisions worth
-- explaining are no longer only about one task: "why was this fire time skipped", "which shards were
-- reassigned", "trigger happened but no instance was created (PRD 8.7). Those rows name the job, not
-- a task, so the decision table gains a job column and allows task_id to stay NULL.
--
-- The decision type column has no CHECK constraint (it never had), so new types are a code change
-- plus a PRD entry, exactly like reason codes.

ALTER TABLE scheduler_decision
    ADD COLUMN job_id BIGINT NULL COMMENT 'Job definition a job level decision is about, NULL for task level decisions' AFTER task_id,
    ADD COLUMN shard_index INT NULL COMMENT 'Shard a shard level decision is about, NULL when not shard specific' AFTER job_id,
    ADD KEY idx_scheduler_decision_job (job_id, id);
