-- ReactorSched V10: execution logs.
--
-- PRD 9: the log of an attempt is produced by the executor and reported back with the result. It
-- lives on the attempt row so it is cleaned together with it (retention: log_text 7 days,
-- task_attempt 30 days - PRD 12.4), and it carries a truncation flag because the SDK bounds the
-- buffer before sending (PRD 7.4/9).
--
-- The column is nullable on purpose: the embedded LOCAL executor and administrative replays do not
-- produce a log, and an absent log must be distinguishable from an empty one.

ALTER TABLE task_execution
    ADD COLUMN log_text     MEDIUMTEXT NULL COMMENT 'Executor-reported log of the attempt, bounded by log-max-bytes' AFTER error_message,
    ADD COLUMN log_truncated BIT         NOT NULL DEFAULT b'0' COMMENT 'Whether the reported log was cut by the byte bound' AFTER log_text;
