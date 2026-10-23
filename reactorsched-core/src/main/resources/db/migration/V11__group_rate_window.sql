-- ReactorSched V11: group-level shared state (PRD 8.5, 12.3).
--
-- Retry dispatch rate and the AIMD concurrency limit must be visible across executor processes
-- (PRD 12.3: counting inside a single process and calling it "global rate limiting" is forbidden).
-- Both live in this one table, keyed by (executor_group, window_second):
--
--   window_second > 0  rate window row   - admitted counts retry dispatches admitted within that
--                                          epoch second; admission is a conditional UPDATE that
--                                          only succeeds while admitted < the configured cap.
--   window_second = 0  group state row   - one permanent row per group carrying the shared AIMD
--                                          concurrency limit; the adaptive controller writes it,
--                                          every executor reads it each poll round.
--
-- Rate window rows are disposable: the limiter deletes a group's rows older than two minutes when
-- it opens a new window, so the table stays small without waiting for the retention job (PRD 12.4).

CREATE TABLE group_rate_window
(
    executor_group VARCHAR(64) NOT NULL COMMENT 'Executor group this row belongs to',
    window_second  BIGINT      NOT NULL COMMENT 'Epoch second of the rate window; 0 marks the permanent group state row',
    admitted       INT         NOT NULL DEFAULT 0 COMMENT 'Retry dispatches admitted within this window (rate rows only)',
    current_limit  INT         NULL COMMENT 'Shared AIMD concurrency of the group (state row only)',
    updated_at     DATETIME(3) NULL COMMENT 'Last concurrency adjustment (state row only)',
    PRIMARY KEY (executor_group, window_second)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;
