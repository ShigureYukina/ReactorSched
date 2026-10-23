-- ReactorSched V1: job definitions.
--
-- The scheduler's unit of configuration: what to run, when to run it and how to hand it to an
-- executor. Every instantiation of a definition is a separate row in {@code task} (the instance
-- table), so changing a definition never rewrites history.
--
-- All timestamps are UTC values written through the application's injected Clock, stored with
-- millisecond precision (AGENTS.md 3).

CREATE TABLE job_definition
(
    id                     BIGINT       NOT NULL AUTO_INCREMENT,
    job_key                VARCHAR(128) NOT NULL COMMENT 'Business unique name; free to use, still taken after a soft delete',
    name                   VARCHAR(128) NOT NULL,
    description            VARCHAR(512) NULL,
    task_type              VARCHAR(64)  NOT NULL COMMENT 'Handler type expected on the executor',
    executor_group         VARCHAR(64)  NOT NULL DEFAULT 'DEFAULT' COMMENT 'Executors that may claim the instances',
    payload_template       JSON         NOT NULL COMMENT 'Rendered into the instance payload; placeholders use a dollar-brace form, see docs/PRD.md 6.3',
    schedule_type          VARCHAR(16)  NOT NULL COMMENT 'CRON / FIXED_RATE / ONE_TIME',
    cron_expr              VARCHAR(64)  NULL COMMENT 'Six-field expression (seconds included), interpreted in UTC',
    interval_seconds       INT          NULL,
    run_at                 DATETIME(3)  NULL,
    misfire_policy         VARCHAR(16)  NOT NULL DEFAULT 'SKIP' COMMENT 'SKIP / FIRE_ONCE / CATCH_UP',
    misfire_grace_seconds  INT          NOT NULL DEFAULT 300,
    shard_total            INT          NOT NULL DEFAULT 1 COMMENT 'One instance per shard on every trigger',
    status                 VARCHAR(16)  NOT NULL COMMENT 'ENABLED / PAUSED / DELETED',
    priority               INT          NOT NULL DEFAULT 100 COMMENT 'Lower value is scheduled first',
    max_attempts           INT          NOT NULL DEFAULT 3,
    timeout_ms             BIGINT       NOT NULL DEFAULT 60000,
    retry_base_delay_ms    BIGINT       NOT NULL DEFAULT 1000,
    retry_multiplier       DECIMAL(4, 2) NOT NULL DEFAULT 2.00,
    retry_max_delay_ms     BIGINT       NOT NULL DEFAULT 300000,
    retry_jitter           DECIMAL(4, 2) NOT NULL DEFAULT 0.20,
    next_fire_at           DATETIME(3)  NULL COMMENT 'Claimed by schedulers with an optimistically locked update; NULL while paused',
    last_fire_at           DATETIME(3)  NULL COMMENT 'Most recent planned fire time that was claimed and turned into instances',
    created_by             VARCHAR(64)  NOT NULL,
    updated_by             VARCHAR(64)  NULL,
    created_at             DATETIME(3)  NOT NULL,
    updated_at             DATETIME(3)  NOT NULL,
    deleted_at             DATETIME(3)  NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_job_definition_key (job_key),
    KEY idx_job_due (status, next_fire_at),
    KEY idx_job_created_at (created_at)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;
