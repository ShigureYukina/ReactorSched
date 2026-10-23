-- ReactorSched V9: executor registry (PRD 7.1, 11.1).
--
-- The registry is how the scheduling center knows which executors exist, which group they serve and
-- whether they are alive. Liveness is the heartbeat: a row whose last_heartbeat_at went stale is
-- marked OFFLINE by a sweep and stops receiving tasks, while its in-flight instances stay under
-- lease protection and are recovered through the ordinary lease path (PRD 8.3).
--
-- (app_name, instance_id) is the natural identity of an executor process: a restarted instance of
-- the same application re-registers onto the same row instead of piling up duplicates.

CREATE TABLE executor_registry
(
    id               BIGINT       NOT NULL AUTO_INCREMENT,
    app_name         VARCHAR(128) NOT NULL COMMENT 'Application name the executor registers under',
    instance_id      VARCHAR(128) NOT NULL COMMENT 'Instance identity, default host:port',
    group_name       VARCHAR(64)  NOT NULL DEFAULT 'DEFAULT' COMMENT 'Executor group this process serves',
    address          VARCHAR(256) NULL COMMENT 'Advertised address, informational only',
    status           VARCHAR(16)  NOT NULL COMMENT 'ONLINE / OFFLINE',
    last_heartbeat_at DATETIME(3) NULL COMMENT 'Last heartbeat; staleness drives the OFFLINE sweep',
    registered_at    DATETIME(3)  NOT NULL COMMENT 'First registration of this (app_name, instance_id)',
    metadata         JSON         NULL COMMENT 'Free-form executor metadata, e.g. version or tags',
    PRIMARY KEY (id),
    UNIQUE KEY uk_executor_identity (app_name, instance_id),
    KEY idx_executor_group_status (group_name, status)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;
