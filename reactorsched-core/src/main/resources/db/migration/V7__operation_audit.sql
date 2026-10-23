-- ReactorSched V2: operator audit trail.
--
-- Every administrative write (job create/update/enable/pause/delete/trigger, instance cancel and
-- replay, fault injection switches) is recorded here in the same transaction as the write it
-- describes (PRD 10.4). An audit row that cannot be written fails the operation - a management
-- action that leaves no trace is worse than one that was refused.

CREATE TABLE operation_audit
(
    id          BIGINT       NOT NULL AUTO_INCREMENT,
    operator    VARCHAR(64)  NOT NULL,
    action      VARCHAR(64)  NOT NULL,
    target_type VARCHAR(32)  NOT NULL,
    target_id   VARCHAR(64)  NOT NULL,
    reason      VARCHAR(512) NOT NULL,
    before_json JSON         NULL,
    after_json  JSON         NULL,
    created_at  DATETIME(3)  NOT NULL,
    PRIMARY KEY (id),
    KEY idx_audit_target (target_type, target_id),
    KEY idx_audit_created (created_at)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;
