package com.yuki.reactorsched.core.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * One row of the operator audit trail (table {@code operation_audit}, PRD 10.4).
 *
 * <p>Written in the same transaction as the administrative change it describes: if the audit row
 * cannot be stored, the change is rolled back. {@code beforeJson} / {@code afterJson} are stored as
 * text because they are opaque snapshots from the caller's point of view.
 */
@Data
@TableName("operation_audit")
public class OperationAudit {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String operator;

    private String action;

    private String targetType;

    private String targetId;

    private String reason;

    private String beforeJson;

    private String afterJson;

    private LocalDateTime createdAt;
}
