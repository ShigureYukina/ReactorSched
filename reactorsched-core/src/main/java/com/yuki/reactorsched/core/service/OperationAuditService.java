package com.yuki.reactorsched.core.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactorsched.core.domain.OperationAudit;
import com.yuki.reactorsched.core.mapper.OperationAuditMapper;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.LocalDateTime;

/**
 * Appends entries to the operator audit trail (PRD 10.4).
 *
 * <p>Called from inside the administrative transaction on purpose: if the audit row cannot be
 * written, the administrative change rolls back with it. A management action that leaves no trace is
 * worse than one that was refused.
 */
@Profile("!executor")
@Service
public class OperationAuditService {

    public static final String TARGET_JOB = "JOB";
    public static final String TARGET_TASK = "TASK";
    public static final String TARGET_FAULT_LAB = "FAULT_LAB";

    public static final String ACTION_CREATE = "CREATE";
    public static final String ACTION_UPDATE = "UPDATE";
    public static final String ACTION_ENABLE = "ENABLE";
    public static final String ACTION_PAUSE = "PAUSE";
    public static final String ACTION_DELETE = "DELETE";
    public static final String ACTION_TRIGGER = "TRIGGER";
    public static final String ACTION_CANCEL = "CANCEL";
    public static final String ACTION_REPLAY = "REPLAY";
    public static final String ACTION_FAULT_CONFIG = "FAULT_CONFIG";

    private static final int MAX_REASON_LENGTH = 512;

    private final OperationAuditMapper auditMapper;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public OperationAuditService(OperationAuditMapper auditMapper, ObjectMapper objectMapper, Clock clock) {
        this.auditMapper = auditMapper;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    /** Records one administrative change with the snapshots of the row before and after it. */
    @Transactional(propagation = Propagation.MANDATORY)
    public OperationAudit record(String operator, String action, String targetType, Object targetId,
                                 String reason, Object before, Object after) {
        String actor = requireText(operator, "operator", 64);
        String cause = requireText(reason, "reason", MAX_REASON_LENGTH);
        OperationAudit audit = new OperationAudit();
        audit.setOperator(actor);
        audit.setAction(requireText(action, "action", 64));
        audit.setTargetType(requireText(targetType, "targetType", 32));
        audit.setTargetId(String.valueOf(targetId));
        audit.setReason(cause);
        audit.setBeforeJson(toJson(before));
        audit.setAfterJson(toJson(after));
        audit.setCreatedAt(LocalDateTime.now(clock));
        auditMapper.insert(audit);
        return audit;
    }

    private String toJson(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            // The audit row must not silently lose its snapshot; the caller fails and rolls back.
            throw new IllegalStateException("cannot serialise audit snapshot: " + e.getMessage(), e);
        }
    }

    private static String requireText(String value, String field, int maxLength) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required for administrative operations");
        }
        String trimmed = value.trim();
        if (trimmed.length() > maxLength) {
            throw new IllegalArgumentException(field + " must not exceed " + maxLength + " characters");
        }
        return trimmed;
    }
}
