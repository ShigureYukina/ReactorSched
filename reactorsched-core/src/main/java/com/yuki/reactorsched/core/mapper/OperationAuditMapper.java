package com.yuki.reactorsched.core.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.yuki.reactorsched.core.domain.OperationAudit;

/**
 * Persistence of the operator audit trail. Append-only by design: no update or delete statements
 * are declared here, so nothing in the application can rewrite an audit entry (PRD 10.4).
 */
public interface OperationAuditMapper extends BaseMapper<OperationAudit> {
}
