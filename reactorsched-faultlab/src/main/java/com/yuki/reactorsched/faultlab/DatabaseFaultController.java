package com.yuki.reactorsched.faultlab;

import com.yuki.reactorsched.core.service.OperationAuditService;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.RestController;

/**
 * Database fault injection control (PRD 8.8): {@code READ_ONLY} fails every non-audit write,
 * {@code LATENCY} delays it. The audit write of this endpoint itself is exempt from the injection,
 * so a running fault can always be switched off again.
 */
@Profile("!executor")
@RestController
@RequestMapping("/api/v1/fault-lab/database")
public class DatabaseFaultController {

    private final DatabaseFaultState state;
    private final OperationAuditService auditService;

    public DatabaseFaultController(DatabaseFaultState state, OperationAuditService auditService) {
        this.state = state;
        this.auditService = auditService;
    }

    @GetMapping
    public DatabaseFaultView current() {
        return view();
    }

    @PostMapping
    @Transactional
    public DatabaseFaultView inject(@RequestBody DatabaseFaultRequest request) {
        auditService.record(request.operator(), "fault_lab.database", "database", "primary",
                request.reason(), state.mode(), request.mode());
        state.set(request.mode(), request.latencyMs());
        return view();
    }

    private DatabaseFaultView view() {
        return new DatabaseFaultView(state.mode(), state.latencyMs());
    }

    public record DatabaseFaultRequest(String operator, String reason, DatabaseFaultMode mode, Long latencyMs) {
    }

    public record DatabaseFaultView(DatabaseFaultMode mode, long latencyMs) {
    }
}
