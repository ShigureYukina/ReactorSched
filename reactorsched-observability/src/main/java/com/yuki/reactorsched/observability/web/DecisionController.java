package com.yuki.reactorsched.observability.web;

import com.yuki.reactorsched.core.domain.DecisionType;
import com.yuki.reactorsched.observability.dto.DecisionPage;
import com.yuki.reactorsched.observability.dto.DecisionQuery;
import com.yuki.reactorsched.observability.service.DecisionQueryService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;

/**
 * Decision query API (PRD 5.6): what the scheduler decided, with the recorded reason.
 *
 * <p>Every filter is optional and the result is the stored rows only - a reason is never inferred
 * from the current state of a task.
 */
@Profile("!executor")
@RestController
@RequestMapping("/api/v1/decisions")
public class DecisionController {

    private final DecisionQueryService decisionQueryService;

    public DecisionController(DecisionQueryService decisionQueryService) {
        this.decisionQueryService = decisionQueryService;
    }

    @GetMapping
    public DecisionPage list(@RequestParam(required = false) Long taskId,
                             @RequestParam(required = false) DecisionType type,
                             @RequestParam(required = false) String reasonCode,
                             @RequestParam(required = false)
                             @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
                             @RequestParam(required = false)
                             @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to,
                             @RequestParam(defaultValue = "0") int page,
                             @RequestParam(defaultValue = "20") int size) {
        return decisionQueryService.query(
                new DecisionQuery(taskId, type, reasonCode, from, to, page, size));
    }
}
