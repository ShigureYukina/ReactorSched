package com.yuki.reactorsched.executor.registry.dto;

import com.yuki.reactorsched.core.domain.ExecutorGroups;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

/**
 * Registration body of {@code POST /api/v1/executors/register} (PRD 7.1).
 *
 * @param appName       application name the executor runs under
 * @param instanceId    instance identity; defaults to {@code host:port} when the SDK leaves it blank
 * @param group         executor group this process serves
 * @param maxConcurrency how many attempts this process may run at once; informational for the
 *                       center, authoritative for the executor's own pool sizing
 * @param address       advertised address, informational only
 * @param metadata      free-form JSON, e.g. version or tags
 */
public record RegisterExecutorRequest(
        @NotBlank(message = "appName must not be blank") String appName,
        String instanceId,
        String group,
        @Positive(message = "maxConcurrency must be positive") Integer maxConcurrency,
        String address,
        String metadata) {

    /** Group actually registered, applying the default when the request left it blank. */
    public String effectiveGroup() {
        return group == null || group.isBlank() ? ExecutorGroups.DEFAULT : group.trim();
    }
}
