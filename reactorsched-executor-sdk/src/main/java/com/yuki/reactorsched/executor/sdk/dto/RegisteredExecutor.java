package com.yuki.reactorsched.executor.sdk.dto;

/**
 * What a registration reads back: the row the center stored for this executor (PRD 7.1).
 *
 * <p>The id is what the executor uses from now on - heartbeat, claim, result and renewal all carry
 * it. Unknown fields of the center's response are ignored, so the center can grow its read model
 * without breaking a deployed executor.
 *
 * @param id         center-side id of this executor
 * @param appName    application name the row is registered under
 * @param instanceId instance identity of the row
 * @param groupName  group the center recorded
 * @param status     liveness the center believes in, {@code ONLINE} right after registration
 */
public record RegisteredExecutor(long id,
                                 String appName,
                                 String instanceId,
                                 String groupName,
                                 String status) {
}
