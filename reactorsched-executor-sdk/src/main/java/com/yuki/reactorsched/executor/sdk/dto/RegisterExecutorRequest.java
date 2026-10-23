package com.yuki.reactorsched.executor.sdk.dto;

/**
 * Registration body of {@code POST /api/v1/executors/register} (PRD 7.1).
 *
 * <p>The SDK owns the shape it sends; the scheduling center binds the same fields. Keeping the
 * record on the executor side means an executor process compiles against the contract only - it
 * never carries the center's modules on its classpath.
 *
 * @param appName        application name this executor runs under
 * @param instanceId     instance identity; the center defaults it to {@code host:pid}
 * @param group          executor group this process serves
 * @param maxConcurrency how many attempts this process may run at once; informational for the
 *                       center, authoritative for this process's pool sizing
 * @param address        advertised address, informational only
 * @param metadata       free-form JSON, e.g. version or tags
 */
public record RegisterExecutorRequest(String appName,
                                      String instanceId,
                                      String group,
                                      Integer maxConcurrency,
                                      String address,
                                      String metadata) {
}
