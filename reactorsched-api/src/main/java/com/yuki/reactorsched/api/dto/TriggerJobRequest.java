package com.yuki.reactorsched.api.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * Body of {@code POST /api/v1/jobs/{id}/trigger}.
 *
 * <p>{@code clientKey} makes the trigger idempotent: sending the same key twice returns the instance
 * the first call created instead of running the job twice. {@code reason} is mandatory because a
 * manual trigger is an administrative action that must be auditable (PRD 10.4).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TriggerJobRequest(
        @NotBlank @Size(max = 64, message = "operator must not exceed 64 characters") String operator,
        @NotBlank @Size(max = 512, message = "reason must not exceed 512 characters") String reason,
        @NotBlank @Size(max = 128, message = "clientKey must not exceed 128 characters") String clientKey) {
}
