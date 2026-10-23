package com.yuki.reactorsched.api.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * Body of the job state endpoints (pause, enable, delete).
 *
 * <p>Only the audit fields are needed: the target state is the endpoint itself, so it cannot be
 * confused with a caller-supplied value.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record JobAdminRequest(
        @NotBlank @Size(max = 64, message = "operator must not exceed 64 characters") String operator,
        @NotBlank @Size(max = 512, message = "reason must not exceed 512 characters") String reason) {
}
