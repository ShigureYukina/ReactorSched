package com.yuki.reactorsched.common.web;

import java.util.List;

/**
 * Uniform error body for every failed API call:
 * {@code {"code": "...", "message": "...", "details": [{"field": "...", "reason": "..."}]}}.
 *
 * <p>{@code details} carries per-field reasons for validation failures and is empty for every other
 * error, so clients can always read the field without branching on the error code (PRD 10.1).
 */
public record ApiError(String code, String message, List<FieldError> details) {

    /** One rejected field of a request body. */
    public record FieldError(String field, String reason) {
    }

    public ApiError(String code, String message) {
        this(code, message, List.of());
    }

    public static ApiError ofFields(String code, String message, List<FieldError> details) {
        return new ApiError(code, message, details == null ? List.of() : List.copyOf(details));
    }
}
