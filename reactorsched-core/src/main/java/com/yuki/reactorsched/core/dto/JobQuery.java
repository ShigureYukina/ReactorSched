package com.yuki.reactorsched.core.dto;

import com.yuki.reactorsched.core.domain.JobStatus;

import java.time.LocalDateTime;

/**
 * Filter for listing job definitions: state, creation window and paging.
 */
public record JobQuery(JobStatus status, LocalDateTime from, LocalDateTime to, int page, int size) {
}
