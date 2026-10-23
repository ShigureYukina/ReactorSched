package com.yuki.reactorsched.core.dto;

import java.util.List;

/** One page of job definitions plus the total number of rows matching the filter. */
public record JobPage(List<JobView> items, long total, int page, int size) {
}
