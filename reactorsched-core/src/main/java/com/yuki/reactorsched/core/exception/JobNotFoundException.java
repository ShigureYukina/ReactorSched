package com.yuki.reactorsched.core.exception;

/**
 * Thrown when a job definition does not exist (or was never created under that id).
 */
public class JobNotFoundException extends RuntimeException {

    private final long jobId;

    public JobNotFoundException(long jobId) {
        super("job " + jobId + " does not exist");
        this.jobId = jobId;
    }

    public JobNotFoundException(String jobKey) {
        super("job with key '" + jobKey + "' does not exist");
        this.jobId = -1L;
    }

    public long getJobId() {
        return jobId;
    }
}
