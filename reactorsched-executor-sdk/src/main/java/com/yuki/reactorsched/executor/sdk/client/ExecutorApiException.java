package com.yuki.reactorsched.executor.sdk.client;

/**
 * One call to the scheduling center that failed at the HTTP layer (PRD 7, 10.3).
 *
 * <p>The status and the machine-readable code of the error body travel with the exception, so the
 * engine can react to a semantic answer - 409 {@code LEASE_VERSION_STALE} means "stop working on
 * that attempt", 404 means "re-register" - without parsing JSON itself. A status of 0 means the
 * call never reached the center: the engine retries on the next tick.
 */
public class ExecutorApiException extends RuntimeException {

    private static final int TRANSPORT_STATUS = 0;

    private final int status;
    private final String code;

    public ExecutorApiException(int status, String code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }

    /** Status of a call that could not be sent or answered at all. */
    public static ExecutorApiException transport(String message) {
        return new ExecutorApiException(TRANSPORT_STATUS, "TRANSPORT", message);
    }

    /** An error answer of the center: its status plus the code and message of the error body. */
    public static ExecutorApiException of(int status, String code, String message) {
        return new ExecutorApiException(status, code == null ? "HTTP_" + status : code, message);
    }

    public int status() {
        return status;
    }

    public String code() {
        return code;
    }

    /** Whether the call never reached the center. */
    public boolean isTransportFailure() {
        return status == TRANSPORT_STATUS;
    }

    public boolean isStatus(int expected) {
        return status == expected;
    }

    public boolean hasCode(String expected) {
        return code != null && code.equals(expected);
    }
}
