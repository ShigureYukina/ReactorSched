package com.yuki.reactorsched.core.exception;

/**
 * Thrown when a job definition cannot move into the requested state, for example enabling a
 * deleted job or pausing one that is already paused. The state machine holds the authoritative
 * transition table; this exception carries the message that reaches the HTTP layer as a 409.
 */
public class InvalidJobStateException extends RuntimeException {

    public InvalidJobStateException(String message) {
        super(message);
    }
}
