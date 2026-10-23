package com.yuki.reactorsched.api.web;

import com.yuki.reactorsched.common.web.ApiError;
import com.yuki.reactorsched.core.exception.BackpressureRejectedException;
import com.yuki.reactorsched.core.exception.ExecutionNotFoundException;
import com.yuki.reactorsched.core.exception.InvalidJobStateException;
import com.yuki.reactorsched.core.exception.InvalidTaskStateException;
import com.yuki.reactorsched.core.exception.JobNotFoundException;
import com.yuki.reactorsched.core.exception.TaskNotFoundException;
import com.yuki.reactorsched.executor.api.UnknownTaskTypeException;
import com.yuki.reactorsched.executor.registry.exception.ExecutorMismatchException;
import com.yuki.reactorsched.executor.registry.exception.ExecutorNotFoundException;
import com.yuki.reactorsched.executor.registry.exception.ExecutorOfflineException;
import com.yuki.reactorsched.executor.registry.exception.LeaseVersionStaleException;
import com.yuki.reactorsched.core.statemachine.IllegalJobTransitionException;
import com.yuki.reactorsched.core.statemachine.IllegalTaskTransitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.List;

/**
 * Turns domain failures into one consistent error shape (AGENTS.md 11):
 * 400 for bad input, 404 for unknown jobs or instances, 409 for state conflicts, 500 for everything
 * else.
 *
 * <p>Validation failures keep their field names in {@code details}, so a client can point at the
 * offending property instead of parsing the message (PRD 10.2).
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(TaskNotFoundException.class)
    public ResponseEntity<ApiError> handleNotFound(TaskNotFoundException e) {
        return error(HttpStatus.NOT_FOUND, "TASK_NOT_FOUND", e.getMessage());
    }

    @ExceptionHandler(BackpressureRejectedException.class)
    public ResponseEntity<ApiError> handleBackpressure(BackpressureRejectedException e) {
        return error(HttpStatus.TOO_MANY_REQUESTS, "BACKPRESSURE_REJECTED", e.getMessage());
    }

    @ExceptionHandler(JobNotFoundException.class)
    public ResponseEntity<ApiError> handleJobNotFound(JobNotFoundException e) {
        return error(HttpStatus.NOT_FOUND, "JOB_NOT_FOUND", e.getMessage());
    }

    @ExceptionHandler(ExecutionNotFoundException.class)
    public ResponseEntity<ApiError> handleExecutionNotFound(ExecutionNotFoundException e) {
        return error(HttpStatus.NOT_FOUND, "EXECUTION_NOT_FOUND", e.getMessage());
    }

    @ExceptionHandler(ExecutorNotFoundException.class)
    public ResponseEntity<ApiError> handleExecutorNotFound(ExecutorNotFoundException e) {
        return error(HttpStatus.NOT_FOUND, "EXECUTOR_NOT_FOUND", e.getMessage());
    }

    @ExceptionHandler(ExecutorOfflineException.class)
    public ResponseEntity<ApiError> handleExecutorOffline(ExecutorOfflineException e) {
        return error(HttpStatus.CONFLICT, "EXECUTOR_OFFLINE", e.getMessage());
    }

    /**
     * A result or renewal that arrived with a lease version which no longer owns the instance: the
     * attempt was already closed as {@code ABANDONED} with {@code STALE_WRITER} (PRD 8.2). The
     * executor drops the attempt instead of retrying the submission.
     */
    @ExceptionHandler(LeaseVersionStaleException.class)
    public ResponseEntity<ApiError> handleLeaseVersionStale(LeaseVersionStaleException e) {
        return error(HttpStatus.CONFLICT, "LEASE_VERSION_STALE", e.getMessage());
    }

    /** An attempt that belongs to another executor, even though the fleet token is shared. */
    @ExceptionHandler(ExecutorMismatchException.class)
    public ResponseEntity<ApiError> handleExecutorMismatch(ExecutorMismatchException e) {
        return error(HttpStatus.FORBIDDEN, "EXECUTOR_MISMATCH", e.getMessage());
    }

    @ExceptionHandler(UnknownTaskTypeException.class)
    public ResponseEntity<ApiError> handleUnknownTaskType(UnknownTaskTypeException e) {
        return error(HttpStatus.BAD_REQUEST, "UNKNOWN_TASK_TYPE", e.getMessage());
    }

    @ExceptionHandler(IllegalJobTransitionException.class)
    public ResponseEntity<ApiError> handleIllegalJobTransition(IllegalJobTransitionException e) {
        return error(HttpStatus.CONFLICT, "ILLEGAL_JOB_TRANSITION", e.getMessage());
    }

    @ExceptionHandler(InvalidJobStateException.class)
    public ResponseEntity<ApiError> handleInvalidJobState(InvalidJobStateException e) {
        return error(HttpStatus.CONFLICT, "INVALID_JOB_STATE", e.getMessage());
    }

    @ExceptionHandler(IllegalTaskTransitionException.class)
    public ResponseEntity<ApiError> handleIllegalTransition(IllegalTaskTransitionException e) {
        return error(HttpStatus.CONFLICT, "ILLEGAL_TASK_TRANSITION", e.getMessage());
    }

    @ExceptionHandler(InvalidTaskStateException.class)
    public ResponseEntity<ApiError> handleInvalidState(InvalidTaskStateException e) {
        return error(HttpStatus.CONFLICT, "INVALID_TASK_STATE", e.getMessage());
    }

    @ExceptionHandler(DuplicateKeyException.class)
    public ResponseEntity<ApiError> handleDuplicateKey(DuplicateKeyException e) {
        return error(HttpStatus.CONFLICT, "IDEMPOTENCY_CONFLICT",
                "a conflicting task already exists for this idempotency key");
    }

    /**
     * Bean Validation rejections: one {@code details} entry per rejected field, in the declaration
     * order of the request record.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiError> handleValidation(MethodArgumentNotValidException e) {
        List<ApiError.FieldError> details = e.getBindingResult().getFieldErrors().stream()
                .map(field -> new ApiError.FieldError(field.getField(), field.getDefaultMessage()))
                .toList();
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ApiError.ofFields("INVALID_REQUEST", "request validation failed", details));
    }

    @ExceptionHandler({
            IllegalArgumentException.class,
            MethodArgumentTypeMismatchException.class,
            HttpMessageNotReadableException.class,
            MissingServletRequestParameterException.class
    })
    public ResponseEntity<ApiError> handleBadRequest(Exception e) {
        return error(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", e.getMessage());
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleUnexpected(Exception e) {
        log.error("unhandled error while serving request", e);
        return error(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "unexpected server error");
    }

    private static ResponseEntity<ApiError> error(HttpStatus status, String code, String message) {
        return ResponseEntity.status(status).body(new ApiError(code, message));
    }
}
