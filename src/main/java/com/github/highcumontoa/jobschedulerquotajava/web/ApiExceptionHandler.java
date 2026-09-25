package com.github.highcumontoa.jobschedulerquotajava.web;

import com.github.highcumontoa.jobschedulerquotajava.exception.JobRejectedException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * 统一异常出口：把可区分的拒绝原因映射为 4xx 响应体。
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(JobRejectedException.class)
    public ResponseEntity<Map<String, Object>> handleRejected(JobRejectedException ex) {
        HttpStatus status = switch (ex.getReason()) {
            case JOB_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case DUPLICATE_SUBMISSION -> HttpStatus.CONFLICT;
            default -> HttpStatus.UNPROCESSABLE_ENTITY;
        };
        return ResponseEntity.status(status).body(Map.of(
                "rejected", true,
                "reason", ex.getReason().name(),
                "message", ex.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArg(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(Map.of(
                "rejected", true,
                "reason", "INVALID_ARGUMENT",
                "message", String.valueOf(ex.getMessage())));
    }
}
