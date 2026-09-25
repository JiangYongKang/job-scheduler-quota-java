package com.github.highcumontoa.jobschedulerquotajava.api;

import com.github.highcumontoa.jobschedulerquotajava.exception.SchedulerException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * 将领域错误翻译为可区分的 HTTP 响应：
 * 任务不存在 404；非法状态迁移/过期租约回传/参数非法 409 或 400。
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(SchedulerException.class)
    public ResponseEntity<Map<String, String>> handleScheduler(SchedulerException ex) {
        String message = ex.getMessage() == null ? "" : ex.getMessage();
        HttpStatus status = message.startsWith("任务不存在") ? HttpStatus.NOT_FOUND
                : message.contains("不能为空") || message.contains("必须为正数")
                        ? HttpStatus.BAD_REQUEST
                : HttpStatus.CONFLICT;
        String code = message.contains("过期租约") ? "stale_lease"
                : message.contains("非法状态迁移") ? "illegal_transition"
                : "domain_error";
        return ResponseEntity.status(status).body(Map.of("code", code, "message", message));
    }
}
