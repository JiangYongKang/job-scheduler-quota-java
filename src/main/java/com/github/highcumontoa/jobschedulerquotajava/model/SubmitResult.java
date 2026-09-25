package com.github.highcumontoa.jobschedulerquotajava.model;

/**
 * 提交结论（骨架占位）。
 */
public record SubmitResult(boolean accepted, String jobId, RejectReason reason, String message) {
}
