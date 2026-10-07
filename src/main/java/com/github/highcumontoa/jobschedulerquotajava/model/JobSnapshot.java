package com.github.highcumontoa.jobschedulerquotajava.model;

/**
 * 任务的只读快照（接口返回 / 日志使用）。
 */
public record JobSnapshot(String jobId,
                          String subject,
                          ResourceQuota demand,
                          int priority,
                          String state,
                          int attempt,
                          int maxRetries,
                          String leaseId,
                          String executorId,
                          String terminalReason,
                          String resultDetail,
                          boolean quotaHeld,
                          int preemptedCount,
                          long createdAt) {
}
