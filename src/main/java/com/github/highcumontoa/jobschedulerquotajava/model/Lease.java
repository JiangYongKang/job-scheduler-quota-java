package com.github.highcumontoa.jobschedulerquotajava.model;

/**
 * 一次下发产生的租约。
 *
 * @param leaseId    租约唯一标识
 * @param jobId      关联任务
 * @param executorId 持有租约的本地执行者
 * @param issuedAt   发放时间（epoch ms）
 * @param expiresAt  到期时间（epoch ms）
 * @param attempt    第几次尝试（从 1 开始）
 */
public record Lease(String leaseId,
                    String jobId,
                    String executorId,
                    long issuedAt,
                    long expiresAt,
                    int attempt) {

    public boolean isExpired(long nowMs) {
        return nowMs >= expiresAt;
    }
}
