package com.github.highcumontoa.jobschedulerquotajava.model;

/**
 * 请求被拒绝 / 任务终结的可区分原因。
 */
public enum RejectReason {
    /** 提交即超出主体配额上限。 */
    QUOTA_EXCEEDED_ON_SUBMIT,
    /** 幂等键冲突：同一 jobId 已存在。 */
    DUPLICATE_SUBMISSION,
    /** 非法状态迁移（终态再迁移、状态不符等）。 */
    ILLEGAL_STATE_TRANSITION,
    /** 执行失败且重试次数已耗尽。 */
    RETRY_LIMIT_EXHAUSTED,
    /** 租约已过期，操作被拒绝。 */
    LEASE_EXPIRED,
    /** 租约不属于该执行者。 */
    LEASE_OWNER_MISMATCH,
    /** 参数非法。 */
    INVALID_ARGUMENT,
    /** 任务不存在。 */
    JOB_NOT_FOUND
}
