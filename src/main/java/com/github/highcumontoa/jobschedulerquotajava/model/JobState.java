package com.github.highcumontoa.jobschedulerquotajava.model;

/**
 * 任务生命周期状态。
 */
public enum JobState {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELLED,
    ABANDONED;

    /**
     * 终态：成功、失败、取消、重试上限终结。终态任务的配额必须已归还。
     */
    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == CANCELLED || this == ABANDONED;
    }
}
