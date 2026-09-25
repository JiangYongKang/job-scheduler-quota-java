package com.github.highcumontoa.jobschedulerquotajava.model;

/**
 * 任务生命周期状态。
 *
 * <pre>
 * PENDING  --dispatch-->   RUNNING
 * RUNNING  --complete-->   SUCCEEDED        (终态)
 * RUNNING  --fail+retry--> PENDING
 * RUNNING  --fail+limit--> FAILED           (终态)
 * RUNNING  --cancel----->  CANCELLED        (终态)
 * RUNNING  --timeout---->  PENDING / FAILED (重试上限)
 * 终态不再接受任何迁移。
 * </pre>
 */
public enum JobState {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELLED
}
