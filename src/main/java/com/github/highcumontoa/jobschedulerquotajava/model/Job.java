package com.github.highcumontoa.jobschedulerquotajava.model;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 任务聚合根。任务的全部状态迁移都必须在持有对象内置锁 ({@code #getLock()}) 的前提下进行，
 * 从而保证状态、租约、占用标记三者在并发下结论唯一。
 */
public class Job {

    private final String jobId;
    private final String subject;
    private final ResourceQuota demand;
    private final int priority;
    private final String payload;
    private final long createdAt;
    private final int maxRetries;

    private volatile JobState state = JobState.PENDING;
    private volatile Lease lease;
    private volatile int attempt;
    private volatile RejectReason terminalReason;
    private volatile String resultDetail;

    /** 该任务的配额当前是否已计入"占用中"，用于保证配额只发放/归还一次。 */
    private volatile boolean quotaHeld;

    /** 被高优先级任务抢占的次数（防饿死依据：达到上限后获得抢占免疫）。 */
    private volatile int preemptedCount;

    /** 调度公平性：同一主体连续被选中的序号依据（由调度器维护，仅作可读记录）。 */
    private final AtomicInteger scheduleSequence = new AtomicInteger(0);

    public Job(String jobId, String subject, ResourceQuota demand, int priority,
               String payload, long createdAt, int maxRetries) {
        this.jobId = jobId;
        this.subject = subject;
        this.demand = demand;
        this.priority = priority;
        this.payload = payload;
        this.createdAt = createdAt;
        this.maxRetries = maxRetries;
    }

    /** 任务自身的锁：状态机 + 租约 + 配额占用标记的原子保护单元。 */
    public Object getLock() {
        return this;
    }

    public String getJobId() { return jobId; }
    public String getSubject() { return subject; }
    public ResourceQuota getDemand() { return demand; }
    public int getPriority() { return priority; }
    public String getPayload() { return payload; }
    public long getCreatedAt() { return createdAt; }
    public int getMaxRetries() { return maxRetries; }

    public JobState getState() { return state; }
    public void setState(JobState state) { this.state = state; }

    public Lease getLease() { return lease; }
    public void setLease(Lease lease) { this.lease = lease; }

    public int getAttempt() { return attempt; }
    public void setAttempt(int attempt) { this.attempt = attempt; }

    public RejectReason getTerminalReason() { return terminalReason; }
    public void setTerminalReason(RejectReason terminalReason) { this.terminalReason = terminalReason; }

    public String getResultDetail() { return resultDetail; }
    public void setResultDetail(String resultDetail) { this.resultDetail = resultDetail; }

    public boolean isQuotaHeld() { return quotaHeld; }
    public void setQuotaHeld(boolean quotaHeld) { this.quotaHeld = quotaHeld; }

    public int getPreemptedCount() { return preemptedCount; }
    public void setPreemptedCount(int preemptedCount) { this.preemptedCount = preemptedCount; }

    public AtomicInteger getScheduleSequence() { return scheduleSequence; }

    public boolean isTerminal() {
        return state == JobState.SUCCEEDED || state == JobState.FAILED || state == JobState.CANCELLED;
    }

    /** 快照视图，用于日志与接口返回。 */
    public JobSnapshot snapshot() {
        return new JobSnapshot(jobId, subject, demand, priority, state.name(), attempt, maxRetries,
                lease == null ? null : lease.leaseId(), lease == null ? null : lease.executorId(),
                terminalReason == null ? null : terminalReason.name(), resultDetail, quotaHeld, createdAt);
    }
}
