package com.github.highcumontoa.jobschedulerquotajava.model;

/**
 * 任务实体。
 * <p>
 * 所有字段读写都必须在 {@code JobSchedulerService} 的全局锁保护下进行，
 * 因此本类自身不加锁，保持为可变数据载体。
 */
public class Job {

    private final String jobId;
    private final String submitter;
    private final int cpuUnits;
    private final int priority;
    /** 同一优先级内的公平序号，单调递增，先入队者序号小。 */
    private final long enqueueSeq;

    private JobState state = JobState.PENDING;
    /** 已下发次数，首次执行前为 0；用于重试上限判定。 */
    private int attempts = 0;
    /** 当前租约标识；PENDING / 终态时为 null。 */
    private String leaseId;
    /** 当前租约到期的绝对时间（毫秒）；PENDING / 终态时为 0。 */
    private long leaseExpiresAt;
    /** 终态原因说明（失败、取消、重试上限等），便于解释最终结论。 */
    private String finishReason;

    public Job(String jobId, String submitter, int cpuUnits, int priority, long enqueueSeq) {
        this.jobId = jobId;
        this.submitter = submitter;
        this.cpuUnits = cpuUnits;
        this.priority = priority;
        this.enqueueSeq = enqueueSeq;
    }

    /**
     * 租约是否已超时：仅 RUNNING 任务、当前时间不早于到期时间才算超时。
     */
    public boolean isLeaseExpired(long nowMillis) {
        return state == JobState.RUNNING && leaseId != null && nowMillis >= leaseExpiresAt;
    }

    public String getJobId() {
        return jobId;
    }

    public String getSubmitter() {
        return submitter;
    }

    public int getCpuUnits() {
        return cpuUnits;
    }

    public int getPriority() {
        return priority;
    }

    public long getEnqueueSeq() {
        return enqueueSeq;
    }

    public JobState getState() {
        return state;
    }

    public void setState(JobState state) {
        this.state = state;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public String getLeaseId() {
        return leaseId;
    }

    public void setLeaseId(String leaseId) {
        this.leaseId = leaseId;
    }

    public long getLeaseExpiresAt() {
        return leaseExpiresAt;
    }

    public void setLeaseExpiresAt(long leaseExpiresAt) {
        this.leaseExpiresAt = leaseExpiresAt;
    }

    public String getFinishReason() {
        return finishReason;
    }

    public void setFinishReason(String finishReason) {
        this.finishReason = finishReason;
    }

    @Override
    public String toString() {
        return "Job{id=" + jobId
                + ", submitter=" + submitter
                + ", cpu=" + cpuUnits
                + ", priority=" + priority
                + ", state=" + state
                + ", attempts=" + attempts
                + ", lease=" + leaseId + "}";
    }
}
