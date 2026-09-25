package com.github.highcumontoa.jobschedulerquotajava.lease;

import com.github.highcumontoa.jobschedulerquotajava.config.SchedulerProperties;
import com.github.highcumontoa.jobschedulerquotajava.model.Job;
import com.github.highcumontoa.jobschedulerquotajava.model.JobResult;
import com.github.highcumontoa.jobschedulerquotajava.model.JobState;
import com.github.highcumontoa.jobschedulerquotajava.model.Lease;
import com.github.highcumontoa.jobschedulerquotajava.model.RejectReason;
import com.github.highcumontoa.jobschedulerquotajava.quota.QuotaService;
import com.github.highcumontoa.jobschedulerquotajava.store.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 租约管理：发放、校验（执行者归属 + 是否过期）、过期/失联回收。
 *
 * 回收语义（全部在任务锁内完成，与取消、结果回传互斥）：
 * RUNNING 且租约到期 → 先归还配额 →
 *   仍有重试额度：attempt+1 并回到 PENDING，等待重新挑选下发（重新发放配额）；
 *   已达上限：终结为 FAILED(RETRY_LIMIT_EXHAUSTED)。
 */
@Component
public class LeaseManager {

    private static final Logger log = LoggerFactory.getLogger(LeaseManager.class);

    private final JobStore jobStore;
    private final QuotaService quotaService;
    private final SchedulerProperties properties;

    public LeaseManager(JobStore jobStore, QuotaService quotaService, SchedulerProperties properties) {
        this.jobStore = jobStore;
        this.quotaService = quotaService;
        this.properties = properties;
    }

    /** 为任务发放新租约。调用方已持有任务锁，并已成功发放配额、状态已置 RUNNING。 */
    public Lease issue(Job job, String executorId, long nowMs, long ttlMs) {
        Lease lease = new Lease(
                "lease-" + UUID.randomUUID(),
                job.getJobId(),
                executorId,
                nowMs,
                nowMs + ttlMs,
                job.getAttempt());
        job.setLease(lease);
        log.info("[lease-issue] jobId={} leaseId={} executor={} attempt={} expiresAt={}",
                job.getJobId(), lease.leaseId(), executorId, lease.attempt(), lease.expiresAt());
        return lease;
    }

    public long ttlMs() {
        return properties.getLeaseTtlMs();
    }

    /** 回收所有过期租约，返回被回收（重新排队或终结）的任务 id。 */
    public List<String> reclaimExpired(long nowMs) {
        List<String> affected = new ArrayList<>();
        for (Job job : jobStore.all()) {
            synchronized (job.getLock()) {
                Lease lease = job.getLease();
                if (job.getState() == JobState.RUNNING && lease != null && lease.isExpired(nowMs)) {
                    log.info("[lease-expired] jobId={} leaseId={} executor={} now={} expiresAt={} 判定依据=租约到期,开始回收",
                            job.getJobId(), lease.leaseId(), lease.executorId(), nowMs, lease.expiresAt());
                    reclaimLocked(job, nowMs);
                    affected.add(job.getJobId());
                }
            }
        }
        return affected;
    }

    /** 模拟"执行者失联"：无视是否到期，强制回收该任务的租约。 */
    public boolean reclaim(Job job, long nowMs) {
        synchronized (job.getLock()) {
            if (job.getState() != JobState.RUNNING || job.getLease() == null) {
                return false;
            }
            log.info("[lease-lost] jobId={} leaseId={} 判定依据=执行者失联,开始回收",
                    job.getJobId(), job.getLease().leaseId());
            reclaimLocked(job, nowMs);
            return true;
        }
    }

    /**
     * 锁内回收：调用方必须持有 job.getLock()，且当前状态为 RUNNING。
     */
    private void reclaimLocked(Job job, long nowMs) {
        Lease oldLease = job.getLease();
        // 1) 先归还占用配额（仅当仍在占用中），并翻转标记，杜绝重复归还。
        releaseQuotaOnce(job, "lease-reclaim");
        job.setLease(null);

        // 2) 重试判定：当前 attempt 已用掉的次数 >= 最大尝试次数，则终结。
        if (job.getAttempt() >= job.getMaxRetries()) {
            job.setState(JobState.FAILED);
            job.setTerminalReason(RejectReason.RETRY_LIMIT_EXHAUSTED);
            job.setResultDetail("租约回收(" + (oldLease == null ? "-" : oldLease.leaseId()) + ")后重试次数耗尽");
            log.info("[job-terminal] jobId={} state=FAILED reason={} attempt={} quotaHeld={} 判定依据=已达最大尝试次数{},终结并归还配额",
                    job.getJobId(), RejectReason.RETRY_LIMIT_EXHAUSTED, job.getAttempt(), job.isQuotaHeld(),
                    job.getMaxRetries());
        } else {
            job.setState(JobState.PENDING);
            log.info("[job-requeue] jobId={} state=PENDING attempt={} maxRetries={} quotaHeld={} 判定依据=租约失效,重新排队等待调度",
                    job.getJobId(), job.getAttempt(), job.getMaxRetries(), job.isQuotaHeld());
        }
    }

    /** 在任务锁内把配额归还且只归还一次（回收 / 终结路径共用）。 */
    public void releaseQuotaOnce(Job job, String scene) {
        if (job.isQuotaHeld()) {
            quotaService.release(job);
            job.setQuotaHeld(false);
            log.info("[quota-release-once] jobId={} scene={} quotaHeld=false 判定依据=任务离开RUNNING,配额归还一次",
                    job.getJobId(), scene);
        } else {
            log.info("[quota-release-skip] jobId={} scene={} quotaHeld=false 判定依据=配额未在占用,跳过归还(防止重复归还)",
                    job.getJobId(), scene);
        }
    }

    /**
     * 校验执行者回传是否可接受（调用方持有任务锁）：
     * 状态必须 RUNNING、租约存在、租约 id 与执行者一致、且租约未过期。
     */
    public boolean isValidReport(Job job, JobResult result, long nowMs) {
        Lease lease = job.getLease();
        if (job.getState() != JobState.RUNNING) {
            log.info("[report-reject] jobId={} reason={} 判定依据=任务非RUNNING(current={})",
                    job.getJobId(), RejectReason.ILLEGAL_STATE_TRANSITION, job.getState());
            return false;
        }
        if (lease == null || !lease.leaseId().equals(result.leaseId())) {
            log.info("[report-reject] jobId={} reason={} 判定依据=租约id不匹配或租约已被回收",
                    job.getJobId(), RejectReason.LEASE_EXPIRED);
            return false;
        }
        if (!lease.executorId().equals(result.executorId())) {
            log.info("[report-reject] jobId={} reason={} 判定依据=执行者不属于该租约 expect={} actual={}",
                    job.getJobId(), RejectReason.LEASE_OWNER_MISMATCH, lease.executorId(), result.executorId());
            return false;
        }
        if (lease.isExpired(nowMs)) {
            log.info("[report-reject] jobId={} reason={} 判定依据=租约已过期 expiresAt={} now={}",
                    job.getJobId(), RejectReason.LEASE_EXPIRED, lease.expiresAt(), nowMs);
            return false;
        }
        return true;
    }
}
