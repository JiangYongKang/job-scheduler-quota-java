package com.github.highcumontoa.jobschedulerquotajava.service;

import com.github.highcumontoa.jobschedulerquotajava.config.SchedulerProperties;
import com.github.highcumontoa.jobschedulerquotajava.exception.JobRejectedException;
import com.github.highcumontoa.jobschedulerquotajava.lease.LeaseManager;
import com.github.highcumontoa.jobschedulerquotajava.model.Job;
import com.github.highcumontoa.jobschedulerquotajava.model.JobResult;
import com.github.highcumontoa.jobschedulerquotajava.model.JobSnapshot;
import com.github.highcumontoa.jobschedulerquotajava.model.JobState;
import com.github.highcumontoa.jobschedulerquotajava.model.RejectReason;
import com.github.highcumontoa.jobschedulerquotajava.model.ResourceQuota;
import com.github.highcumontoa.jobschedulerquotajava.model.SubmitRequest;
import com.github.highcumontoa.jobschedulerquotajava.quota.QuotaService;
import com.github.highcumontoa.jobschedulerquotajava.store.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.List;
import java.util.Objects;

/**
 * 任务域服务（唯一的状态迁移入口）：提交准入、取消、结果回传、查询。
 *
 * 一致性约定：
 * 所有写操作在 {@link Job#getLock()} 内完成"校验 → 状态迁移 → 配额联动"，
 * 拒绝发生在任何写入之前，因此不改变既有占用；并发的取消/回传/回收互相排斥，结论唯一。
 */
@Service
public class JobService {

    private static final Logger log = LoggerFactory.getLogger(JobService.class);

    private final JobStore jobStore;
    private final QuotaService quotaService;
    private final LeaseManager leaseManager;
    private final SchedulerProperties properties;
    private final Clock clock;

    public JobService(JobStore jobStore, QuotaService quotaService, LeaseManager leaseManager,
                      SchedulerProperties properties, Clock clock) {
        this.jobStore = jobStore;
        this.quotaService = quotaService;
        this.leaseManager = leaseManager;
        this.properties = properties;
        this.clock = clock;
    }

    public JobSnapshot submit(SubmitRequest request) {
        validate(request);
        // 1) 重复提交：同一 jobId 已存在则拒绝（幂等键冲突），不触碰任何占用。
        var existing = jobStore.find(request.jobId());
        if (existing.isPresent()) {
            Job old = existing.get();
            log.info("[submit-reject] jobId={} subject={} reason={} 判定依据=jobId已存在,state={}",
                    request.jobId(), request.subject(), RejectReason.DUPLICATE_SUBMISSION, old.getState());
            throw new JobRejectedException(RejectReason.DUPLICATE_SUBMISSION,
                    "任务已存在，重复提交被拒绝: jobId=" + request.jobId() + " currentState=" + old.getState());
        }

        // 2) 配额准入：当前占用 + 本次需求超上限则拒绝；拒绝不改占用。
        if (!quotaService.canAdmit(request.subject(), request.demand())) {
            log.info("[submit-reject] jobId={} subject={} demand={} used={} reason={} 判定依据=提交即超出主体配额上限",
                    request.jobId(), request.subject(), request.demand(),
                    quotaService.getUsed(request.subject()), RejectReason.QUOTA_EXCEEDED_ON_SUBMIT);
            throw new JobRejectedException(RejectReason.QUOTA_EXCEEDED_ON_SUBMIT,
                    "配额不足，提交被拒绝: subject=" + request.subject()
                            + " demand=" + request.demand() + " used=" + quotaService.getUsed(request.subject()));
        }

        // 3) 构造 PENDING 任务（尚未占用配额）；再次用 putIfAbsent 兜住并发提交。
        Job job = new Job(request.jobId(), request.subject(), request.demand(), request.priority(),
                request.payload(), clock.millis(), properties.getMaxAttempts());
        Job raced = jobStore.putIfAbsent(job);
        if (raced != null) {
            log.info("[submit-reject] jobId={} reason={} 判定依据=并发下jobId被抢先创建",
                    request.jobId(), RejectReason.DUPLICATE_SUBMISSION);
            throw new JobRejectedException(RejectReason.DUPLICATE_SUBMISSION,
                    "并发重复提交被拒绝: jobId=" + request.jobId());
        }
        log.info("[submit-accept] jobId={} subject={} demand={} priority={} state=PENDING quotaHeld=false 判定依据=校验通过,进入待调度队列",
                job.getJobId(), job.getSubject(), job.getDemand(), job.getPriority());
        return job.snapshot();
    }

    private void validate(SubmitRequest request) {
        if (request == null || request.jobId() == null || request.jobId().isBlank()
                || request.subject() == null || request.subject().isBlank()) {
            throw new JobRejectedException(RejectReason.INVALID_ARGUMENT, "jobId/subject 不能为空");
        }
        if (request.demand() == null || request.demand().hasNegative()) {
            throw new JobRejectedException(RejectReason.INVALID_ARGUMENT, "demand 非法: " + request.demand());
        }
    }

    /**
     * 取消任务。
     * PENDING：直接终结为 CANCELLED（从未占用配额）。
     * RUNNING：仅当租约仍属于调用上下文时有效；取消与回传/回收并发时，先拿到任务锁者生效，
     *          若任务已终结则拒绝（ILLEGAL_STATE_TRANSITION）。
     */
    public JobSnapshot cancel(String jobId) {
        Job job = jobStore.getRequired(jobId);
        synchronized (job.getLock()) {
            JobState state = job.getState();
            if (state == JobState.SUCCEEDED || state == JobState.FAILED || state == JobState.CANCELLED) {
                log.info("[cancel-reject] jobId={} state={} reason={} 判定依据=任务已终结,拒绝非法状态迁移",
                        jobId, state, RejectReason.ILLEGAL_STATE_TRANSITION);
                throw new JobRejectedException(RejectReason.ILLEGAL_STATE_TRANSITION,
                        "任务已终结，不能取消: jobId=" + jobId + " state=" + state);
            }
            if (state == JobState.RUNNING) {
                leaseManager.releaseQuotaOnce(job, "cancel");
                job.setLease(null);
            }
            job.setState(JobState.CANCELLED);
            log.info("[cancel-accept] jobId={} state=CANCELLED attempt={} quotaHeld={} 判定依据=取消生效,{}配额已归还",
                    jobId, state, job.getAttempt(), job.isQuotaHeld(),
                    state == JobState.RUNNING ? "运行中" : "未占用");
            return job.snapshot();
        }
    }

    /**
     * 执行者回传结果。
     * 校验链（任一不过即拒绝，原因可区分）：存在 → 状态 RUNNING → 租约 id → 执行者归属 → 租约未过期。
     * 成功：SUCCEEDED + 归还配额。
     * 失败：仍有重试额度 → attempt+1、归还配额、回到 PENDING；否则 FAILED(RETRY_LIMIT_EXHAUSTED)。
     */
    public JobSnapshot report(JobResult result) {
        Objects.requireNonNull(result, "result");
        Job job = jobStore.getRequired(result.jobId());
        synchronized (job.getLock()) {
            if (job.isTerminal()) {
                log.info("[report-reject] jobId={} state={} reason={} 判定依据=任务已终结,结果回传不改变终态",
                        job.getJobId(), job.getState(), RejectReason.ILLEGAL_STATE_TRANSITION);
                throw new JobRejectedException(RejectReason.ILLEGAL_STATE_TRANSITION,
                        "任务已终结，结果回传被拒绝: jobId=" + job.getJobId() + " state=" + job.getState());
            }
            long now = clock.millis();
            if (!leaseManager.isValidReport(job, result, now)) {
                // isValidReport 内部已按可区分原因打印日志；这里统一以租约/状态类原因拒绝。
                RejectReason reason = classifyInvalidReport(job, result, now);
                throw new JobRejectedException(reason,
                        "结果回传被拒绝: jobId=" + job.getJobId() + " reason=" + reason);
            }

            applyResult(job, result);
            return job.snapshot();
        }
    }

    /** 根据当前任务/租约状态把无效回传归类为可区分原因（调用方持锁）。 */
    private RejectReason classifyInvalidReport(Job job, JobResult result, long now) {
        if (job.getState() != JobState.RUNNING) {
            return RejectReason.ILLEGAL_STATE_TRANSITION;
        }
        var lease = job.getLease();
        if (lease == null || !lease.leaseId().equals(result.leaseId())) {
            return RejectReason.LEASE_EXPIRED;
        }
        if (!lease.executorId().equals(result.executorId())) {
            return RejectReason.LEASE_OWNER_MISMATCH;
        }
        return RejectReason.LEASE_EXPIRED;
    }

    /** 应用一份已通过校验的执行结果（调用方持有任务锁）。 */
    private void applyResult(Job job, JobResult result) {
        // 进入终态/重排队之前先归还配额并清租约，保证配额只归还一次。
        leaseManager.releaseQuotaOnce(job, "report");
        job.setLease(null);
        job.setResultDetail(result.detail());

        if (result.success()) {
            job.setState(JobState.SUCCEEDED);
            log.info("[job-terminal] jobId={} state=SUCCEEDED attempt={} quotaHeld=false detail={} 判定依据=执行者回传成功,配额归还",
                    job.getJobId(), job.getAttempt(), result.detail());
            return;
        }

        if (job.getAttempt() >= job.getMaxRetries()) {
            job.setState(JobState.FAILED);
            job.setTerminalReason(RejectReason.RETRY_LIMIT_EXHAUSTED);
            log.info("[job-terminal] jobId={} state=FAILED reason={} attempt={} maxRetries={} quotaHeld=false 判定依据=执行失败且重试上限耗尽,终结并归还配额",
                    job.getJobId(), RejectReason.RETRY_LIMIT_EXHAUSTED, job.getAttempt(), job.getMaxRetries());
        } else {
            job.setState(JobState.PENDING);
            log.info("[job-retry] jobId={} state=PENDING attempt={} maxRetries={} quotaHeld=false detail={} 判定依据=执行失败但仍有重试额度,重新排队",
                    job.getJobId(), job.getAttempt(), job.getMaxRetries(), result.detail());
        }
    }

    public JobSnapshot get(String jobId) {
        return jobStore.getRequired(jobId).snapshot();
    }

    public List<JobSnapshot> list() {
        return jobStore.all().stream().map(Job::snapshot).toList();
    }

    public void setQuotaLimit(String subject, ResourceQuota limit) {
        quotaService.setLimit(subject, limit);
        log.info("[quota-limit] subject={} limit={}", subject, limit);
    }

    public ResourceQuota getUsedQuota(String subject) {
        return quotaService.getUsed(subject);
    }
}
