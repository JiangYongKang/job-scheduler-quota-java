package com.github.highcumontoa.jobschedulerquotajava.core;

import com.github.highcumontoa.jobschedulerquotajava.exception.SchedulerException;
import com.github.highcumontoa.jobschedulerquotajava.model.DispatchResult;
import com.github.highcumontoa.jobschedulerquotajava.model.Job;
import com.github.highcumontoa.jobschedulerquotajava.model.JobRequest;
import com.github.highcumontoa.jobschedulerquotajava.model.JobState;
import com.github.highcumontoa.jobschedulerquotajava.model.RejectReason;
import com.github.highcumontoa.jobschedulerquotajava.model.SubmitResult;
import com.github.highcumontoa.jobschedulerquotajava.quota.QuotaManager;
import com.github.highcumontoa.jobschedulerquotajava.store.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.Comparator;
import java.util.UUID;

/**
 * 调度与配额领域的唯一权威入口。
 *
 * <h2>一致性约定</h2>
 * <ul>
 *   <li><b>单锁模型</b>：所有状态流转与配额变动都在 {@link #lock} 同一把锁的临界区内完成，
 *       HTTP 入口、本地执行者、租约回收器、测试线程并发进入也只会串行化，
 *       因此"同一任务被多个执行者同时下发"在结构上不可能发生。</li>
 *   <li><b>配额模型</b>：仅 RUNNING（占用中）任务计数。提交时按当前占用做准入校验，
 *       超配额拒绝且不改变占用；PENDING 任务不预留配额，下发时在同一临界区内原子获取，
 *       余额不足则跳过继续等待，绝不越权下发。</li>
 *   <li><b>租约栅栏（fencing）</b>：每次下发生成新的 leaseId；结果回传必须携带当前
 *       leaseId，旧执行者/超时租约的回传一律按"过期租约"拒绝。</li>
 *   <li><b>终态一次性</b>：进入终态与归还配额在同一临界区内完成，任务离开 RUNNING
 *       的路径唯一，配额有且仅有一次归还（QuotaManager 另有下限保护兜底）。</li>
 * </ul>
 */
@Service
public class JobSchedulerService {

    private static final Logger log = LoggerFactory.getLogger(JobSchedulerService.class);

    /** 挑选顺序：优先级数值大者优先；同优先级按入队序号 FIFO；再以 jobId 兜底，保证全序可解释。 */
    private static final Comparator<Job> PICK_ORDER = Comparator
            .comparingInt(Job::getPriority).reversed()
            .thenComparingLong(Job::getEnqueueSeq)
            .thenComparing(Job::getJobId);

    private final SchedulerProperties properties;
    private final JobStore store = new JobStore();
    private final QuotaManager quota;
    private final Object lock = new Object();

    private Clock clock = Clock.systemUTC();
    private long enqueueSequence = 0;

    public JobSchedulerService(SchedulerProperties properties) {
        this.properties = properties;
        this.quota = new QuotaManager(properties.getDefaultQuota());
        log.info("调度器初始化: defaultQuota={}, maxAttempts={}, leaseTtlMillis={}",
                properties.getDefaultQuota(), properties.getMaxAttempts(), properties.getLeaseTtlMillis());
    }

    private long now() {
        return clock.millis();
    }

    // ------------------------------------------------------------------
    // 提交与配额准入
    // ------------------------------------------------------------------

    public SubmitResult submit(JobRequest request) {
        validate(request);
        synchronized (lock) {
            Job existing = store.get(request.jobId());
            if (existing != null && !existing.getState().isTerminal()) {
                log.info("准入拒绝[重复提交冲突]: jobId={}, submitter={}, 既有状态={}, 依据=相同id存在未终结任务",
                        request.jobId(), request.submitter(), existing.getState());
                return new SubmitResult(false, request.jobId(), RejectReason.DUPLICATE_SUBMISSION,
                        "任务 " + request.jobId() + " 已存在且状态为 " + existing.getState()
                                + "，重复提交冲突，既有占用保持不变");
            }

            int used = quota.usedOf(request.submitter());
            int limit = quota.limitOf(request.submitter());
            if (used + request.cpuUnits() > limit) {
                log.info("准入拒绝[配额超限]: jobId={}, submitter={}, 申请={}, 占用={}/{}, 依据=占用+申请>配额, 拒绝后占用仍={}",
                        request.jobId(), request.submitter(), request.cpuUnits(), used, limit, used);
                return new SubmitResult(false, request.jobId(), RejectReason.QUOTA_EXCEEDED,
                        "提交主体 " + request.submitter() + " 配额不足: 申请 " + request.cpuUnits()
                                + ", 当前占用 " + used + "/" + limit + "，拒绝准入且不改变既有占用");
            }

            long seq = ++enqueueSequence;
            Job job = new Job(request.jobId(), request.submitter(), request.cpuUnits(),
                    request.priority(), seq);
            store.put(job);
            log.info("准入通过: jobId={}, submitter={}, 申请={}, 占用={}/{} (PENDING不占配额), 优先级={}, 公平序号={}",
                    job.getJobId(), job.getSubmitter(), job.getCpuUnits(), used, limit,
                    job.getPriority(), seq);
            return new SubmitResult(true, job.getJobId(), null,
                    "已准入并进入 PENDING 队列，公平序号=" + seq);
        }
    }

    private void validate(JobRequest request) {
        if (request == null) {
            throw new SchedulerException("提交请求不能为空");
        }
        if (request.jobId() == null || request.jobId().isBlank()) {
            throw new SchedulerException("jobId 不能为空");
        }
        if (request.submitter() == null || request.submitter().isBlank()) {
            throw new SchedulerException("submitter 不能为空");
        }
        if (request.cpuUnits() <= 0) {
            throw new SchedulerException("cpuUnits 必须为正数: " + request.cpuUnits());
        }
    }

    // ------------------------------------------------------------------
    // 挑选与下发
    // ------------------------------------------------------------------

    /**
     * 在所有 PENDING 任务中按"优先级 + FIFO"全序挑选第一个配额可满足的任务下发。
     * 排在前面但当前配额不足的任务会阻塞其后的低优先级任务（不抢占、不插队），
     * 该任务保持 PENDING 继续等待；本方法每次最多下发一个任务。
     */
    public DispatchResult dispatchOne() {
        synchronized (lock) {
            Job picked = store.all().stream()
                    .filter(j -> j.getState() == JobState.PENDING)
                    .min(PICK_ORDER)
                    .orElse(null);
            if (picked == null) {
                return new DispatchResult(false, null, null, "无 PENDING 任务可调度");
            }

            int used = quota.usedOf(picked.getSubmitter());
            int limit = quota.limitOf(picked.getSubmitter());
            if (used + picked.getCpuUnits() > limit) {
                log.info("下发等待: jobId={}, submitter={}, 需要={}, 占用={}/{}, 依据=运行中配额不足, 任务保持PENDING, 不越权下发",
                        picked.getJobId(), picked.getSubmitter(), picked.getCpuUnits(), used, limit);
                return new DispatchResult(false, picked.getJobId(), null,
                        "任务 " + picked.getJobId() + " 配额不足等待中: 需要 "
                                + picked.getCpuUnits() + ", 当前占用 " + used + "/" + limit);
            }

            // 挑选、配额获取、状态翻转在同一临界区：其它调度线程拿不到锁，不可能重复下发。
            quota.tryAcquire(picked.getSubmitter(), picked.getCpuUnits());
            String leaseId = UUID.randomUUID().toString();
            picked.setState(JobState.RUNNING);
            picked.setAttempts(picked.getAttempts() + 1);
            picked.setLeaseId(leaseId);
            picked.setLeaseExpiresAt(now() + properties.getLeaseTtlMillis());
            log.info("下发成功: jobId={}, submitter={}, leaseId={}, 第{}次执行, 需要={}, 下发后占用={}/{}, TTL={}ms",
                    picked.getJobId(), picked.getSubmitter(), leaseId, picked.getAttempts(),
                    picked.getCpuUnits(), quota.usedOf(picked.getSubmitter()), limit,
                    properties.getLeaseTtlMillis());
            return new DispatchResult(true, picked.getJobId(), leaseId,
                    "已下发给本地执行者，租约=" + leaseId + ", 第 " + picked.getAttempts() + " 次执行");
        }
    }

    // ------------------------------------------------------------------
    // 执行结果回传
    // ------------------------------------------------------------------

    /**
     * 执行者回传结果。leaseId 必须与当前租约一致，否则视为过期/冒名回传，拒绝且不改变任何状态。
     * 成功 → SUCCEEDED；失败且未超重试上限 → 回收并重新排队（PENDING）；失败且达上限 → ABANDONED。
     */
    public void complete(String jobId, String leaseId, boolean success) {
        synchronized (lock) {
            Job job = requireJob(jobId);
            // 先做租约栅栏校验：任务当前持有租约必须与回传凭证一致。
            // 取消/超时回收/重试重排都会使旧 leaseId 失效（置 null），因此迟到回传一律在此被拒，
            // 无论任务此刻是 PENDING 还是 CANCELLED/ABANDONED 等终态，结论唯一且可解释。
            if (job.getLeaseId() == null || !job.getLeaseId().equals(leaseId)) {
                log.info("回传拒绝[过期租约]: jobId={}, 回传leaseId={}, 当前leaseId={}, 当前状态={}, 依据=租约栅栏不匹配",
                        jobId, leaseId, job.getLeaseId(), job.getState());
                throw new SchedulerException("过期租约回传被拒绝: 任务 " + jobId
                        + " 当前状态 " + job.getState()
                        + "，回传凭证租约已失效（超时回收/取消/重排后旧执行者迟到回传），结论以当前租约为准");
            }
            if (job.getState() != JobState.RUNNING) {
                throw new SchedulerException("非法状态迁移: 任务 " + jobId + " 当前状态 " + job.getState()
                        + "，只有 RUNNING 任务可以回传结果");
            }

            if (success) {
                finishTerminal(job, JobState.SUCCEEDED, "执行成功");
                log.info("任务成功: jobId={}, submitter={}, 占用释放={}, 释放后占用={}/{}",
                        jobId, job.getSubmitter(), job.getCpuUnits(),
                        quota.usedOf(job.getSubmitter()), quota.limitOf(job.getSubmitter()));
                return;
            }

            int max = properties.getMaxAttempts();
            if (job.getAttempts() >= max) {
                finishTerminal(job, JobState.ABANDONED,
                        "执行失败且已达重试上限(" + max + "次)，终结不再重试");
                log.info("任务终结[重试上限]: jobId={}, attempts={}/{}, submitter={}, 释放后占用={}/{}",
                        jobId, job.getAttempts(), max, job.getSubmitter(),
                        quota.usedOf(job.getSubmitter()), quota.limitOf(job.getSubmitter()));
            } else {
                releaseToPending(job, "执行失败，第 " + job.getAttempts() + " 次尝试未达上限，重新排队");
                log.info("任务重新排队[失败重试]: jobId={}, attempts={}/{}, submitter={}, 释放后占用={}/{}",
                        jobId, job.getAttempts(), max, job.getSubmitter(),
                        quota.usedOf(job.getSubmitter()), quota.limitOf(job.getSubmitter()));
            }
        }
    }

    // ------------------------------------------------------------------
    // 取消
    // ------------------------------------------------------------------

    /**
     * 取消任务。PENDING 直接转 CANCELLED（本就不占配额）；RUNNING 转 CANCELLED 并归还配额；
     * 终态任务不可再取消（非法状态迁移）。取消后旧执行者的迟到回传会被租约栅栏拒绝。
     */
    public void cancel(String jobId) {
        synchronized (lock) {
            Job job = requireJob(jobId);
            switch (job.getState()) {
                case PENDING -> {
                    job.setState(JobState.CANCELLED);
                    job.setFinishReason("等待中被取消");
                    log.info("任务取消[PENDING]: jobId={}, submitter={}, PENDING不占配额无需释放",
                            jobId, job.getSubmitter());
                }
                case RUNNING -> {
                    finishTerminal(job, JobState.CANCELLED, "运行中被取消");
                    log.info("任务取消[RUNNING]: jobId={}, leaseId={}, submitter={}, 释放后占用={}/{}",
                            jobId, leaseOf(job), job.getSubmitter(),
                            quota.usedOf(job.getSubmitter()), quota.limitOf(job.getSubmitter()));
                }
                default -> throw new SchedulerException("非法状态迁移: 任务 " + jobId + " 已处于终态 "
                        + job.getState() + "，不可取消");
            }
        }
    }

    // ------------------------------------------------------------------
    // 租约超时回收
    // ------------------------------------------------------------------

    /**
     * 扫描并回收所有租约超时的 RUNNING 任务（模拟执行者失联）。
     * 未达重试上限则重新排队，已达上限则 ABANDONED；返回本次回收的任务数。
     */
    public int reclaimExpired() {
        synchronized (lock) {
            long now = now();
            int reclaimed = 0;
            for (Job job : store.all()) {
                if (!job.isLeaseExpired(now)) {
                    continue;
                }
                int max = properties.getMaxAttempts();
                if (job.getAttempts() >= max) {
                    String oldLease = job.getLeaseId();
                    finishTerminal(job, JobState.ABANDONED,
                            "租约超时且已达重试上限(" + max + "次)，判定执行者失联，终结");
                    log.info("租约回收[终结]: jobId={}, 过期leaseId={}, attempts={}/{}, now={}>={}, 释放后占用={}/{}",
                            job.getJobId(), oldLease, job.getAttempts(), max, now,
                            job.getLeaseExpiresAt(), quota.usedOf(job.getSubmitter()),
                            quota.limitOf(job.getSubmitter()));
                } else {
                    String oldLease = job.getLeaseId();
                    long expiresAt = job.getLeaseExpiresAt();
                    releaseToPending(job, "租约超时，执行者失联，重新排队等待再次下发");
                    log.info("租约回收[重排]: jobId={}, 过期leaseId={}, attempts={}/{}, now={}>={}, 释放后占用={}/{}",
                            job.getJobId(), oldLease, job.getAttempts(), max, now, expiresAt,
                            quota.usedOf(job.getSubmitter()), quota.limitOf(job.getSubmitter()));
                }
                reclaimed++;
            }
            return reclaimed;
        }
    }

    // ------------------------------------------------------------------
    // 状态机内部原语（调用时必须持有 lock）
    // ------------------------------------------------------------------

    /** RUNNING → 终态：唯一的"归还配额"出口，任务终态后不再可能再次进入此方法。 */
    private void finishTerminal(Job job, JobState terminal, String reason) {
        if (job.getState() != JobState.RUNNING) {
            throw new SchedulerException("内部错误: 仅 RUNNING 可终结, jobId=" + job.getJobId()
                    + ", state=" + job.getState());
        }
        quota.release(job.getSubmitter(), job.getCpuUnits());
        job.setState(terminal);
        job.setLeaseId(null);
        job.setLeaseExpiresAt(0L);
        job.setFinishReason(reason);
    }

    /** RUNNING → PENDING：归还配额并使旧租约失效，等待下一轮按优先级重新挑选。 */
    private void releaseToPending(Job job, String reason) {
        if (job.getState() != JobState.RUNNING) {
            throw new SchedulerException("内部错误: 仅 RUNNING 可回收重排, jobId=" + job.getJobId());
        }
        quota.release(job.getSubmitter(), job.getCpuUnits());
        job.setState(JobState.PENDING);
        job.setLeaseId(null);
        job.setLeaseExpiresAt(0L);
        job.setFinishReason(reason);
    }

    private Job requireJob(String jobId) {
        Job job = store.get(jobId);
        if (job == null) {
            throw new SchedulerException("任务不存在: " + jobId);
        }
        return job;
    }

    private static String leaseOf(Job job) {
        return job.getLeaseId();
    }

    // ------------------------------------------------------------------
    // 查询与测试支撑
    // ------------------------------------------------------------------

    public Job getJob(String jobId) {
        synchronized (lock) {
            return requireJob(jobId);
        }
    }

    public int usedQuota(String submitter) {
        synchronized (lock) {
            return quota.usedOf(submitter);
        }
    }

    public int quotaLimit(String submitter) {
        synchronized (lock) {
            return quota.limitOf(submitter);
        }
    }

    /** 注入可控时钟，供测试精确制造租约超时；生产使用系统 UTC 时钟。 */
    public void setClock(Clock clock) {
        synchronized (lock) {
            this.clock = clock;
        }
    }
}
