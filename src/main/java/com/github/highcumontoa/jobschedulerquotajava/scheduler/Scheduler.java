package com.github.highcumontoa.jobschedulerquotajava.scheduler;

import com.github.highcumontoa.jobschedulerquotajava.config.SchedulerProperties;
import com.github.highcumontoa.jobschedulerquotajava.executor.TaskExecutor;
import com.github.highcumontoa.jobschedulerquotajava.lease.LeaseManager;
import com.github.highcumontoa.jobschedulerquotajava.model.Job;
import com.github.highcumontoa.jobschedulerquotajava.model.JobState;
import com.github.highcumontoa.jobschedulerquotajava.model.Lease;
import com.github.highcumontoa.jobschedulerquotajava.quota.QuotaService;
import com.github.highcumontoa.jobschedulerquotajava.store.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 调度器：从 PENDING 队列中按"优先级（高者）→ 主体公平（历史下发少者）→ 提交时间（早者）
 * → jobId（确定性兜底）"挑选任务并下发给本地执行者。
 *
 * 并发与配额：
 * 1) 多线程同时 tick 时，任务在锁内二次确认 PENDING 并迁移到 RUNNING，同一任务不会被下发两次；
 * 2) 执行者槽位先原子 reserve，成功后才进入任务临界区，失败则回滚槽位；
 * 3) 配额"检查+发放"在 {@link QuotaService#tryAcquire} 内按主体原子完成；
 *    配额不足时任务保持 PENDING 继续等待，绝不越权下发。
 */
@Component
public class Scheduler {

    private static final Logger log = LoggerFactory.getLogger(Scheduler.class);

    private final CopyOnWriteArrayList<TaskExecutor> executors = new CopyOnWriteArrayList<>();
    private final AtomicInteger rrIndex = new AtomicInteger(0);

    private final JobStore jobStore;
    private final QuotaService quotaService;
    private final LeaseManager leaseManager;
    private final FairnessTracker fairnessTracker;
    private final SchedulerProperties properties;
    private final Clock clock;

    public Scheduler(JobStore jobStore, QuotaService quotaService, LeaseManager leaseManager,
                     FairnessTracker fairnessTracker, SchedulerProperties properties, Clock clock) {
        this.jobStore = jobStore;
        this.quotaService = quotaService;
        this.leaseManager = leaseManager;
        this.fairnessTracker = fairnessTracker;
        this.properties = properties;
        this.clock = clock;
    }

    public void registerExecutor(TaskExecutor executor) {
        executors.addIfAbsent(executor);
        log.info("[scheduler-register] executor={} total={}", executor.getId(), executors.size());
    }

    /** 执行一轮挑选与下发，返回本轮成功下发的任务 id。 */
    public List<String> tick() {
        List<Job> candidates = selectCandidates(new ArrayList<>(jobStore.all()), clock.millis());
        List<String> dispatched = new ArrayList<>();

        for (Job job : candidates) {
            // 1) 先原子占用执行者槽位；所有执行者都满时跳过本任务继续尝试后续
            //    （不 break：不同执行者容量恢复节奏不同，且本任务放不下不代表后续任务一定放不下）。
            TaskExecutor executor = reserveAnExecutor();
            if (executor == null) {
                log.info("[schedule-skip] jobId={} 判定依据=所有执行者槽位已满,任务继续等待", job.getJobId());
                continue;
            }

            Lease lease;
            boolean committed = false;
            try {
                synchronized (job.getLock()) {
                    // 2) 并发二次确认：只有仍在 PENDING 的任务才能被下发。
                    if (job.getState() != JobState.PENDING) {
                        log.info("[schedule-skip] jobId={} 判定依据=并发下已被其他调度线程处理(state={}),跳过",
                                job.getJobId(), job.getState());
                        continue;
                    }
                    // 3) 原子配额准入：不足则保持 PENDING 继续等待（绝不越权下发）。
                    if (!quotaService.tryAcquire(job)) {
                        continue;
                    }
                    // 4) 提交点：配额标记 + 状态 + 尝试次数 + 租约在同一把任务锁内一起生效。
                    job.setQuotaHeld(true);
                    job.setAttempt(job.getAttempt() + 1);
                    job.setState(JobState.RUNNING);
                    lease = leaseManager.issue(job, executor.getId(), clock.millis(), properties.getLeaseTtlMs());
                    committed = true;
                }

                // 5) 锁外异步执行（submit 仅入队，不阻塞），并记录公平性记账。
                executor.submit(job, lease);
                fairnessTracker.recordDispatch(job.getSubject());
                long seq = job.getScheduleSequence().incrementAndGet();
                dispatched.add(job.getJobId());
                log.info("[schedule-dispatch] jobId={} subject={} priority={} attempt={} executor={} leaseId={} fairSeq={} 判定依据=排序选中且配额充足,下发执行",
                        job.getJobId(), job.getSubject(), job.getPriority(), job.getAttempt(),
                        executor.getId(), lease.leaseId(), seq);
            } finally {
                // 未到达提交点（非 PENDING / 配额不足）的路径归还预留槽位；
                // 已提交的槽位由执行者在任务结束（含回传被拒）时 releaseSlot 归还。
                if (!committed) {
                    executor.releaseSlot();
                }
            }
        }
        log.info("[schedule-tick] candidates={} dispatched={} result={}", candidates.size(), dispatched.size(), dispatched);
        return dispatched;
    }

    /** 轮询找到一个有容量并成功预留槽位的执行者；无可用返回 null。 */
    private TaskExecutor reserveAnExecutor() {
        int n = executors.size();
        if (n == 0) {
            return null;
        }
        int start = Math.floorMod(rrIndex.getAndIncrement(), n);
        for (int i = 0; i < n; i++) {
            TaskExecutor executor = executors.get((start + i) % n);
            if (executor.reserveSlot()) {
                return executor;
            }
        }
        return null;
    }

    /**
     * 从全量任务中挑出 PENDING 候选并按可解释规则排序：
     * 优先级降序 → 主体历史下发次数升序（公平）→ 提交时间升序 → jobId 升序。
     */
    public List<Job> selectCandidates(List<Job> all, long nowMs) {
        List<Map.Entry<Job, Long>> pending = new ArrayList<>();
        for (Job job : all) {
            synchronized (job.getLock()) {
                if (job.getState() == JobState.PENDING) {
                    pending.add(new AbstractMap.SimpleEntry<>(job, fairnessTracker.dispatchCount(job.getSubject())));
                }
            }
        }
        pending.sort(fairnessTracker.comparator());
        List<Job> ordered = pending.stream().map(Map.Entry::getKey).toList();
        if (!ordered.isEmpty()) {
            log.info("[schedule-order] now={} 判定依据=优先级↓,主体下发次数↑,提交时间↑,jobId↑ 顺序={}",
                    nowMs,
                    ordered.stream().map(j -> j.getJobId()
                            + "(p=" + j.getPriority()
                            + ",subject=" + j.getSubject()
                            + ",served=" + fairnessTracker.dispatchCount(j.getSubject())
                            + ",createdAt=" + j.getCreatedAt() + ")").toList());
        }
        return ordered;
    }
}
