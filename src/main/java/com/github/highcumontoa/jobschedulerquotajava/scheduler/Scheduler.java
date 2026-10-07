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
import java.util.Comparator;
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
        // 抢占通道：仅当仍有等待任务被"槽位占满 / 配额占满"卡住、且存在严格更低优先级的
        // 运行中任务时生效；无抢占需求时本通道为空操作，既有调度结论完全不变。
        preemptionPass(dispatched);
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

    private TaskExecutor executorById(String executorId) {
        for (TaskExecutor executor : executors) {
            if (executor.getId().equals(executorId)) {
                return executor;
            }
        }
        return null;
    }

    /**
     * 抢占通道（在既有正常下发之后执行，驱动方式不变：仍是 tick 触发）。
     * 对每个仍被卡住的 PENDING 任务（按既有排序），尝试抢占一个严格更低优先级的 RUNNING 任务。
     */
    private void preemptionPass(List<String> dispatched) {
        if (properties.getPreemptionMaxPerJob() <= 0) {
            return;
        }
        List<Job> waiting = selectCandidates(new ArrayList<>(jobStore.all()), clock.millis());
        for (Job preemptor : waiting) {
            // 兼容性守卫：仅当该任务此刻确实无法被正常下发（执行者槽位满 或 主体配额不足）
            // 才考虑抢占；否则保持既有行为——等下一轮正常调度，绝不因抢占通道改变结论。
            boolean slotBlocked = executors.stream().noneMatch(TaskExecutor::hasCapacity);
            boolean quotaBlocked = !quotaService.wouldFit(preemptor.getSubject(), preemptor.getDemand());
            if (!slotBlocked && !quotaBlocked) {
                continue;
            }
            tryPreemptAndDispatch(preemptor, slotBlocked, quotaBlocked, dispatched);
        }
    }

    /**
     * 为单个等待任务尝试一次"抢占 + 立即下发"。
     * 规则（全部确定性，无随机）：
     * 1) 只抢优先级严格更低、且未达到被抢次数上限（抢占免疫）的 RUNNING 任务；
     * 2) 账目预判：victim 让出的配额必须足以让 preemptor 立刻开跑，否则不做无效抢占；
     * 3) 槽位先征用（计数不变、归属移交），victim 状态迁移与 preemptor 下发分别在各自任务锁内提交，
     *    与取消/回传/租约回收互斥，并发时先到者生效、结论唯一；
     * 4) 被抢任务回 PENDING 且回滚 attempt——被抢占不消耗失败重试预算。
     */
    private boolean tryPreemptAndDispatch(Job preemptor, boolean slotBlocked, boolean quotaBlocked,
                                          List<String> dispatched) {
        for (Job victim : selectVictims(preemptor)) {
            // 1) 锁内快照 victim 当前租约；提交前会在锁内复核同一条租约仍有效。
            Lease victimLease;
            synchronized (victim.getLock()) {
                if (victim.getState() != JobState.RUNNING || victim.getLease() == null
                        || victim.getPreemptedCount() >= properties.getPreemptionMaxPerJob()) {
                    continue;
                }
                victimLease = victim.getLease();
            }
            TaskExecutor executor = executorById(victimLease.executorId());
            if (executor == null) {
                continue;
            }
            // 2) 账目预判：victim 让出的配额（同主体才惠及）+ 当前余量必须装得下 preemptor。
            if (!quotaService.wouldFitAfterRelease(preemptor, victim)) {
                log.info("[preempt-skip] jobId={} victim={} 判定依据=即使释放victim配额,主体{}配额仍不足,不做无效抢占",
                        preemptor.getJobId(), victim.getJobId(), preemptor.getSubject());
                continue;
            }
            // 3) 征用 victim 的槽位（计数不变、归属移交）；若其工作线程已结束则退化为普通预留。
            boolean seized = executor.seizeSlot(victimLease.leaseId());
            if (!seized && !executor.reserveSlot()) {
                log.info("[preempt-skip] jobId={} victim={} 判定依据=槽位既无法移交也无法预留,本次抢占放弃",
                        preemptor.getJobId(), victim.getJobId());
                continue;
            }
            // 4) 提交点 A：victim 任务锁内挤回 PENDING（归还配额、回滚 attempt、累计被抢次数）。
            //    提交前最后确认 preemptor 仍在等待，避免"为已被并发下发的任务"做无效抢占。
            synchronized (preemptor.getLock()) {
                if (preemptor.getState() != JobState.PENDING) {
                    executor.releaseSlot();
                    log.info("[preempt-skip] jobId={} victim={} 判定依据=preemptor已被并发调度处理(state={}),放弃抢占并归还槽位",
                            preemptor.getJobId(), victim.getJobId(), preemptor.getState());
                    return false;
                }
            }
            boolean victimPreempted = false;
            synchronized (victim.getLock()) {
                if (victim.getState() == JobState.RUNNING && victim.getLease() == victimLease) {
                    leaseManager.releaseQuotaOnce(victim, "preempt");
                    victim.setLease(null);
                    victim.setAttempt(victim.getAttempt() - 1);
                    victim.setPreemptedCount(victim.getPreemptedCount() + 1);
                    victim.setState(JobState.PENDING);
                    victimPreempted = true;
                }
            }
            if (!victimPreempted) {
                // 并发下 victim 已被取消/回传/回收先到处理：槽位如数归还，结论以先到者为准。
                executor.releaseSlot();
                log.info("[preempt-skip] jobId={} victim={} 判定依据=victim状态已变化(取消/回传/回收先生效),放弃抢占并归还槽位",
                        preemptor.getJobId(), victim.getJobId());
                continue;
            }
            executor.interruptWorker(victimLease.leaseId());
            log.info("[preempt-commit] preemptor={}(p={}) victim={}(p={}) victimLease={} 被抢次数={} 判定依据=等待任务被{}卡住且优先级更高,"
                            + "victim回PENDING(attempt回滚,不消耗重试预算)",
                    preemptor.getJobId(), preemptor.getPriority(), victim.getJobId(), victim.getPriority(),
                    victimLease.leaseId(), victim.getPreemptedCount(),
                    slotBlocked ? "槽位占满" : "配额占满");
            // 5) 提交点 B：preemptor 任务锁内完成"配额发放 + RUNNING + 新租约"，立刻占用让出的能力。
            Lease newLease = null;
            boolean started = false;
            synchronized (preemptor.getLock()) {
                if (preemptor.getState() == JobState.PENDING && quotaService.tryAcquire(preemptor)) {
                    preemptor.setQuotaHeld(true);
                    preemptor.setAttempt(preemptor.getAttempt() + 1);
                    preemptor.setState(JobState.RUNNING);
                    newLease = leaseManager.issue(preemptor, executor.getId(), clock.millis(),
                            properties.getLeaseTtlMs());
                    started = true;
                }
            }
            if (started) {
                executor.submit(preemptor, newLease);
                fairnessTracker.recordDispatch(preemptor.getSubject());
                dispatched.add(preemptor.getJobId());
                log.info("[preempt-dispatch] jobId={} subject={} priority={} attempt={} executor={} 判定依据=抢占成功,立即占用victim让出的槽位与配额开始执行",
                        preemptor.getJobId(), preemptor.getSubject(), preemptor.getPriority(),
                        preemptor.getAttempt(), executor.getId());
                return true;
            }
            // preemptor 并发下已被处理（如被取消）：槽位归还；victim 保持 PENDING 等待正常重调度。
            executor.releaseSlot();
            log.info("[preempt-abort] jobId={} victim={} 判定依据=preemptor并发下已被处理,槽位归还,victim等待正常重调度",
                    preemptor.getJobId(), victim.getJobId());
            return false;
        }
        return false;
    }

    /**
     * 被抢对象选择（确定性）：优先级严格更低且未免疫的 RUNNING 任务中，
     * 优先级最低者先被抢 → 同优先级租约发放时间最新（最年轻、沉没成本最小）者先被抢 → jobId 升序兜底。
     */
    private List<Job> selectVictims(Job preemptor) {
        List<Job> victims = new ArrayList<>();
        for (Job job : jobStore.all()) {
            synchronized (job.getLock()) {
                if (job.getState() == JobState.RUNNING && job.getLease() != null
                        && job.getPriority() < preemptor.getPriority()
                        && job.getPreemptedCount() < properties.getPreemptionMaxPerJob()) {
                    victims.add(job);
                }
            }
        }
        victims.sort(Comparator.comparingInt(Job::getPriority)
                .thenComparing((Job j) -> {
                    Lease lease = j.getLease();
                    return lease == null ? Long.MIN_VALUE : lease.issuedAt();
                }, Comparator.reverseOrder())
                .thenComparing(Job::getJobId));
        if (!victims.isEmpty()) {
            log.info("[preempt-order] preemptor={}(p={}) 判定依据=优先级↑,租约发放时间↓,jobId↑ 被抢候选={}",
                    preemptor.getJobId(), preemptor.getPriority(),
                    victims.stream().map(v -> v.getJobId() + "(p=" + v.getPriority()
                            + ",preempted=" + v.getPreemptedCount() + ")").toList());
        }
        return victims;
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
