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
 *
 * 抢占：候选任务因槽位满或配额不足无法下发时，若其优先级严格高于某个运行中任务，
 * 按确定性规则（优先级最低 → 租约最新 → jobId 最小）选出受害者挤下：
 * 受害者归还配额、退还本次尝试、回到 PENDING 等待重新调度；抢占者复用同一提交点立即下发。
 * 无更低优先级运行任务时完全不进入抢占路径，行为与无抢占能力时一致。
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
            // 1) 先原子占用执行者槽位；所有执行者都满时进入抢占路径
            //    （不 break：不同执行者容量恢复节奏不同，且本任务放不下不代表后续任务一定放不下）。
            TaskExecutor executor = reserveAnExecutor();
            if (executor != null) {
                DispatchResult result = commitDispatch(job, executor);
                if (result == DispatchResult.COMMITTED) {
                    dispatched.add(job.getJobId());
                    continue;
                }
                // 未到达提交点（非 PENDING / 配额不足）的路径归还预留槽位。
                executor.releaseSlot();
                if (result == DispatchResult.NOT_PENDING) {
                    continue;
                }
                // QUOTA_SHORT：落入抢占路径。
            } else {
                log.info("[schedule-skip] jobId={} 判定依据=所有执行者槽位已满,任务继续等待", job.getJobId());
            }

            // 2) 执行能力或主体配额被占满：高优先级等待任务尝试抢占低优先级运行任务。
            if (preemptAndDispatch(job)) {
                dispatched.add(job.getJobId());
            }
        }
        log.info("[schedule-tick] candidates={} dispatched={} result={}", candidates.size(), dispatched.size(), dispatched);
        return dispatched;
    }

    /** 提交结果：已提交（槽位被消耗）/ 任务已非 PENDING / 配额不足。 */
    private enum DispatchResult { COMMITTED, NOT_PENDING, QUOTA_SHORT }

    /**
     * 在已持有执行者槽位的前提下完成"任务锁内提交点"：
     * 并发二次确认 PENDING → 原子配额准入 → 配额标记 + 状态 + 尝试次数 + 租约同时生效 → 锁外异步执行。
     * 返回 COMMITTED 时槽位被消耗（由执行者在任务结束时归还）；其余情况调用方负责还槽。
     */
    private DispatchResult commitDispatch(Job job, TaskExecutor executor) {
        Lease lease;
        synchronized (job.getLock()) {
            // 并发二次确认：只有仍在 PENDING 的任务才能被下发。
            if (job.getState() != JobState.PENDING) {
                log.info("[schedule-skip] jobId={} 判定依据=并发下已被其他调度线程处理(state={}),跳过",
                        job.getJobId(), job.getState());
                return DispatchResult.NOT_PENDING;
            }
            // 原子配额准入：不足则保持 PENDING 继续等待（绝不越权下发）。
            if (!quotaService.tryAcquire(job)) {
                return DispatchResult.QUOTA_SHORT;
            }
            // 提交点：配额标记 + 状态 + 尝试次数 + 租约在同一把任务锁内一起生效。
            job.setQuotaHeld(true);
            job.setAttempt(job.getAttempt() + 1);
            job.setState(JobState.RUNNING);
            lease = leaseManager.issue(job, executor.getId(), clock.millis(), properties.getLeaseTtlMs());
        }

        // 锁外异步执行（submit 仅入队，不阻塞），并记录公平性记账。
        executor.submit(job, lease);
        fairnessTracker.recordDispatch(job.getSubject());
        long seq = job.getScheduleSequence().incrementAndGet();
        log.info("[schedule-dispatch] jobId={} subject={} priority={} attempt={} executor={} leaseId={} fairSeq={} 判定依据=排序选中且配额充足,下发执行",
                job.getJobId(), job.getSubject(), job.getPriority(), job.getAttempt(),
                executor.getId(), lease.leaseId(), seq);
        return DispatchResult.COMMITTED;
    }

    /**
     * 抢占路径：为等待中的高优先级任务挤出一个低优先级运行任务的执行能力。
     *
     * 规则（确定性、可解释）：
     * 1) 仅当等待任务优先级严格高于运行任务时才允许抢占；
     * 2) 受害者排序：优先级低者优先 → 租约发放晚者（刚开始跑、被挤浪费最少）优先 → jobId 升序兜底；
     * 3) 防饿死：同一任务被抢占次数达到 scheduler.max-preemptions-per-job 后获得免疫，不再被选为受害者；
     * 4) 账目：受害者让出的配额先归还、再由抢占者重新入账（总量不多不少）；槽位优先从受害者所在
     *    执行者 1:1 接管，接管失败则常规预留；被抢占不退还失败重试预算以外的任何账目，
     *    且本次被挤下的尝试次数退还给受害者（被抢占 ≠ 执行失败）。
     * 每轮循环挤下一个受害者后重试下发；配额仍不足（被其他线程捷足先登）则继续找下一个受害者，
     * 直到下发成功或无受害者可抢。
     *
     * 锁序：抢占者锁（PENDING）→ 受害者锁（RUNNING）→ 主体配额锁。
     * "确认抢占者仍需要插队"与"挤下受害者"在同一把抢占者锁内完成，
     * 不会为已被其他线程处理掉的任务误伤受害者；PENDING 任务被持锁期间不可能成为
     * 别人的受害者（受害者必须 RUNNING），故多调度线程并发抢占无循环等待。
     */
    private boolean preemptAndDispatch(Job job) {
        int maxPreemptions = properties.getMaxPreemptionsPerJob();
        if (maxPreemptions <= 0) {
            return false;
        }
        while (true) {
            Job victim = selectVictim(job, maxPreemptions);
            if (victim == null) {
                log.info("[preempt-skip] jobId={} priority={} 判定依据=无优先级更低且未免疫的运行任务可抢,继续等待",
                        job.getJobId(), job.getPriority());
                return false;
            }

            synchronized (job.getLock()) {
                // 并发二次确认：抢占者已非 PENDING（被其他调度线程处理）则本轮放弃，
                // 且此时尚未做任何破坏性动作（未接管槽位、未挤下受害者）。
                if (job.getState() != JobState.PENDING) {
                    log.info("[preempt-skip] jobId={} state={} 判定依据=并发下已被其他调度线程处理,放弃抢占",
                            job.getJobId(), job.getState());
                    return false;
                }

                // 1) 槽位交接：优先从受害者所在执行者 1:1 接管其槽位；接管失败（受害者恰好自行结束）
                //    则退化为常规预留；两者都失败说明此刻确实没有能力，放弃本轮。
                TaskExecutor executor = executorOf(victim);
                boolean transferred = executor != null && executor.transferSlot(victim.getJobId());
                if (!transferred) {
                    executor = reserveAnExecutor();
                    if (executor == null) {
                        log.info("[preempt-skip] jobId={} victim={} 判定依据=槽位接管失败且无空闲槽位,本轮放弃抢占",
                                job.getJobId(), victim.getJobId());
                        return false;
                    }
                }

                // 2) 锁内挤下受害者：归还其配额、退还本次尝试、记抢占次数、回到 PENDING。
                preemptVictim(victim, job);

                // 3) 复用同一提交点为抢占者下发（commitDispatch 重入当前任务锁）。
                DispatchResult result = commitDispatch(job, executor);
                if (result == DispatchResult.COMMITTED) {
                    log.info("[preempt-dispatch] jobId={} priority={} victim={} transferred={} 判定依据=抢占成功,高优先级任务立即占用让出的能力",
                            job.getJobId(), job.getPriority(), victim.getJobId(), transferred);
                    return true;
                }
                executor.releaseSlot();
                if (result == DispatchResult.NOT_PENDING) {
                    return false;
                }
                // QUOTA_SHORT：让出的配额可能被其他调度线程捷足先登，继续寻找下一个受害者。
            }
        }
    }

    /**
     * 确定性选择受害者：在 RUNNING 且优先级严格更低、未达抢占免疫上限的任务中，
     * 按"优先级升序 → 租约发放时间降序 → jobId 升序"取第一个。
     * 排序键在任务锁内快照，保证单次选择完全确定、可复现。
     */
    private Job selectVictim(Job preemptor, int maxPreemptions) {
        record VictimKey(Job job, int priority, long issuedAt, String jobId) {}
        List<VictimKey> victims = new ArrayList<>();
        for (Job candidate : jobStore.all()) {
            synchronized (candidate.getLock()) {
                if (candidate.getState() == JobState.RUNNING
                        && candidate.getPriority() < preemptor.getPriority()
                        && candidate.getPreemptedCount() < maxPreemptions) {
                    Lease lease = candidate.getLease();
                    victims.add(new VictimKey(candidate, candidate.getPriority(),
                            lease == null ? Long.MIN_VALUE : lease.issuedAt(), candidate.getJobId()));
                }
            }
        }
        if (victims.isEmpty()) {
            return null;
        }
        victims.sort(Comparator.comparingInt(VictimKey::priority)
                .thenComparing(Comparator.comparingLong(VictimKey::issuedAt).reversed())
                .thenComparing(VictimKey::jobId));
        VictimKey chosen = victims.get(0);
        log.info("[preempt-select] preemptor={} preemptorPriority={} victim={} victimPriority={} victimIssuedAt={} candidates={} 判定依据=优先级最低→租约最新→jobId最小",
                preemptor.getJobId(), preemptor.getPriority(), chosen.jobId(), chosen.priority(), chosen.issuedAt(),
                victims.stream().map(v -> v.jobId() + "(p=" + v.priority() + ",issuedAt=" + v.issuedAt() + ")").toList());
        return chosen.job();
    }

    /**
     * 锁内挤下受害者（调用方不得持有受害者锁）。受害者已离开 RUNNING 时什么都不做
     * （并发下它已自行终结/迁移，其配额与槽位由对应路径结算）。
     */
    private void preemptVictim(Job victim, Job preemptor) {
        synchronized (victim.getLock()) {
            if (victim.getState() != JobState.RUNNING) {
                log.info("[preempt-victim-skip] victim={} state={} 判定依据=受害者已离开RUNNING,无需挤下",
                        victim.getJobId(), victim.getState());
                return;
            }
            Lease oldLease = victim.getLease();
            // 额度交接：受害者让出的配额只归还一次，稍后由抢占者重新入账，总量不多不少。
            leaseManager.releaseQuotaOnce(victim, "preempt");
            victim.setLease(null);
            // 被抢占 ≠ 执行失败：退还本次尝试，不消耗失败重试预算。
            victim.setAttempt(victim.getAttempt() - 1);
            victim.setPreemptedCount(victim.getPreemptedCount() + 1);
            victim.setState(JobState.PENDING);
            log.info("[preempt-victim] victim={} preemptor={} leaseId={} attempt={} preemptedCount={} quotaHeld={} 判定依据=被更高优先级任务抢占,退回等待队列,本次尝试已退还不消耗重试预算",
                    victim.getJobId(), preemptor.getJobId(), oldLease == null ? "-" : oldLease.leaseId(),
                    victim.getAttempt(), victim.getPreemptedCount(), victim.isQuotaHeld());
        }
    }

    /** 按租约找到受害者当前所在的执行者；租约已不存在或执行者已注销时返回 null。 */
    private TaskExecutor executorOf(Job job) {
        Lease lease = job.getLease();
        if (lease == null) {
            return null;
        }
        for (TaskExecutor executor : executors) {
            if (executor.getId().equals(lease.executorId())) {
                return executor;
            }
        }
        return null;
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
