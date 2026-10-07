package com.github.highcumontoa.jobschedulerquotajava.quota;

import com.github.highcumontoa.jobschedulerquotajava.model.Job;
import com.github.highcumontoa.jobschedulerquotajava.model.RejectReason;
import com.github.highcumontoa.jobschedulerquotajava.model.ResourceQuota;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 配额账本：
 * <ul>
 *   <li>提交时按主体校验"当前占用 + 本次需求"是否越过硬上限（可区分 QUOTA_EXCEEDED_ON_SUBMIT），
 *       被拒绝的请求不会改动任何占用。</li>
 *   <li>真正发放（占用）发生在任务被下发（进入 RUNNING）时；{@code tryAcquire} 在主体锁内
 *       完成"检查 + 记账"，因此并发调度不会突破配额，不足时任务只能继续等待。</li>
 *   <li>任务终结时在任务锁内一次性归还；{@code quotaHeld} 标记保证同一任务不会重复归还。</li>
 * </ul>
 * 锁序固定为：任务锁 {@link Job#getLock()} → 主体配额锁，全工程无反向获取，避免死锁。
 */
@Component
public class QuotaService {

    private static final Logger log = LoggerFactory.getLogger(QuotaService.class);

    private final Map<String, ResourceQuota> limits = new ConcurrentHashMap<>();
    private final Map<String, ResourceQuota> used = new ConcurrentHashMap<>();
    /** 每个主体一把锁，保证"检查 + 记账"原子。 */
    private final Map<String, Object> subjectLocks = new ConcurrentHashMap<>();

    /** 未为某主体显式配置上限时的默认上限；null 表示不限。 */
    private volatile ResourceQuota defaultLimit = null;

    public void setLimit(String subject, ResourceQuota limit) {
        if (limit == null || limit.hasNegative()) {
            throw new IllegalArgumentException("配额上限非法: " + limit);
        }
        limits.put(subject, limit);
    }

    public Optional<ResourceQuota> findLimit(String subject) {
        return Optional.ofNullable(limits.getOrDefault(subject, defaultLimit));
    }

    public void setDefaultLimit(ResourceQuota defaultLimit) {
        this.defaultLimit = defaultLimit;
    }

    private Object lockOf(String subject) {
        return subjectLocks.computeIfAbsent(subject, k -> new Object());
    }

    private boolean exceeds(ResourceQuota current, ResourceQuota demand, ResourceQuota limit) {
        return current.cpu() + demand.cpu() > limit.cpu()
                || current.memoryMb() + demand.memoryMb() > limit.memoryMb()
                || current.instances() + demand.instances() > limit.instances();
    }

    /**
     * 提交准入：当前占用 + demand 是否在限内。
     * 注意：PENDING 任务不占配额，真正的硬隔离在 {@link #tryAcquire}。
     */
    public boolean canAdmit(String subject, ResourceQuota demand) {
        synchronized (lockOf(subject)) {
            ResourceQuota limit = limits.getOrDefault(subject, defaultLimit);
            if (limit == null) {
                return true;
            }
            boolean ok = !exceeds(used.getOrDefault(subject, ResourceQuota.ZERO), demand, limit);
            log.info("[quota-admit] subject={} demand={} used={} limit={} decision={}",
                    subject, demand, used.getOrDefault(subject, ResourceQuota.ZERO), limit, ok ? "ACCEPT" : "REJECT:" + RejectReason.QUOTA_EXCEEDED_ON_SUBMIT);
            return ok;
        }
    }

    /**
     * 下发时原子地"检查 + 发放"。成功返回 true；任一维度不足返回 false（调用方让任务继续等待）。
     * 调用方必须持有 job 自身锁，且 job.quotaHeld == false。
     */
    public boolean tryAcquire(Job job) {
        String subject = job.getSubject();
        ResourceQuota demand = job.getDemand();
        synchronized (lockOf(subject)) {
            ResourceQuota current = used.getOrDefault(subject, ResourceQuota.ZERO);
            ResourceQuota limit = limits.getOrDefault(subject, defaultLimit);
            if (limit != null && exceeds(current, demand, limit)) {
                log.info("[quota-acquire] jobId={} subject={} demand={} used={} limit={} decision=WAIT(配额不足,继续等待)",
                        job.getJobId(), subject, demand, current, limit);
                return false;
            }
            ResourceQuota next = current.plus(demand);
            used.put(subject, next);
            log.info("[quota-acquire] jobId={} subject={} demand={} usedBefore={} usedAfter={} limit={} decision=ACQUIRE",
                    job.getJobId(), subject, demand, current, next, limit);
            return true;
        }
    }

    /**
     * 归还占用。调用方已在任务锁内把 quotaHeld 置为 false，故同一任务只会计入一次扣减。
     */
    public void release(Job job) {
        String subject = job.getSubject();
        ResourceQuota demand = job.getDemand();
        synchronized (lockOf(subject)) {
            ResourceQuota current = used.getOrDefault(subject, ResourceQuota.ZERO);
            ResourceQuota next = current.minus(demand);
            // 记账不变量保护：任何维度都不应为负（若为负说明出现重复归还，属于必须暴露的 bug）。
            if (next.hasNegative()) {
                throw new IllegalStateException("配额占用扣减为负，疑似重复归还: jobId=" + job.getJobId()
                        + " subject=" + subject + " used=" + current + " release=" + demand);
            }
            used.put(subject, next);
            log.info("[quota-release] jobId={} subject={} release={} usedBefore={} usedAfter={} decision=RELEASED",
                    job.getJobId(), subject, demand, current, next);
        }
    }

    /**
     * 静默预判：当前占用 + demand 是否在限内（不记日志、不改账）。
     * 供调度器判断"任务此刻是否被配额卡住"，真正的硬隔离仍是 {@link #tryAcquire}。
     */
    public boolean wouldFit(String subject, ResourceQuota demand) {
        synchronized (lockOf(subject)) {
            ResourceQuota limit = limits.getOrDefault(subject, defaultLimit);
            if (limit == null) {
                return true;
            }
            return !exceeds(used.getOrDefault(subject, ResourceQuota.ZERO), demand, limit);
        }
    }

    /**
     * 静默预判：若 victim 的占用被释放，preemptor 是否放得下。
     * 只有同主体时 victim 的释放才惠及 preemptor；不同主体只看当前余量。
     */
    public boolean wouldFitAfterRelease(Job preemptor, Job victim) {
        String subject = preemptor.getSubject();
        synchronized (lockOf(subject)) {
            ResourceQuota limit = limits.getOrDefault(subject, defaultLimit);
            if (limit == null) {
                return true;
            }
            ResourceQuota current = used.getOrDefault(subject, ResourceQuota.ZERO);
            ResourceQuota freed = subject.equals(victim.getSubject()) ? victim.getDemand() : ResourceQuota.ZERO;
            ResourceQuota after = current.minus(freed);
            return !exceeds(after, preemptor.getDemand(), limit);
        }
    }

    public ResourceQuota getUsed(String subject) {
        synchronized (lockOf(subject)) {
            return used.getOrDefault(subject, ResourceQuota.ZERO);
        }
    }

    public Map<String, ResourceQuota> snapshotUsed() {
        Map<String, ResourceQuota> snap = new HashMap<>();
        used.forEach((subject, q) -> {
            synchronized (lockOf(subject)) {
                snap.put(subject, q);
            }
        });
        return snap;
    }
}
