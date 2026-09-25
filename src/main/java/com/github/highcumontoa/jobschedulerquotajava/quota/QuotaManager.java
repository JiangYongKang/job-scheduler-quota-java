package com.github.highcumontoa.jobschedulerquotajava.quota;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按提交主体维护的资源配额账本。
 * <p>
 * 配额只统计"占用中"资源，即已下发、正在执行（RUNNING）的任务。
 * PENDING 任务不占配额，因此配额不足时只能继续等待，后续下发时再尝试获取。
 * <p>
 * 并发安全说明：本类使用 ConcurrentHashMap 保证 map 结构安全，
 * 但"检查余额-扣减"的原子性由 {@code JobSchedulerService} 的全局锁保证
 * （与任务状态挑选在同一临界区内完成，杜绝"配额够却没被选中"之外的越权下发）。
 */
public class QuotaManager {

    private static final Logger log = LoggerFactory.getLogger(QuotaManager.class);

    /** 每个提交主体的配额上限；缺省主体使用 defaultLimit。 */
    private final int defaultLimit;
    private final Map<String, Integer> limits = new ConcurrentHashMap<>();
    /** 每个提交主体当前占用量，恒有 0 <= used <= limit。 */
    private final Map<String, Integer> used = new ConcurrentHashMap<>();

    public QuotaManager(int defaultLimit) {
        if (defaultLimit <= 0) {
            throw new IllegalArgumentException("defaultLimit 必须为正数: " + defaultLimit);
        }
        this.defaultLimit = defaultLimit;
    }

    /** 覆盖某提交主体的配额上限（演示/配置用）。 */
    public void setLimit(String submitter, int limit) {
        if (limit < 0) {
            throw new IllegalArgumentException("limit 不能为负: " + limit);
        }
        limits.put(submitter, limit);
        log.info("配额上限更新: submitter={}, limit={}", submitter, limit);
    }

    public int limitOf(String submitter) {
        return limits.getOrDefault(submitter, defaultLimit);
    }

    public int usedOf(String submitter) {
        return used.getOrDefault(submitter, 0);
    }

    public int availableOf(String submitter) {
        return limitOf(submitter) - usedOf(submitter);
    }

    /**
     * 尝试占用配额。余额不足返回 false 且不改变任何占用（准入拒绝语义）。
     */
    public boolean tryAcquire(String submitter, int units) {
        if (units <= 0) {
            throw new IllegalArgumentException("占用单元数必须为正数: " + units);
        }
        int current = usedOf(submitter);
        if (current + units > limitOf(submitter)) {
            return false;
        }
        used.put(submitter, current + units);
        return true;
    }

    /**
     * 归还配额。为防止终态路径重复归还导致占用变负，本方法做下限保护并告警；
     * 正常流程下归还次数严格等于获取次数（由任务机的终态一次性转换保证）。
     */
    public void release(String submitter, int units) {
        if (units <= 0) {
            throw new IllegalArgumentException("归还单元数必须为正数: " + units);
        }
        int current = usedOf(submitter);
        int next = current - units;
        if (next < 0) {
            log.error("检测到重复/超额归还，已拦截: submitter={}, currentUsed={}, releaseUnits={}",
                    submitter, current, units);
            throw new IllegalStateException("配额归还后为负，疑似重复归还: submitter=" + submitter
                    + ", used=" + current + ", release=" + units);
        }
        used.put(submitter, next);
    }
}
