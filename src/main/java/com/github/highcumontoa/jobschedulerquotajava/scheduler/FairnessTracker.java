package com.github.highcumontoa.jobschedulerquotajava.scheduler;

import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 公平性记账：记录每个主体历史被下发的次数。
 * 调度排序在"优先级"之后比较该计数：计数更少的主体先被服务，
 * 从而避免同优先级下某一主体长期霸占执行者；计数相同再按提交时间先到先得。
 */
@Component
public class FairnessTracker {

    private final Map<String, AtomicLong> dispatchCountBySubject = new ConcurrentHashMap<>();
    private final AtomicLong globalSequence = new AtomicLong(0);

    public long dispatchCount(String subject) {
        AtomicLong c = dispatchCountBySubject.get(subject);
        return c == null ? 0L : c.get();
    }

    /** 一次成功下发后记账。 */
    public void recordDispatch(String subject) {
        dispatchCountBySubject.computeIfAbsent(subject, k -> new AtomicLong()).incrementAndGet();
    }

    public long nextSequence() {
        return globalSequence.incrementAndGet();
    }

    /**
     * 可解释的调度排序：
     * 1) 优先级高者优先（priority 降序）；
     * 2) 同优先级，主体历史下发次数少者优先（公平）；
     * 3) 仍相同，提交时间早者优先（FIFO）；
     * 4) 最后以 jobId 兜底，保证排序完全确定、可复现。
     */
    public Comparator<Map.Entry<com.github.highcumontoa.jobschedulerquotajava.model.Job, Long>> comparator() {
        return Comparator
                .comparingLong((Map.Entry<com.github.highcumontoa.jobschedulerquotajava.model.Job, Long> e)
                        -> -e.getKey().getPriority())
                .thenComparingLong(Map.Entry::getValue)
                .thenComparingLong(e -> e.getKey().getCreatedAt())
                .thenComparing(e -> e.getKey().getJobId());
    }
}
