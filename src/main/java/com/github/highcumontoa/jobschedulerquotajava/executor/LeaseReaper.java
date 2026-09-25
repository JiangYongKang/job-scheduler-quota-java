package com.github.highcumontoa.jobschedulerquotajava.executor;

import com.github.highcumontoa.jobschedulerquotajava.core.JobSchedulerService;
import com.github.highcumontoa.jobschedulerquotajava.core.SchedulerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 租约回收器：周期性扫描超时 RUNNING 任务，模拟执行者失联后的租约回收。
 *
 * <p>回收与"结果回传/取消"并发发生时，三者都经过 {@code JobSchedulerService} 的同一把锁：
 * 先到者使旧 leaseId 失效并完成状态迁移，后到者面对的要么不是 RUNNING、
 * 要么 leaseId 不匹配，结论必然唯一。</p>
 */
@Component
public class LeaseReaper {

    private static final Logger log = LoggerFactory.getLogger(LeaseReaper.class);

    private final JobSchedulerService scheduler;
    private final SchedulerProperties properties;

    public LeaseReaper(JobSchedulerService scheduler, SchedulerProperties properties) {
        this.scheduler = scheduler;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${scheduler.reaper-interval-millis:500}")
    public void tick() {
        // 测试配置关闭后台执行者与回收器，回收由测试显式调用 reclaimExpired 驱动。
        if (!properties.isExecutorEnabled()) {
            return;
        }
        int reclaimed = scheduler.reclaimExpired();
        if (reclaimed > 0) {
            log.info("租约回收周期完成: 回收任务数={}", reclaimed);
        }
    }
}
