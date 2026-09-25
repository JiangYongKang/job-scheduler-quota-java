package com.github.highcumontoa.jobschedulerquotajava.scheduler;

import com.github.highcumontoa.jobschedulerquotajava.config.SchedulerProperties;
import com.github.highcumontoa.jobschedulerquotajava.lease.LeaseManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Clock;

/**
 * 可选后台驱动：周期执行"租约回收 → 调度一轮"。
 * scheduler.tick-interval-ms &lt;= 0 时不启动（默认关闭，测试通过手动 tick 保证可复现）。
 */
@Component
public class SchedulerDaemon {

    private static final Logger log = LoggerFactory.getLogger(SchedulerDaemon.class);

    private final Scheduler scheduler;
    private final LeaseManager leaseManager;
    private final SchedulerProperties properties;
    private final Clock clock;

    private volatile boolean running;
    private Thread worker;

    public SchedulerDaemon(Scheduler scheduler, LeaseManager leaseManager,
                           SchedulerProperties properties, Clock clock) {
        this.scheduler = scheduler;
        this.leaseManager = leaseManager;
        this.properties = properties;
        this.clock = clock;
    }

    @PostConstruct
    public void start() {
        long interval = properties.getTickIntervalMs();
        if (interval <= 0) {
            log.info("[daemon] scheduler.tick-interval-ms={} 后台调度关闭,使用 /api/admin/tick 或测试手动驱动", interval);
            return;
        }
        running = true;
        worker = new Thread(() -> {
            while (running) {
                try {
                    leaseManager.reclaimExpired(clock.millis());
                    scheduler.tick();
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (RuntimeException e) {
                    log.warn("[daemon] 调度轮次异常(已忽略,下轮继续): {}", e.toString());
                }
            }
        }, "scheduler-daemon");
        worker.setDaemon(true);
        worker.start();
        log.info("[daemon] 后台调度已启动 intervalMs={}", interval);
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (worker != null) {
            worker.interrupt();
        }
    }
}
