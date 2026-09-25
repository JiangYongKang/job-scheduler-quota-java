package com.github.highcumontoa.jobschedulerquotajava.executor;

import com.github.highcumontoa.jobschedulerquotajava.core.JobSchedulerService;
import com.github.highcumontoa.jobschedulerquotajava.core.SchedulerProperties;
import com.github.highcumontoa.jobschedulerquotajava.exception.SchedulerException;
import com.github.highcumontoa.jobschedulerquotajava.model.DispatchResult;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地执行者，模拟"调度器下发 -> 执行者执行 -> 结果回传"全过程，不依赖任何外部服务。
 *
 * <p>每个调度周期最多领取一个任务；领取后在独立工作线程中执行，固定延迟后回传结果。
 * 回传时携带下发时拿到的 leaseId：若期间租约被回收或任务被取消，
 * {@code complete} 会以"过期租约"拒绝，旧执行者的迟到结论不会覆盖权威终态。</p>
 */
@Component
public class LocalExecutor {

    private static final Logger log = LoggerFactory.getLogger(LocalExecutor.class);

    private final JobSchedulerService scheduler;
    private final SchedulerProperties properties;
    private final ExecutorService workers = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "local-exec-worker");
        t.setDaemon(true);
        return t;
    });
    private final AtomicInteger inFlight = new AtomicInteger();

    public LocalExecutor(JobSchedulerService scheduler, SchedulerProperties properties) {
        this.scheduler = scheduler;
        this.properties = properties;
    }

    /**
     * 周期性 tick。{@code scheduler.executor-enabled=false}（测试配置）时整体停摆，
     * 保证单元测试完全由测试线程驱动，不被后台调度干扰。
     */
    @Scheduled(fixedDelayString = "${scheduler.executor-interval-millis:200}")
    public void tick() {
        if (!properties.isExecutorEnabled()) {
            return;
        }
        DispatchResult result;
        try {
            result = scheduler.dispatchOne();
        } catch (RuntimeException e) {
            log.warn("挑选下发异常: {}", e.getMessage());
            return;
        }
        if (!result.dispatched()) {
            return;
        }
        String jobId = result.jobId();
        String leaseId = result.leaseId();
        inFlight.incrementAndGet();
        workers.submit(() -> runJob(jobId, leaseId));
    }

    private void runJob(String jobId, String leaseId) {
        try {
            Thread.sleep(properties.getExecuteDelayMillis());
            // 模拟执行成功（真实场景由任务退出码决定）；失败路径与租约超时由测试直接驱动服务。
            scheduler.complete(jobId, leaseId, true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (SchedulerException e) {
            // 预期分支：租约已超时回收或任务已取消，迟到回传被权威状态机拒绝。
            log.info("执行者回传被拒绝，以调度器状态为准: jobId={}, leaseId={}, 原因={}",
                    jobId, leaseId, e.getMessage());
        } catch (RuntimeException e) {
            log.warn("执行者回传异常: jobId={}, {}", jobId, e.getMessage());
        } finally {
            inFlight.decrementAndGet();
        }
    }

    public int getInFlight() {
        return inFlight.get();
    }

    @PreDestroy
    public void shutdown() {
        workers.shutdownNow();
    }
}
