package com.github.highcumontoa.jobschedulerquotajava.executor;

import com.github.highcumontoa.jobschedulerquotajava.model.Job;
import com.github.highcumontoa.jobschedulerquotajava.model.JobResult;
import com.github.highcumontoa.jobschedulerquotajava.model.Lease;
import com.github.highcumontoa.jobschedulerquotajava.service.JobService;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 基于本机线程池的执行者：
 * <ul>
 *   <li>payload 以 "fail" 开头 或 equals("error") → 回传失败（触发重试 / 上限终结）；</li>
 *   <li>payload 形如 "sleep:1500" → 先 sleep 1500ms 再回传，可用于制造租约超时窗口；</li>
 *   <li>payload = "never" → 长时间不回传（模拟执行者卡住/失联，等待租约回收）；</li>
 *   <li>其余 payload → 成功。</li>
 * </ul>
 * 执行槽位用原子计数管理：调度器先 reserve 再 submit，结束后一定 release，避免超发。
 */
@Component
public class LocalTaskExecutor implements TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(LocalTaskExecutor.class);

    private final String id = "exec-" + UUID.randomUUID().toString().substring(0, 8);
    private final AtomicInteger inFlight = new AtomicInteger(0);

    @Value("${scheduler.local-executor.parallelism:4}")
    private int parallelism;

    private final ExecutorService pool;
    private final ObjectProvider<JobService> jobServiceProvider;

    public LocalTaskExecutor(ObjectProvider<JobService> jobServiceProvider) {
        this.jobServiceProvider = jobServiceProvider;
        this.pool = Executors.newCachedThreadPool(new ThreadFactory() {
            private final AtomicInteger n = new AtomicInteger();
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, id + "-worker-" + n.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        });
    }

    @Override
    public String getId() {
        return id;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = Math.max(1, parallelism);
    }

    public int getParallelism() {
        return parallelism;
    }

    @Override
    public boolean hasCapacity() {
        return inFlight.get() < parallelism;
    }

    @Override
    public boolean reserveSlot() {
        while (true) {
            int cur = inFlight.get();
            if (cur >= parallelism) {
                return false;
            }
            if (inFlight.compareAndSet(cur, cur + 1)) {
                return true;
            }
        }
    }

    @Override
    public void releaseSlot() {
        inFlight.decrementAndGet();
    }

    @Override
    public void submit(Job job, Lease lease) {
        pool.submit(() -> run(job, lease));
    }

    private void run(Job job, Lease lease) {
        String payload = job.getPayload() == null ? "" : job.getPayload();
        log.info("[exec-start] executor={} jobId={} leaseId={} attempt={} payload={} inFlight={}",
                id, job.getJobId(), lease.leaseId(), lease.attempt(), payload, inFlight.get());
        JobResult result;
        try {
            Long sleepMs = parseSleep(payload);
            if (sleepMs != null) {
                Thread.sleep(sleepMs);
            } else if ("never".equals(payload)) {
                // 模拟执行者失联：持有槽位但永不回传，由租约超时/显式回收接管。
                log.info("[exec-hang] executor={} jobId={} leaseId={} payload=never,执行者不回传,等待租约回收",
                        id, job.getJobId(), lease.leaseId());
                while (!Thread.currentThread().isInterrupted()) {
                    Thread.sleep(60_000);
                }
                return;
            }
            boolean fail;
            Integer failFirstN = parseFailFirst(payload);
            if (failFirstN != null) {
                // "failfirst:N"：第 1..N 次尝试失败，之后成功，用于验证重试恢复路径。
                fail = lease.attempt() <= failFirstN;
            } else {
                fail = payload.startsWith("fail") || payload.equals("error");
            }
            result = new JobResult(job.getJobId(), lease.leaseId(), id, !fail,
                    fail ? ("模拟执行失败(第" + lease.attempt() + "次尝试)") : ("模拟第" + lease.attempt() + "次尝试成功"));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("[exec-interrupted] executor={} jobId={} leaseId={} 执行线程随执行者关闭被中断",
                    id, job.getJobId(), lease.leaseId());
            return;
        } catch (RuntimeException e) {
            result = new JobResult(job.getJobId(), lease.leaseId(), id, false, "执行者异常: " + e);
        }
        try {
            jobServiceProvider.getObject().report(result);
            log.info("[exec-report] executor={} jobId={} leaseId={} success={} 回传已被接受",
                    id, job.getJobId(), lease.leaseId(), result.success());
        } catch (RuntimeException ex) {
            // 回传被拒（租约已过期/已取消/已终结）：结论以域服务为准，执行者只记录，不改变任何状态。
            log.info("[exec-report-rejected] executor={} jobId={} leaseId={} 判定依据={}",
                    id, job.getJobId(), lease.leaseId(), ex.getMessage());
        } finally {
            releaseSlot();
            log.info("[exec-finish] executor={} jobId={} leaseId={} inFlight={}",
                    id, job.getJobId(), lease.leaseId(), inFlight.get());
        }
    }

    /** 解析 "sleep:毫秒数"；不匹配返回 null。 */
    static Long parseSleep(String payload) {
        if (payload.startsWith("sleep:")) {
            try {
                long ms = Long.parseLong(payload.substring("sleep:".length()).trim());
                return Math.max(0, ms);
            } catch (NumberFormatException ignore) {
                return null;
            }
        }
        return null;
    }

    /** 解析 "failfirst:N"：前 N 次尝试失败；不匹配返回 null。 */
    static Integer parseFailFirst(String payload) {
        if (payload.startsWith("failfirst:")) {
            try {
                return Integer.parseInt(payload.substring("failfirst:".length()).trim());
            } catch (NumberFormatException ignore) {
                return null;
            }
        }
        return null;
    }

    @PreDestroy
    public void shutdown() {
        pool.shutdownNow();
        try {
            pool.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
