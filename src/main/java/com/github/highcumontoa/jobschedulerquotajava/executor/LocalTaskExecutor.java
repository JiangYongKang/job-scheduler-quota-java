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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

    /**
     * 在途任务注册表：jobId → 工作句柄。
     * 每个句柄的 slotAccounting 是槽位账目的唯一裁决点：
     * CAS(false→true) 的胜者负责该槽位的最终去向——工作线程胜则自行 releaseSlot，
     * 调度器（transferSlot）胜则槽位被接管、工作线程不得再归还。
     */
    private final ConcurrentHashMap<String, WorkerHandle> workers = new ConcurrentHashMap<>();

    private static final class WorkerHandle {
        volatile Thread thread;
        final AtomicBoolean slotAccounting = new AtomicBoolean(false);
    }

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
        WorkerHandle handle = new WorkerHandle();
        workers.put(job.getJobId(), handle);
        pool.submit(() -> run(job, lease, handle));
    }

    @Override
    public boolean transferSlot(String jobId) {
        WorkerHandle h = workers.get(jobId);
        if (h == null) {
            log.info("[slot-transfer-skip] executor={} jobId={} 判定依据=任务不在本执行者在途表,槽位已被正常归还", id, jobId);
            return false;
        }
        if (!h.slotAccounting.compareAndSet(false, true)) {
            log.info("[slot-transfer-skip] executor={} jobId={} 判定依据=槽位账目已被工作线程结算", id, jobId);
            return false;
        }
        Thread t = h.thread;
        if (t != null) {
            t.interrupt();
        }
        log.info("[slot-transfer] executor={} jobId={} 判定依据=抢占接管槽位,工作线程被中断且不再归还该槽位", id, jobId);
        return true;
    }

    private void run(Job job, Lease lease, WorkerHandle handle) {
        handle.thread = Thread.currentThread();
        try {
            if (handle.slotAccounting.get()) {
                // 槽位在执行前已被抢占者接管：不执行、不回传、不触碰槽位计数。
                log.info("[exec-skipped] executor={} jobId={} 判定依据=槽位已被抢占接管,本次下发作废", id, job.getJobId());
                return;
            }
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
                log.info("[exec-interrupted] executor={} jobId={} leaseId={} 判定依据=执行线程被中断(抢占或关闭),不回传结果",
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
                // 回传被拒（租约已过期/已取消/已终结/已被抢占）：结论以域服务为准，执行者只记录，不改变任何状态。
                log.info("[exec-report-rejected] executor={} jobId={} leaseId={} 判定依据={}",
                        id, job.getJobId(), lease.leaseId(), ex.getMessage());
            }
        } finally {
            workers.remove(job.getJobId(), handle);
            // 槽位账目唯一裁决：只有 CAS 胜者才归还槽位；被 transferSlot 接管时此处为败者，不得归还。
            if (handle.slotAccounting.compareAndSet(false, true)) {
                releaseSlot();
            }
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
