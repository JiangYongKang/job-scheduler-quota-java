package com.github.highcumontoa.jobschedulerquotajava.tests.support;

import com.github.highcumontoa.jobschedulerquotajava.config.SchedulerProperties;
import com.github.highcumontoa.jobschedulerquotajava.executor.LocalTaskExecutor;
import com.github.highcumontoa.jobschedulerquotajava.lease.LeaseManager;
import com.github.highcumontoa.jobschedulerquotajava.model.ResourceQuota;
import com.github.highcumontoa.jobschedulerquotajava.quota.QuotaService;
import com.github.highcumontoa.jobschedulerquotajava.scheduler.FairnessTracker;
import com.github.highcumontoa.jobschedulerquotajava.scheduler.Scheduler;
import com.github.highcumontoa.jobschedulerquotajava.service.JobService;
import com.github.highcumontoa.jobschedulerquotajava.store.JobStore;
import org.springframework.beans.factory.ObjectProvider;

/**
 * 测试装配：手工组装全部域组件，使用可控时钟，避免后台线程干扰，保证结论可复现。
 */
public class TestHarness implements AutoCloseable {

    public final ManualClock clock;
    public final SchedulerProperties props;
    public final JobStore jobStore;
    public final QuotaService quotaService;
    public final LeaseManager leaseManager;
    public final FairnessTracker fairnessTracker;
    public final Scheduler scheduler;
    public final JobService jobService;
    public final LocalTaskExecutor executor;

    public TestHarness(long leaseTtlMs, int maxAttempts) {
        this.clock = new ManualClock(1_000_000L);
        this.props = new SchedulerProperties();
        this.props.setLeaseTtlMs(leaseTtlMs);
        this.props.setMaxAttempts(maxAttempts);
        this.props.setTickIntervalMs(0);

        this.jobStore = new JobStore();
        this.quotaService = new QuotaService();
        this.leaseManager = new LeaseManager(jobStore, quotaService, props);
        this.fairnessTracker = new FairnessTracker();
        this.scheduler = new Scheduler(jobStore, quotaService, leaseManager, fairnessTracker, props, clock);
        this.jobService = new JobService(jobStore, quotaService, leaseManager, props, clock);

        // LocalTaskExecutor 依赖 JobService，用 ObjectProvider 直供。
        ObjectProvider<JobService> provider = new ObjectProvider<>() {
            @Override
            public JobService getObject() { return jobService; }
            @Override
            public JobService getIfAvailable() { return jobService; }
            @Override
            public JobService getIfUnique() { return jobService; }
            @Override
            public java.util.Iterator<JobService> iterator() {
                return java.util.List.of(jobService).iterator();
            }
        };
        this.executor = new LocalTaskExecutor(provider);
        this.executor.setParallelism(8);
        scheduler.registerExecutor(executor);
    }

    public void limit(String subject, int cpu, int mem, int instances) {
        quotaService.setLimit(subject, new ResourceQuota(cpu, mem, instances));
    }

    /** 等待所有执行槽位空闲（本地异步执行结束）。 */
    public void awaitIdle(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (executorInflight() == 0) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("执行者未在超时内空闲");
    }

    private int executorInflight() {
        try {
            var f = LocalTaskExecutor.class.getDeclaredField("inFlight");
            f.setAccessible(true);
            var ai = (java.util.concurrent.atomic.AtomicInteger) f.get(executor);
            return ai.get();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}
