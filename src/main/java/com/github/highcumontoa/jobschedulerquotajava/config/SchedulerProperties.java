package com.github.highcumontoa.jobschedulerquotajava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 调度器可调参数（application.properties 中以 scheduler.* 覆盖）。
 */
@ConfigurationProperties(prefix = "scheduler")
public class SchedulerProperties {

    /** 租约有效期（毫秒）。 */
    private long leaseTtlMs = 3000;

    /** 单次任务最大尝试次数（1 表示不重试）。 */
    private int maxAttempts = 3;

    /** 后台调度扫描间隔（毫秒），<=0 表示关闭后台线程（测试中手动驱动）。 */
    private long tickIntervalMs = 0;

    /**
     * 同一任务最多允许被抢占的次数；达到该次数后获得"抢占免疫"，不再被选为被抢对象
     * （防饿死）。0 表示完全关闭抢占能力。
     */
    private int preemptionMaxPerJob = 3;

    public long getLeaseTtlMs() { return leaseTtlMs; }
    public void setLeaseTtlMs(long leaseTtlMs) { this.leaseTtlMs = leaseTtlMs; }

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }

    public long getTickIntervalMs() { return tickIntervalMs; }
    public void setTickIntervalMs(long tickIntervalMs) { this.tickIntervalMs = tickIntervalMs; }

    public int getPreemptionMaxPerJob() { return preemptionMaxPerJob; }
    public void setPreemptionMaxPerJob(int preemptionMaxPerJob) { this.preemptionMaxPerJob = preemptionMaxPerJob; }
}
