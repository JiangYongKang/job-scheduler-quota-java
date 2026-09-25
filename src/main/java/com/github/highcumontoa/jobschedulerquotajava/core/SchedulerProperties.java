package com.github.highcumontoa.jobschedulerquotajava.core;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 调度器可配置参数（骨架占位）。
 */
@ConfigurationProperties(prefix = "scheduler")
public class SchedulerProperties {
    private int defaultQuota = 0;
    private int maxAttempts = 0;
    private long leaseTtlMillis = 0;
    private long reaperIntervalMillis = 0;
    private boolean executorEnabled = false;
    private long executeDelayMillis = 0;

    public int getDefaultQuota() {
        return defaultQuota;
    }

    public void setDefaultQuota(int defaultQuota) {
        this.defaultQuota = defaultQuota;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public long getLeaseTtlMillis() {
        return leaseTtlMillis;
    }

    public void setLeaseTtlMillis(long leaseTtlMillis) {
        this.leaseTtlMillis = leaseTtlMillis;
    }

    public long getReaperIntervalMillis() {
        return reaperIntervalMillis;
    }

    public void setReaperIntervalMillis(long reaperIntervalMillis) {
        this.reaperIntervalMillis = reaperIntervalMillis;
    }

    public boolean isExecutorEnabled() {
        return executorEnabled;
    }

    public void setExecutorEnabled(boolean executorEnabled) {
        this.executorEnabled = executorEnabled;
    }

    public long getExecuteDelayMillis() {
        return executeDelayMillis;
    }

    public void setExecuteDelayMillis(long executeDelayMillis) {
        this.executeDelayMillis = executeDelayMillis;
    }
}
