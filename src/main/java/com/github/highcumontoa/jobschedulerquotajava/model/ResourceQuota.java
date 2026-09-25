package com.github.highcumontoa.jobschedulerquotajava.model;

/**
 * 一份不可变的资源配额描述（cpu 核数 / 内存 MB / 任务实例数）。
 */
public record ResourceQuota(int cpu, int memoryMb, int instances) {

    public static final ResourceQuota ZERO = new ResourceQuota(0, 0, 0);

    /** 是否任一维度为负数，用于入参校验。 */
    public boolean hasNegative() {
        return cpu < 0 || memoryMb < 0 || instances < 0;
    }

    /** 各维度逐项相加。 */
    public ResourceQuota plus(ResourceQuota other) {
        return new ResourceQuota(cpu + other.cpu, memoryMb + other.memoryMb, instances + other.instances);
    }

    /** 各维度逐项相减（调用方需保证不产生负数，这里不做强制）。 */
    public ResourceQuota minus(ResourceQuota other) {
        return new ResourceQuota(cpu - other.cpu, memoryMb - other.memoryMb, instances - other.instances);
    }
}
