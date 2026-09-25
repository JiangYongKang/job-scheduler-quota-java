package com.github.highcumontoa.jobschedulerquotajava.model;

/**
 * 执行者回传的执行结果。
 */
public record JobResult(String jobId,
                        String leaseId,
                        String executorId,
                        boolean success,
                        String detail) {
}
