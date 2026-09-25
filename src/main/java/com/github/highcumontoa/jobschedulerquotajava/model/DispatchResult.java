package com.github.highcumontoa.jobschedulerquotajava.model;

/**
 * 单次挑选下发的结论（骨架占位）。
 */
public record DispatchResult(boolean dispatched, String jobId, String leaseId, String reason) {
}
