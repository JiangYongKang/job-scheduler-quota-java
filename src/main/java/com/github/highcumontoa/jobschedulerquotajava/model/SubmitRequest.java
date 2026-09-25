package com.github.highcumontoa.jobschedulerquotajava.model;

/**
 * 任务提交请求。
 *
 * @param jobId    调用方指定的任务标识（幂等键）
 * @param subject  提交主体（配额归属）
 * @param demand   任务需要占用的资源量
 * @param priority 调度优先级，数值越大优先级越高
 * @param payload  本地执行者使用的任务载荷（如 "success" / "fail"）
 */
public record SubmitRequest(String jobId,
                            String subject,
                            ResourceQuota demand,
                            int priority,
                            String payload) {
}
