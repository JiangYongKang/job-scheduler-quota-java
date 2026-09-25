package com.github.highcumontoa.jobschedulerquotajava.store;

import com.github.highcumontoa.jobschedulerquotajava.model.Job;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 任务内存存储（本机可复现，无外部依赖）。
 * 仅提供按 id 存取与全集快照，状态机的一致性由 {@link Job#getLock()} 保证。
 */
@Component
public class JobStore {

    private final Map<String, Job> jobs = new ConcurrentHashMap<>();

    /** 不存在则放入并返回 null；已存在则返回既有任务（不覆盖）。 */
    public Job putIfAbsent(Job job) {
        Job existing = jobs.putIfAbsent(job.getJobId(), job);
        return existing;
    }

    public Optional<Job> find(String jobId) {
        return Optional.ofNullable(jobs.get(jobId));
    }

    public Job getRequired(String jobId) {
        return find(jobId).orElseThrow(() ->
                new com.github.highcumontoa.jobschedulerquotajava.exception.JobRejectedException(
                        com.github.highcumontoa.jobschedulerquotajava.model.RejectReason.JOB_NOT_FOUND,
                        "任务不存在: jobId=" + jobId));
    }

    public Collection<Job> all() {
        return jobs.values();
    }
}
