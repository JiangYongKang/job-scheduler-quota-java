package com.github.highcumontoa.jobschedulerquotajava.store;

import com.github.highcumontoa.jobschedulerquotajava.model.Job;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 任务的内存存储。
 * <p>
 * 仅负责按 jobId 的存取与全量快照；任务的挑选、状态流转等复合逻辑
 * 一律放在 {@code JobSchedulerService} 的全局临界区内，保证多入口一致。
 */
public class JobStore {

    private final ConcurrentHashMap<String, Job> jobs = new ConcurrentHashMap<>();

    public Job get(String jobId) {
        return jobs.get(jobId);
    }

    public boolean contains(String jobId) {
        return jobs.containsKey(jobId);
    }

    public void put(Job job) {
        jobs.put(job.getJobId(), job);
    }

    /** 返回不可修改的快照视图，遍历期间的结构性增删由 CHM 安全承接。 */
    public Collection<Job> all() {
        return Collections.unmodifiableCollection(jobs.values());
    }

    public int size() {
        return jobs.size();
    }
}
