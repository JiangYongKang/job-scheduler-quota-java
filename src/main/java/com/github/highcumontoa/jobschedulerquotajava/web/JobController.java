package com.github.highcumontoa.jobschedulerquotajava.web;

import com.github.highcumontoa.jobschedulerquotajava.model.JobResult;
import com.github.highcumontoa.jobschedulerquotajava.model.JobSnapshot;
import com.github.highcumontoa.jobschedulerquotajava.model.ResourceQuota;
import com.github.highcumontoa.jobschedulerquotajava.model.SubmitRequest;
import com.github.highcumontoa.jobschedulerquotajava.service.JobService;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST 入口：提交 / 列表 / 查询 / 取消 / 结果回传 / 手动触发调度与回收。
 * 所有入口共用同一套域服务，因此结论一致。
 */
@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    @PostMapping
    public JobSnapshot submit(@RequestBody SubmitRequest request) {
        return jobService.submit(request);
    }

    @GetMapping
    public List<JobSnapshot> list() {
        return jobService.list();
    }

    @GetMapping("/{jobId}")
    public JobSnapshot get(@PathVariable String jobId) {
        return jobService.get(jobId);
    }

    @PostMapping("/{jobId}/cancel")
    public JobSnapshot cancel(@PathVariable String jobId) {
        return jobService.cancel(jobId);
    }

    @PostMapping("/results")
    public JobSnapshot report(@RequestBody JobResult result) {
        return jobService.report(result);
    }

    @GetMapping("/quotas/{subject}")
    public Map<String, Object> quota(@PathVariable String subject) {
        return Map.of(
                "subject", subject,
                "used", jobService.getUsedQuota(subject));
    }

    @PutMapping("/quotas/{subject}")
    public Map<String, Object> setQuota(@PathVariable String subject, @RequestBody ResourceQuota limit) {
        jobService.setQuotaLimit(subject, limit);
        return Map.of(
                "subject", subject,
                "limit", limit,
                "used", jobService.getUsedQuota(subject));
    }
}
