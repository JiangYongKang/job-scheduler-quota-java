package com.github.highcumontoa.jobschedulerquotajava.api;

import com.github.highcumontoa.jobschedulerquotajava.core.JobSchedulerService;
import com.github.highcumontoa.jobschedulerquotajava.model.DispatchResult;
import com.github.highcumontoa.jobschedulerquotajava.model.Job;
import com.github.highcumontoa.jobschedulerquotajava.model.JobRequest;
import com.github.highcumontoa.jobschedulerquotajava.model.SubmitResult;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 任务调度的 HTTP 入口。所有入口均委托给 {@link JobSchedulerService} 同一权威实例，
 * 因此命令行/测试/HTTP 三条入口得到的结论完全一致。
 */
@RestController
public class JobController {

    private final JobSchedulerService scheduler;

    public JobController(JobSchedulerService scheduler) {
        this.scheduler = scheduler;
    }

    /** 提交任务并做配额准入；被拒绝返回 409，响应体携带可区分的 reason 码。 */
    @PostMapping("/api/jobs")
    public ResponseEntity<SubmitResult> submit(@RequestBody JobRequest request) {
        SubmitResult result = scheduler.submit(request);
        return result.accepted()
                ? ResponseEntity.status(201).body(result)
                : ResponseEntity.status(409).body(result);
    }

    /** 触发一次"挑选并下发"，主要用于本地手动验证；后台执行者也会周期性调用同一方法。 */
    @PostMapping("/api/dispatch")
    public DispatchResult dispatch() {
        return scheduler.dispatchOne();
    }

    /** 执行者回传执行结果，必须携带下发时授予的 leaseId。 */
    @PostMapping("/api/jobs/{jobId}/complete")
    public Map<String, Object> complete(@PathVariable String jobId,
                                        @RequestParam String leaseId,
                                        @RequestParam(defaultValue = "true") boolean success) {
        scheduler.complete(jobId, leaseId, success);
        return body(scheduler.getJob(jobId));
    }

    /** 取消等待中或运行中的任务；终态任务返回 409。 */
    @PostMapping("/api/jobs/{jobId}/cancel")
    public Map<String, Object> cancel(@PathVariable String jobId) {
        scheduler.cancel(jobId);
        return body(scheduler.getJob(jobId));
    }

    /** 查询任务终态、租约与重试信息。 */
    @GetMapping("/api/jobs/{jobId}")
    public Map<String, Object> getJob(@PathVariable String jobId) {
        return body(scheduler.getJob(jobId));
    }

    /** 查询某提交主体当前的配额占用与上限，便于解释准入结论。 */
    @GetMapping("/api/quota")
    public Map<String, Object> quota(@RequestParam String submitter) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("submitter", submitter);
        map.put("used", scheduler.usedQuota(submitter));
        map.put("limit", scheduler.quotaLimit(submitter));
        return map;
    }

    private Map<String, Object> body(Job job) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("jobId", job.getJobId());
        map.put("submitter", job.getSubmitter());
        map.put("cpuUnits", job.getCpuUnits());
        map.put("priority", job.getPriority());
        map.put("state", job.getState());
        map.put("attempts", job.getAttempts());
        map.put("leaseId", job.getLeaseId());
        map.put("leaseExpiresAt", job.getLeaseExpiresAt());
        map.put("finishReason", job.getFinishReason());
        return map;
    }
}
