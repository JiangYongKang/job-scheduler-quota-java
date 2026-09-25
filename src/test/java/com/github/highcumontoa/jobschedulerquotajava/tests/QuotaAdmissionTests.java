package com.github.highcumontoa.jobschedulerquotajava.tests;

import com.github.highcumontoa.jobschedulerquotajava.core.JobSchedulerService;
import com.github.highcumontoa.jobschedulerquotajava.core.SchedulerProperties;
import com.github.highcumontoa.jobschedulerquotajava.model.JobRequest;
import com.github.highcumontoa.jobschedulerquotajava.model.RejectReason;
import com.github.highcumontoa.jobschedulerquotajava.model.SubmitResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景一：配额耗尽与准入。
 * 日志中打印任务标识、配额占用（used/limit）与判定依据。
 */
class QuotaAdmissionTests {

    private static final Logger log = LoggerFactory.getLogger(QuotaAdmissionTests.class);

    private JobSchedulerService newService(int quota) {
        SchedulerProperties p = new SchedulerProperties();
        p.setDefaultQuota(quota);
        p.setMaxAttempts(3);
        p.setLeaseTtlMillis(5_000);
        return new JobSchedulerService(p);
    }

    @Test
    void submitRejectedWhenRunningQuotaExhaustedAndOccupancyUnchanged() {
        JobSchedulerService svc = newService(10);

        // 先占满：j1=4(下发), j2=6(下发) => used=10
        assertTrue(svc.submit(new JobRequest("j1", "team-a", 4, 0)).accepted());
        assertTrue(svc.submit(new JobRequest("j2", "team-a", 6, 0)).accepted());
        assertNotNull(svc.dispatchOne().leaseId());
        assertNotNull(svc.dispatchOne().leaseId());
        assertEquals(10, svc.usedQuota("team-a"));
        log.info("[配额耗尽] j1/j2 下发后占用=10/10，尝试提交 j3(申请2)");

        // j3=2：当前运行中占用 10，10+2>10 => 提交即拒绝（PENDING 不允许进入，否则将无界排队）
        SubmitResult rejected = svc.submit(new JobRequest("j3", "team-a", 2, 0));
        assertFalse(rejected.accepted());
        assertEquals(RejectReason.QUOTA_EXCEEDED, rejected.reason());
        log.info("[配额耗尽] j3 被拒绝原因={}（{}），拒绝后占用={}/10",
                rejected.reason(), rejected.message(), svc.usedQuota("team-a"));

        // 拒绝不得改变既有占用
        assertEquals(10, svc.usedQuota("team-a"), "拒绝不得改变既有占用");
        assertNotNull(svc.getJob("j1"));

        // j1 成功终结后归还 4，占用变 6；此时 j3 重新提交可通过（可解释的恢复）
        var j1Lease = svc.getJob("j1").getLeaseId();
        svc.complete("j1", j1Lease, true);
        assertEquals(6, svc.usedQuota("team-a"));
        SubmitResult retry = svc.submit(new JobRequest("j3", "team-a", 2, 0));
        assertTrue(retry.accepted(), "配额释放后同请求应可准入");
        log.info("[配额恢复] j1 终结释放4 后占用=6/10，j3 重新提交通过");
    }

    @Test
    void pendingJobsDoNotConsumeQuotaUntilDispatched() {
        JobSchedulerService svc = newService(5);
        // 配额 5：p1=3、p2=3 都可准入（PENDING 不占配额）
        assertTrue(svc.submit(new JobRequest("p1", "u", 3, 0)).accepted());
        assertTrue(svc.submit(new JobRequest("p2", "u", 3, 0)).accepted());
        assertEquals(0, svc.usedQuota("u"), "PENDING 不占配额");

        // 先下发 p1 占用 3；再次挑选时 p2 需要 3，3+3>5 只能等待，不得越权下发
        assertNotNull(svc.dispatchOne().leaseId());
        var second = svc.dispatchOne();
        assertFalse(second.dispatched(), "配额不足时任务保持等待，不得越权下发");
        assertEquals("p1", svc.getJob("p1").getJobId());
        assertEquals(com.github.highcumontoa.jobschedulerquotajava.model.JobState.PENDING,
                svc.getJob("p2").getState());
        assertEquals(3, svc.usedQuota("u"));
        log.info("[等待不越权] p1 占用3/5，p2 需要3 被判定继续等待，占用仍=3");
    }
}
