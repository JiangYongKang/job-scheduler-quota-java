package com.github.highcumontoa.jobschedulerquotajava.tests;

import com.github.highcumontoa.jobschedulerquotajava.core.JobSchedulerService;
import com.github.highcumontoa.jobschedulerquotajava.core.SchedulerProperties;
import com.github.highcumontoa.jobschedulerquotajava.model.JobRequest;
import com.github.highcumontoa.jobschedulerquotajava.model.JobState;
import com.github.highcumontoa.jobschedulerquotajava.model.RejectReason;
import com.github.highcumontoa.jobschedulerquotajava.model.SubmitResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景二：重复提交冲突。PENDING/RUNNING 的同 id 请求必须以 DUPLICATE_SUBMISSION 拒绝，
 * 且与 QUOTA_EXCEEDED 可区分；终态后同 id 可作为新任务再次提交。
 */
class DuplicateSubmitTests {

    private static final Logger log = LoggerFactory.getLogger(DuplicateSubmitTests.class);

    private JobSchedulerService newService() {
        SchedulerProperties p = new SchedulerProperties();
        p.setDefaultQuota(10);
        p.setMaxAttempts(3);
        p.setLeaseTtlMillis(5_000);
        return new JobSchedulerService(p);
    }

    @Test
    void duplicateWhilePendingOrRunningRejectedWithoutChangingOccupancy() {
        JobSchedulerService svc = newService();
        assertTrue(svc.submit(new JobRequest("dup-1", "team-b", 4, 5)).accepted());

        SubmitResult dupPending = svc.submit(new JobRequest("dup-1", "team-b", 4, 5));
        assertFalse(dupPending.accepted());
        assertEquals(RejectReason.DUPLICATE_SUBMISSION, dupPending.reason());
        log.info("[重复提交] PENDING 阶段 dup-1 重复提交被拒原因={}，占用={}/10",
                dupPending.reason(), svc.usedQuota("team-b"));

        // 下发后再以同 id 提交：仍冲突，原因不变，占用不因拒绝而变化
        var d = svc.dispatchOne();
        assertTrue(d.dispatched());
        assertEquals(4, svc.usedQuota("team-b"));
        SubmitResult dupRunning = svc.submit(new JobRequest("dup-1", "team-b", 4, 5));
        assertFalse(dupRunning.accepted());
        assertEquals(RejectReason.DUPLICATE_SUBMISSION, dupRunning.reason());
        assertEquals(4, svc.usedQuota("team-b"), "重复拒绝不得改变占用");
        log.info("[重复提交] RUNNING 阶段 dup-1 重复提交被拒原因={}，占用仍=4/10，判定依据=同id未终结",
                dupRunning.reason());

        // 与配额原因可区分：换一个新 id 但超配额，应得到 QUOTA_EXCEEDED
        SubmitResult quota = svc.submit(new JobRequest("other", "team-b", 7, 5));
        assertFalse(quota.accepted());
        assertEquals(RejectReason.QUOTA_EXCEEDED, quota.reason());
        log.info("[原因可区分] other(7) 在占用4/10 下被拒原因={} ≠ DUPLICATE_SUBMISSION", quota.reason());

        // 终态后同 id 可重新提交为全新任务（配额已归还）
        svc.complete("dup-1", d.leaseId(), true);
        assertEquals(JobState.SUCCEEDED, svc.getJob("dup-1").getState());
        SubmitResult reused = svc.submit(new JobRequest("dup-1", "team-b", 4, 5));
        assertTrue(reused.accepted(), "终态后 id 可复用为新提交");
        assertNotEquals(0, svc.getJob("dup-1").getEnqueueSeq());
        assertEquals(JobState.PENDING, svc.getJob("dup-1").getState());
        log.info("[终态复用] dup-1 成功终结后同id重新提交通过，新任务处于PENDING");
    }
}
