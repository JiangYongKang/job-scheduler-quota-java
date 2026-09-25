package com.github.highcumontoa.jobschedulerquotajava.tests;

import com.github.highcumontoa.jobschedulerquotajava.exception.JobRejectedException;
import com.github.highcumontoa.jobschedulerquotajava.model.RejectReason;
import com.github.highcumontoa.jobschedulerquotajava.model.ResourceQuota;
import com.github.highcumontoa.jobschedulerquotajava.model.SubmitRequest;
import com.github.highcumontoa.jobschedulerquotajava.tests.support.TestHarness;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景 1：配额耗尽。
 * - 超出主体配额的提交必须以 QUOTA_EXCEEDED_ON_SUBMIT 被拒绝，且拒绝不改变既有占用；
 * - 已下发任务占满配额后，新任务 tick 时只能保持 PENDING 等待，绝不越权下发；
 * - 任务终结后配额准确归还，等待任务随后可被下发。
 *
 * 说明：测试用 "sleep:N" 载荷让本地任务在断言窗口内保持 RUNNING；
 * 租约过期判定走可控 ManualClock，真实 sleep 不会推进它，因此不会误判超时。
 */
class QuotaExhaustionTest {

    private static final Logger log = LoggerFactory.getLogger(QuotaExhaustionTest.class);

    private SubmitRequest req(String jobId, String subject, int cpu, int priority, String payload) {
        return new SubmitRequest(jobId, subject, new ResourceQuota(cpu, 1, 1), priority, payload);
    }

    @Test
    void submitTimeQuotaExhaustionIsRejectedWithoutChangingUsage() throws Exception {
        try (TestHarness h = new TestHarness(3000, 3)) {
            h.limit("team-a", 4, 10, 10);

            h.jobService.submit(req("job-1", "team-a", 2, 5, "sleep:800"));
            h.jobService.submit(req("job-2", "team-a", 2, 5, "sleep:800"));

            // 下发两个任务，各占 2cpu，合计 4cpu 达上限
            h.scheduler.tick();
            assertEquals("RUNNING", h.jobService.get("job-1").state());
            assertEquals("RUNNING", h.jobService.get("job-2").state());
            ResourceQuota usedAfterDispatch = h.jobService.getUsedQuota("team-a");
            log.info("[test] job-1/job-2 下发后 used={} 判定依据=各占2cpu,共4cpu达上限", usedAfterDispatch);
            assertEquals(new ResourceQuota(4, 2, 2), usedAfterDispatch);

            // 再提交一个 1cpu 的任务：当前占用 4 + 需求 1 > 上限 4 → 提交即拒绝
            JobRejectedException ex = assertThrows(JobRejectedException.class,
                    () -> h.jobService.submit(req("job-3", "team-a", 1, 5, "success")));
            assertEquals(RejectReason.QUOTA_EXCEEDED_ON_SUBMIT, ex.getReason());
            log.info("[test] job-3 提交被拒 reason={} message={}", ex.getReason(), ex.getMessage());

            // 拒绝不改变既有占用：任务数仍为 2，占用仍为 4
            assertEquals(new ResourceQuota(4, 2, 2), h.jobService.getUsedQuota("team-a"));
            assertEquals(2, h.jobService.list().size());

            h.awaitIdle(3000);
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));
            assertEquals("SUCCEEDED", h.jobService.get("job-1").state());
            assertEquals("SUCCEEDED", h.jobService.get("job-2").state());
            log.info("[test] 任务全部终结 used=0 判定依据=终态一次性归还配额,无重复归还");
        }
    }

    @Test
    void dispatchWaitsWhenQuotaUnavailableAndRunsAfterRelease() throws Exception {
        try (TestHarness h = new TestHarness(10_000, 3)) {
            h.limit("team-a", 2, 10, 10); // 只够一个 2cpu 任务
            h.jobService.submit(req("job-a", "team-a", 2, 5, "sleep:800"));
            h.jobService.submit(req("job-b", "team-a", 2, 5, "sleep:200"));

            h.scheduler.tick();
            assertEquals("RUNNING", h.jobService.get("job-a").state());
            // job-b 拿不到配额，必须仍是 PENDING（等待而非越权下发）
            assertEquals("PENDING", h.jobService.get("job-b").state());
            assertEquals(new ResourceQuota(2, 1, 1), h.jobService.getUsedQuota("team-a"));
            log.info("[test] job-a RUNNING 占用2cpu; job-b PENDING 判定依据=配额不足只能等待 used={}",
                    h.jobService.getUsedQuota("team-a"));

            // 等待 job-a 执行完成、配额归还
            h.awaitIdle(3000);
            assertEquals("SUCCEEDED", h.jobService.get("job-a").state());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));

            // 再调度一轮：等待中的 job-b 被下发
            h.scheduler.tick();
            assertEquals("RUNNING", h.jobService.get("job-b").state());
            log.info("[test] 配额归还后 job-b 得以下发 state={}", h.jobService.get("job-b").state());
            h.awaitIdle(3000);
            assertEquals("SUCCEEDED", h.jobService.get("job-b").state());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));
        }
    }
}
