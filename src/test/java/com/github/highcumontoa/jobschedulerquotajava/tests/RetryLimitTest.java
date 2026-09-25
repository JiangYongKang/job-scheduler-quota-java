package com.github.highcumontoa.jobschedulerquotajava.tests;

import com.github.highcumontoa.jobschedulerquotajava.exception.JobRejectedException;
import com.github.highcumontoa.jobschedulerquotajava.model.ResourceQuota;
import com.github.highcumontoa.jobschedulerquotajava.model.SubmitRequest;
import com.github.highcumontoa.jobschedulerquotajava.tests.support.TestHarness;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景 6：重试上限与重试恢复。
 * - 连续失败达 maxAttempts → FAILED(RETRY_LIMIT_EXHAUSTED)，终态不再被调度/取消；
 * - "failfirst:N" 载荷：前 N 次尝试失败、之后成功 → SUCCEEDED；
 * - 每次重排队都归还配额，重新下发再发放，最终占用归零。
 */
class RetryLimitTest {

    private static final Logger log = LoggerFactory.getLogger(RetryLimitTest.class);

    @Test
    void failureExhaustingRetriesTerminatesJob() throws Exception {
        try (TestHarness h = new TestHarness(60_000, 2)) {
            h.limit("team-a", 10, 10, 10);
            h.jobService.submit(new SubmitRequest("flaky", "team-a",
                    new ResourceQuota(2, 1, 1), 5, "fail"));

            // 第 1 次尝试：失败 → PENDING，attempt=1，配额归还
            h.scheduler.tick();
            h.awaitIdle(3000);
            assertEquals("PENDING", h.jobService.get("flaky").state());
            assertEquals(1, h.jobService.get("flaky").attempt());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));
            log.info("[test] flaky 第1次失败 -> PENDING attempt=1 used=0 判定依据=仍有重试额度");

            // 第 2 次尝试：失败 → FAILED(RETRY_LIMIT_EXHAUSTED)
            h.scheduler.tick();
            h.awaitIdle(3000);
            assertEquals("FAILED", h.jobService.get("flaky").state());
            assertEquals("RETRY_LIMIT_EXHAUSTED", h.jobService.get("flaky").terminalReason());
            assertEquals(2, h.jobService.get("flaky").attempt());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));
            log.info("[test] flaky 第2次失败 -> FAILED reason=RETRY_LIMIT_EXHAUSTED used=0 判定依据=attempt已达上限2");

            // 终态保护：tick 不再下发
            h.scheduler.tick();
            assertEquals("FAILED", h.jobService.get("flaky").state());

            // 终态取消被拒，原因可区分
            JobRejectedException ex = assertThrows(JobRejectedException.class,
                    () -> h.jobService.cancel("flaky"));
            assertEquals("ILLEGAL_STATE_TRANSITION", ex.getReason().name());
            log.info("[test] 终态任务取消被拒 reason={}", ex.getReason());
        }
    }

    @Test
    void failedThenSucceededOnRetryEndsSucceeded() throws Exception {
        try (TestHarness h = new TestHarness(10_000, 3)) {
            h.limit("team-a", 10, 10, 10);
            // failfirst:1：第 1 次尝试失败，第 2 次成功
            h.jobService.submit(new SubmitRequest("recover", "team-a",
                    new ResourceQuota(1, 1, 1), 5, "failfirst:1"));

            h.scheduler.tick();
            h.awaitIdle(3000);
            assertEquals("PENDING", h.jobService.get("recover").state());
            assertEquals(1, h.jobService.get("recover").attempt());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));
            log.info("[test] recover 第1次失败重排队 attempt=1 used=0");

            h.scheduler.tick();
            h.awaitIdle(3000);
            assertEquals("SUCCEEDED", h.jobService.get("recover").state());
            assertEquals(2, h.jobService.get("recover").attempt());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));
            log.info("[test] recover 第2次成功 -> SUCCEEDED attempt=2 used=0 判定依据=重试恢复后正常终结,配额归还");
        }
    }

    @Test
    void leaseTimeoutRetryAlsoHonorsAttemptLimit() {
        try (TestHarness h = new TestHarness(1000, 2)) {
            h.limit("team-a", 10, 10, 10);
            h.jobService.submit(new SubmitRequest("hang", "team-a",
                    new ResourceQuota(1, 1, 1), 5, "never"));

            h.scheduler.tick(); // attempt=1 RUNNING
            h.clock.advanceMs(1001);
            h.leaseManager.reclaimExpired(h.clock.millis());
            assertEquals("PENDING", h.jobService.get("hang").state());
            assertEquals(1, h.jobService.get("hang").attempt());

            h.scheduler.tick(); // attempt=2 RUNNING
            h.clock.advanceMs(1001);
            h.leaseManager.reclaimExpired(h.clock.millis());
            assertEquals("FAILED", h.jobService.get("hang").state());
            assertEquals("RETRY_LIMIT_EXHAUSTED", h.jobService.get("hang").terminalReason());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));
            log.info("[test] hang 两次租约超时 -> FAILED/RETRY_LIMIT_EXHAUSTED used=0 判定依据=超时回收同样受重试上限约束");
        }
    }
}
