package com.github.highcumontoa.jobschedulerquotajava.tests;

import com.github.highcumontoa.jobschedulerquotajava.exception.JobRejectedException;
import com.github.highcumontoa.jobschedulerquotajava.model.JobResult;
import com.github.highcumontoa.jobschedulerquotajava.model.ResourceQuota;
import com.github.highcumontoa.jobschedulerquotajava.model.SubmitRequest;
import com.github.highcumontoa.jobschedulerquotajava.tests.support.TestHarness;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景 5：运行中取消 / 租约回收 / 结果回传并发发生。
 * 无论多少线程以什么顺序竞争，每个 RUNNING 任务只有一个结论（CANCELLED 或 SUCCEEDED/FAILED），
 * 败者以 ILLEGAL_STATE_TRANSITION / LEASE_EXPIRED 被拒，配额只归还一次，最终占用为 0。
 */
class CancelConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(CancelConcurrencyTest.class);

    @Test
    void cancelAndReportRaceProducesUniqueOutcome() throws Exception {
        int runs = 50;
        int cancelledWon = 0;
        int succeededWon = 0;

        for (int r = 0; r < runs; r++) {
            try (TestHarness h = new TestHarness(60_000, 3)) {
                h.limit("team-a", 100, 100, 100);
                String jobId = "race-" + r;
                h.jobService.submit(new SubmitRequest(jobId, "team-a",
                        new ResourceQuota(1, 1, 1), 5, "never"));
                h.scheduler.tick();
                assertEquals("RUNNING", h.jobService.get(jobId).state());

                CountDownLatch start = new CountDownLatch(1);
                AtomicInteger cancelOk = new AtomicInteger();
                AtomicInteger cancelRejected = new AtomicInteger();
                AtomicInteger reportOk = new AtomicInteger();
                AtomicInteger reportRejected = new AtomicInteger();
                // 取消线程
                Thread canceller = new Thread(() -> {
                    try {
                        start.await();
                        h.jobService.cancel(jobId);
                        cancelOk.incrementAndGet();
                    } catch (JobRejectedException e) {
                        cancelRejected.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                // 回传线程（竞争开始时读取当前有效租约；故意随机延迟打乱两个操作的先后）
                Thread reporter = new Thread(() -> {
                    try {
                        start.await();
                        Thread.sleep((long) (Math.random() * 5));
                        String leaseId = h.jobService.get(jobId).leaseId();
                        h.jobService.report(new JobResult(jobId, leaseId, h.executor.getId(), true, "并发回传"));
                        reportOk.incrementAndGet();
                    } catch (JobRejectedException e) {
                        reportRejected.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                canceller.start();
                reporter.start();
                start.countDown();
                canceller.join();
                reporter.join();

                String finalState = h.jobService.get(jobId).state();

                // 唯一性断言：取消与回传恰好一个生效
                assertEquals(1, cancelOk.get() + reportOk.get(),
                        "run=" + r + " 必须恰好一个操作生效");
                assertTrue("CANCELLED".equals(finalState) || "SUCCEEDED".equals(finalState));
                if (cancelOk.get() == 1) {
                    cancelledWon++;
                    assertEquals(1, reportRejected.get());
                    assertEquals("CANCELLED", finalState);
                } else {
                    succeededWon++;
                    assertEquals(1, cancelRejected.get());
                    assertEquals("SUCCEEDED", finalState);
                }
                // 配额账实相符：终态占用必为 0（归还一次，不重复）
                assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"),
                        "run=" + r + " 终态占用必须归零");
                // 终态后再取消必须被明确拒绝
                assertThrows(JobRejectedException.class, () -> h.jobService.cancel(jobId));
            }
        }
        log.info("[test] 取消vs回传并发 {} 轮: 取消先生效={} 回传先生效={} 判定依据=任务锁串行化,结论唯一,配额只归还一次",
                runs, cancelledWon, succeededWon);
        assertTrue(cancelledWon > 0 && succeededWon > 0,
                "随机交错下两种赢家都应出现过，以证明竞争真实存在");
    }

    @Test
    void cancelPendingGoesStraightToCancelledWithoutQuota() {
        try (TestHarness h = new TestHarness(3000, 3)) {
            h.limit("team-a", 100, 100, 100);
            h.jobService.submit(new SubmitRequest("p1", "team-a",
                    new ResourceQuota(1, 1, 1), 5, "success"));
            // 不调度，直接取消 PENDING
            h.jobService.cancel("p1");
            assertEquals("CANCELLED", h.jobService.get("p1").state());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));
            // tick 也不会再下发已终结任务
            h.scheduler.tick();
            assertEquals("CANCELLED", h.jobService.get("p1").state());
            log.info("[test] PENDING取消直接CANCELLED used=0 且后续tick不再下发 判定依据=终态不参与调度");
        }
    }
}
