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

    /**
     * 取消与回传竞争，结论唯一。
     * 分三部分，全部确定性可复现（不依赖随机时序）：
     * 1) 取消先到：取消生效，随后的回传被拒；
     * 2) 回传先到：回传生效，随后的取消被拒；
     * 3) 50 轮真并发竞速：每轮恰好一个生效、终态唯一、配额只归还一次。
     */
    @Test
    void cancelAndReportRaceProducesUniqueOutcome() throws Exception {
        // 阶段 1：取消先到（确定性顺序）
        try (TestHarness h = new TestHarness(60_000, 3)) {
            h.limit("team-a", 100, 100, 100);
            h.jobService.submit(new SubmitRequest("cancel-first", "team-a",
                    new ResourceQuota(1, 1, 1), 5, "never"));
            h.scheduler.tick();
            String leaseId = h.jobService.get("cancel-first").leaseId();
            h.jobService.cancel("cancel-first");
            assertEquals("CANCELLED", h.jobService.get("cancel-first").state());
            JobRejectedException ex = assertThrows(JobRejectedException.class, () -> h.jobService.report(
                    new JobResult("cancel-first", leaseId, h.executor.getId(), true, "迟到的回传")));
            log.info("[test] 取消先到: 回传被拒 reason={} 判定依据=任务锁串行化,取消生效后回传必败", ex.getMessage());
            assertEquals("CANCELLED", h.jobService.get("cancel-first").state());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));
        }

        // 阶段 2：回传先到（确定性顺序）
        try (TestHarness h = new TestHarness(60_000, 3)) {
            h.limit("team-a", 100, 100, 100);
            h.jobService.submit(new SubmitRequest("report-first", "team-a",
                    new ResourceQuota(1, 1, 1), 5, "never"));
            h.scheduler.tick();
            String leaseId = h.jobService.get("report-first").leaseId();
            h.jobService.report(new JobResult("report-first", leaseId, h.executor.getId(), true, "先到的回传"));
            assertEquals("SUCCEEDED", h.jobService.get("report-first").state());
            JobRejectedException ex = assertThrows(JobRejectedException.class,
                    () -> h.jobService.cancel("report-first"));
            log.info("[test] 回传先到: 取消被拒 reason={} 判定依据=任务锁串行化,回传生效后取消必败", ex.getMessage());
            assertEquals("SUCCEEDED", h.jobService.get("report-first").state());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));
        }

        // 阶段 3：50 轮真并发竞速——两个线程同一起跑线竞争，每轮断言"恰好一个生效"。
        // 断言在任何交错下都成立，因此反复运行稳定通过。
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
                String leaseId = h.jobService.get(jobId).leaseId();

                CountDownLatch start = new CountDownLatch(1);
                AtomicInteger cancelOk = new AtomicInteger();
                AtomicInteger cancelRejected = new AtomicInteger();
                AtomicInteger reportOk = new AtomicInteger();
                AtomicInteger reportRejected = new AtomicInteger();
                AtomicReference<Throwable> error = new AtomicReference<>();
                Thread canceller = new Thread(() -> {
                    try {
                        start.await();
                        h.jobService.cancel(jobId);
                        cancelOk.incrementAndGet();
                    } catch (JobRejectedException e) {
                        cancelRejected.incrementAndGet();
                    } catch (Throwable t) {
                        error.compareAndSet(null, t);
                    }
                });
                Thread reporter = new Thread(() -> {
                    try {
                        start.await();
                        h.jobService.report(new JobResult(jobId, leaseId, h.executor.getId(), true, "并发回传"));
                        reportOk.incrementAndGet();
                    } catch (JobRejectedException e) {
                        reportRejected.incrementAndGet();
                    } catch (Throwable t) {
                        error.compareAndSet(null, t);
                    }
                });
                canceller.start();
                reporter.start();
                start.countDown();
                canceller.join();
                reporter.join();
                assertNull(error.get(), "run=" + r + " 并发取消/回传不应产生异常");

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
