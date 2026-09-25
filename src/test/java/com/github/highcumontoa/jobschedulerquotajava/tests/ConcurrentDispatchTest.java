package com.github.highcumontoa.jobschedulerquotajava.tests;

import com.github.highcumontoa.jobschedulerquotajava.model.ResourceQuota;
import com.github.highcumontoa.jobschedulerquotajava.model.SubmitRequest;
import com.github.highcumontoa.jobschedulerquotajava.tests.support.TestHarness;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景 3：并发调度同一任务。
 * 多个调度线程同时 tick 时，同一 PENDING 任务只能被一个线程迁移到 RUNNING 并下发，
 * 配额也只能发放一次（不会重复占用、不会出现两个执行者同时拿到同一租约）。
 */
class ConcurrentDispatchTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentDispatchTest.class);

    @Test
    void sameJobDispatchedExactlyOnceUnderConcurrentTicks() throws Exception {
        try (TestHarness h = new TestHarness(60_000, 3)) {
            h.limit("team-a", 100, 100, 100);
            int jobCount = 20;
            for (int i = 0; i < jobCount; i++) {
                h.jobService.submit(new SubmitRequest(
                        "job-" + i, "team-a", new ResourceQuota(1, 1, 1), i % 5, "sleep:120"));
            }

            int schedulers = 8;
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(schedulers);
            List<String> allDispatched = Collections.synchronizedList(new ArrayList<>());
            long deadline = System.currentTimeMillis() + 1500; // 持续并发 tick，覆盖任务完成后的再调度

            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < schedulers; i++) {
                Thread t = new Thread(() -> {
                    try {
                        start.await();
                        while (System.currentTimeMillis() < deadline) {
                            allDispatched.addAll(h.scheduler.tick());
                            Thread.sleep(5);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                }, "tick-" + i);
                threads.add(t);
                t.start();
            }
            start.countDown();
            assertTrue(done.await(10, java.util.concurrent.TimeUnit.SECONDS));

            // 核心不变量 1：竞争窗口内没有任何任务被重复下发
            long distinct = allDispatched.stream().distinct().count();
            assertEquals(distinct, allDispatched.size(),
                    "竞争窗口内不允许重复下发同一任务");

            // 兜底：继续调度直到所有任务都终结（或超时断言）
            long guard = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < guard) {
                h.awaitIdle(2_000);
                boolean allDone = true;
                for (int i = 0; i < jobCount; i++) {
                    if (!"SUCCEEDED".equals(h.jobService.get("job-" + i).state())) {
                        allDone = false;
                    }
                }
                if (allDone) {
                    break;
                }
                h.scheduler.tick();
            }

            long finalDistinct = allDispatched.stream().distinct().count();
            log.info("[test] 并发tick调度 totalDispatchedEvents={} distinctJobs={} jobCount={} 判定依据=任务锁+PENDING二次确认保证不重复下发",
                    allDispatched.size(), finalDistinct, jobCount);

            // 每个任务恰好被下发一次（最终全部成功）
            assertEquals(jobCount, finalDistinct, "每个任务只能被下发一次");
            for (int i = 0; i < jobCount; i++) {
                assertEquals("SUCCEEDED", h.jobService.get("job-" + i).state(),
                        "job-" + i + " 应成功");
            }
            // 核心不变量 2：占用归零（无泄漏、无重复发放导致的账实不符）
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));
            log.info("[test] 全部{}任务SUCCEEDED used=0 判定依据=终态配额准确归还", jobCount);
        }
    }
}
