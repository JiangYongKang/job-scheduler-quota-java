package com.github.highcumontoa.jobschedulerquotajava.tests;

import com.github.highcumontoa.jobschedulerquotajava.exception.JobRejectedException;
import com.github.highcumontoa.jobschedulerquotajava.model.ResourceQuota;
import com.github.highcumontoa.jobschedulerquotajava.model.SubmitRequest;
import com.github.highcumontoa.jobschedulerquotajava.tests.support.TestHarness;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景 2：重复提交。
 * 同一 jobId 的重复提交（含并发）只能成功一次，其余以 DUPLICATE_SUBMISSION 拒绝，
 * 与配额拒绝原因可区分，且重复请求不改变既有占用。
 */
class DuplicateSubmissionTest {

    private static final Logger log = LoggerFactory.getLogger(DuplicateSubmissionTest.class);

    private SubmitRequest req(String jobId, String subject, int cpu) {
        return new SubmitRequest(jobId, subject, new ResourceQuota(cpu, 1, 1), 5, "success");
    }

    @Test
    void duplicateJobIdIsRejectedWithDistinctReason() {
        try (TestHarness h = new TestHarness(3000, 3)) {
            h.limit("team-a", 100, 100, 100);
            h.jobService.submit(req("dup-1", "team-a", 1));

            JobRejectedException ex = assertThrows(JobRejectedException.class,
                    () -> h.jobService.submit(req("dup-1", "team-a", 1)));
            assertEquals("DUPLICATE_SUBMISSION", ex.getReason().name());
            assertNotEquals("QUOTA_EXCEEDED_ON_SUBMIT", ex.getReason().name());
            log.info("[test] 重复提交 jobId=dup-1 被拒 reason={} 判定依据=jobId幂等键冲突", ex.getReason());

            // 存储与占用均不受重复请求影响
            assertEquals(1, h.jobService.list().size());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));
        }
    }

    @Test
    void concurrentDuplicateSubmissionsSucceedExactlyOnce() throws Exception {
        try (TestHarness h = new TestHarness(3000, 3)) {
            int threads = 16;
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            AtomicInteger accepted = new AtomicInteger();
            AtomicInteger duplicateRejected = new AtomicInteger();

            for (int i = 0; i < threads; i++) {
                Thread t = new Thread(() -> {
                    try {
                        start.await();
                        h.jobService.submit(req("race-1", "team-a", 1));
                        accepted.incrementAndGet();
                    } catch (JobRejectedException e) {
                        if ("DUPLICATE_SUBMISSION".equals(e.getReason().name())) {
                            duplicateRejected.incrementAndGet();
                        }
                    } catch (InterruptedException ignore) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                }, "submitter-" + i);
                t.start();
            }
            start.countDown();
            assertTrue(done.await(5, java.util.concurrent.TimeUnit.SECONDS));

            log.info("[test] 并发提交 jobId=race-1 threads={} accepted={} duplicateRejected={} 判定依据=putIfAbsent保证仅成功一次",
                    threads, accepted.get(), duplicateRejected.get());
            assertEquals(1, accepted.get());
            assertEquals(threads - 1, duplicateRejected.get());
            assertEquals(1, h.jobService.list().size());
        }
    }
}
