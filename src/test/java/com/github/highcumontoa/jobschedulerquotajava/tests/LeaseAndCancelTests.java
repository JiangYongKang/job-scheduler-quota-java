package com.github.highcumontoa.jobschedulerquotajava.tests;

import com.github.highcumontoa.jobschedulerquotajava.core.JobSchedulerService;
import com.github.highcumontoa.jobschedulerquotajava.core.SchedulerProperties;
import com.github.highcumontoa.jobschedulerquotajava.exception.SchedulerException;
import com.github.highcumontoa.jobschedulerquotajava.model.JobRequest;
import com.github.highcumontoa.jobschedulerquotajava.model.JobState;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景五：租约超时回收、运行中取消、取消/回收/回传并发时结论唯一、
 * 非法状态迁移被拒、终态配额准确归还且不重复归还。
 */
class LeaseAndCancelTests {

    private static final Logger log = LoggerFactory.getLogger(LeaseAndCancelTests.class);

    /** 可向前拨动的时钟，用于确定性制造租约超时。 */
    private static final class MutableClock extends Clock {
        private long millis = 1_000_000L;

        void advance(long delta) {
            millis += delta;
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }
    }

    private JobSchedulerService newService(MutableClock clock) {
        SchedulerProperties p = new SchedulerProperties();
        p.setDefaultQuota(10);
        p.setMaxAttempts(3);
        p.setLeaseTtlMillis(5_000);
        JobSchedulerService svc = new JobSchedulerService(p);
        svc.setClock(clock);
        return svc;
    }

    private String dispatch(JobSchedulerService svc, String id, String who, int cpu, int pr) {
        svc.submit(new JobRequest(id, who, cpu, pr));
        return svc.dispatchOne().leaseId();
    }

    @Test
    void leaseTimeoutRequeuesAndThenTerminalAtMaxAttempts() {
        MutableClock clock = new MutableClock();
        JobSchedulerService svc = newService(clock);
        String lease1 = dispatch(svc, "t1", "u", 4, 0);
        assertEquals(4, svc.usedQuota("u"));

        // 未到 TTL：不回收
        clock.advance(4_999);
        assertEquals(0, svc.reclaimExpired());
        assertEquals(JobState.RUNNING, svc.getJob("t1").getState());

        // 到 TTL：回收重排，配额归还，旧租约失效；attempts 已计 1
        clock.advance(1);
        int n = svc.reclaimExpired();
        assertEquals(1, n);
        assertEquals(JobState.PENDING, svc.getJob("t1").getState());
        assertEquals(0, svc.usedQuota("u"), "回收后配额必须归还");
        assertNull(svc.getJob("t1").getLeaseId());
        log.info("[租约超时回收] t1 第1次租约{}超时 -> PENDING, attempts=1, 占用=0/10", lease1);

        // 旧执行者迟到的回传必须被拒（租约栅栏），且不改变状态与占用
        SchedulerException stale = assertThrows(SchedulerException.class,
                () -> svc.complete("t1", lease1, true));
        assertTrue(stale.getMessage().contains("过期租约"));
        assertEquals(JobState.PENDING, svc.getJob("t1").getState());
        log.info("[过期租约拒绝] 旧leaseId迟到回传被拒: {}", stale.getMessage());

        // 第 2 次下发后再次超时重排
        String lease2 = svc.dispatchOne().leaseId();
        assertEquals(4, svc.usedQuota("u"));
        clock.advance(5_000);
        svc.reclaimExpired();
        assertEquals(JobState.PENDING, svc.getJob("t1").getState());
        assertEquals(2, svc.getJob("t1").getAttempts());

        // 第 3 次下发（attempts 达上限 3）后超时：直接 ABANDONED 终结，不再排队
        String lease3 = svc.dispatchOne().leaseId();
        assertEquals(3, svc.getJob("t1").getAttempts());
        clock.advance(5_000);
        svc.reclaimExpired();
        assertEquals(JobState.ABANDONED, svc.getJob("t1").getState());
        assertEquals(0, svc.usedQuota("u"));
        log.info("[重试上限终结] t1 第3次租约{}超时, attempts=3=maxAttempts -> ABANDONED, 占用=0/10, 依据=超时且重试耗尽",
                lease3);

        // 终态不可再取消/回传
        assertThrows(SchedulerException.class, () -> svc.cancel("t1"));
        assertThrows(SchedulerException.class, () -> svc.complete("t1", lease3, true));
        assertNull(svc.getJob("t1").getLeaseId());
    }

    @Test
    void cancelRunningReleasesQuotaOnceAndStaleResultRejected() {
        MutableClock clock = new MutableClock();
        JobSchedulerService svc = newService(clock);
        String lease = dispatch(svc, "c1", "u", 6, 0);
        assertEquals(6, svc.usedQuota("u"));

        svc.cancel("c1");
        assertEquals(JobState.CANCELLED, svc.getJob("c1").getState());
        assertEquals(0, svc.usedQuota("u"), "运行中取消必须归还配额");
        log.info("[运行中取消] c1 租约{}被取消 -> CANCELLED, 占用=0/10", lease);

        // 旧执行者迟到的成功回传不得"复活"任务（租约已在取消时失效）
        SchedulerException stale = assertThrows(SchedulerException.class,
                () -> svc.complete("c1", lease, true));
        assertTrue(stale.getMessage().contains("过期租约"));
        assertEquals(JobState.CANCELLED, svc.getJob("c1").getState());
        assertEquals(0, svc.usedQuota("u"));

        // 重复取消被明确拒绝
        assertThrows(SchedulerException.class, () -> svc.cancel("c1"));
        log.info("[终态稳定] 迟到回传与重复取消均被拒, c1 终态=CANCELLED, 占用仍=0（无重复归还）");
    }

    @Test
    void failureRequeuesUntilMaxAttemptsThenAbandonedWithSingleRelease() {
        MutableClock clock = new MutableClock();
        JobSchedulerService svc = newService(clock);
        String l1 = dispatch(svc, "f1", "u", 5, 0);
        svc.complete("f1", l1, false); // attempts=1 失败重排
        assertEquals(JobState.PENDING, svc.getJob("f1").getState());
        assertEquals(0, svc.usedQuota("u"));

        String l2 = svc.dispatchOne().leaseId();
        svc.complete("f1", l2, false); // attempts=2 失败重排
        String l3 = svc.dispatchOne().leaseId();
        assertEquals(3, svc.getJob("f1").getAttempts());
        svc.complete("f1", l3, false); // attempts=3 达上限
        assertEquals(JobState.ABANDONED, svc.getJob("f1").getState());
        assertEquals(0, svc.usedQuota("u"));
        assertTrue(svc.getJob("f1").getFinishReason().contains("重试上限"));
        log.info("[失败重试上限] f1 连续失败3次 -> ABANDONED, 三次下发/三轮回借后占用=0/10（每轮仅一次归还）");
    }

    @Test
    void cancelAndReclaimAndResultRacingProduceExactlyOneOutcome() throws Exception {
        // 对同一 RUNNING 任务并发发起：取消、租约回收、旧结果回传。终态必须唯一，占用恰好归零一次。
        for (int iter = 0; iter < 50; iter++) {
            final int fi = iter;
            MutableClock clock = new MutableClock();
            JobSchedulerService svc = newService(clock);
            String lease = dispatch(svc, "race-" + fi, "u", 2, 0);
            clock.advance(5_000); // 令租约同时满足超时条件

            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(3);
            ExecutorService pool = Executors.newFixedThreadPool(3);
            AtomicReference<String> winner = new AtomicReference<>("none");
            AtomicInteger errors = new AtomicInteger();

            pool.submit(() -> {
                await(start);
                try {
                    svc.cancel("race-" + fi);
                    winner.compareAndSet("none", "cancel");
                } catch (SchedulerException e) {
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
            pool.submit(() -> {
                await(start);
                try {
                    int n = svc.reclaimExpired();
                    if (n == 1) {
                        winner.compareAndSet("none", "reclaim");
                    }
                } finally {
                    done.countDown();
                }
            });
            pool.submit(() -> {
                await(start);
                try {
                    svc.complete("race-" + fi, lease, true);
                    winner.compareAndSet("none", "result");
                } catch (SchedulerException e) {
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS));
            pool.shutdownNow();

            JobState st = svc.getJob("race-" + fi).getState();
            // 三选一的唯一结论：取消(CANCELLED) / 回收(未达上限则PENDING) / 回传成功(SUCCEEDED)
            assertTrue(st == JobState.CANCELLED || st == JobState.PENDING || st == JobState.SUCCEEDED,
                    "iter=" + fi + " 出现意外状态 " + st);
            // 无论谁先抢到锁，RUNNING 都不可能残留（三方至少有一方会终结/回收），占用必须恰好归零一次
            assertEquals(0, svc.usedQuota("u"), "iter=" + fi + " 占用与唯一终态不一致, state=" + st);
            log.info("[并发唯一] iter={} 胜出路径={}, 其他方被拒={}, 终态={}, 占用=0/10",
                    iter, winner.get(), errors.get(), st);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
