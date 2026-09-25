package com.github.highcumontoa.jobschedulerquotajava.tests;

import com.github.highcumontoa.jobschedulerquotajava.core.JobSchedulerService;
import com.github.highcumontoa.jobschedulerquotajava.core.SchedulerProperties;
import com.github.highcumontoa.jobschedulerquotajava.model.DispatchResult;
import com.github.highcumontoa.jobschedulerquotajava.model.JobRequest;
import com.github.highcumontoa.jobschedulerquotajava.model.JobState;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景四：并发调度同一任务。
 * N 个"执行者线程"在同一时刻调用 dispatchOne 抢同一批任务，必须满足：
 * 每个任务恰好被一个执行者领取（唯一 leaseId），占用量与领取数严格一致。
 */
class ConcurrentDispatchTests {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentDispatchTests.class);

    @Test
    void sameJobNeverDispatchedToTwoExecutorsUnderContention() throws Exception {
        SchedulerProperties p = new SchedulerProperties();
        p.setDefaultQuota(1000);
        p.setMaxAttempts(3);
        p.setLeaseTtlMillis(60_000);
        JobSchedulerService svc = new JobSchedulerService(p);

        int jobCount = 20;
        for (int i = 0; i < jobCount; i++) {
            assertTrue(svc.submit(new JobRequest("job-" + i, "team-x", 2, i % 3)).accepted());
        }

        int executors = 16;
        int rounds = 60; // 远超任务数的调用次数，制造激烈竞争
        ExecutorService pool = Executors.newFixedThreadPool(executors);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(executors);
        ConcurrentLinkedQueue<DispatchResult> wins = new ConcurrentLinkedQueue<>();
        AtomicInteger noop = new AtomicInteger();

        for (int e = 0; e < executors; e++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int r = 0; r < rounds; r++) {
                        DispatchResult result = svc.dispatchOne();
                        if (result.dispatched()) {
                            wins.add(result);
                        } else {
                            noop.incrementAndGet();
                        }
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdownNow();

        // 恰好 jobCount 次成功下发
        assertEquals(jobCount, wins.size(), "每个任务必须恰好被下发一次");
        // 每个 jobId 恰好出现一次
        long distinctJobs = wins.stream().map(DispatchResult::jobId).distinct().count();
        assertEquals(jobCount, distinctJobs, "同一任务不得被两个执行者领取");
        // 每个 leaseId 唯一
        long distinctLeases = wins.stream().map(DispatchResult::leaseId).distinct().count();
        assertEquals(jobCount, distinctLeases, "租约必须唯一");
        // 占用量 == 成功下发任务的资源总和
        assertEquals(jobCount * 2, svc.usedQuota("team-x"), "占用与下发数严格一致，无越权/无漏计");

        for (int i = 0; i < jobCount; i++) {
            assertEquals(JobState.RUNNING, svc.getJob("job-" + i).getState());
        }
        log.info("[并发调度] {}个执行者竞争{}个任务: 成功领取={}, 空转={}, 唯一任务={}, 唯一租约={}, 占用={}/1000",
                executors, jobCount, wins.size(), noop.get(), distinctJobs, distinctLeases,
                svc.usedQuota("team-x"));
    }

    @Test
    void concurrentDispatchOnSingleJobYieldsExactlyOneWinner() throws Exception {
        SchedulerProperties p = new SchedulerProperties();
        p.setDefaultQuota(10);
        p.setMaxAttempts(3);
        p.setLeaseTtlMillis(60_000);
        JobSchedulerService svc = new JobSchedulerService(p);
        svc.submit(new JobRequest("only", "team-y", 3, 0));

        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch barrier = new CountDownLatch(threads);
        List<DispatchResult> results = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    synchronized (results) {
                        results.add(svc.dispatchOne());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    barrier.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(barrier.await(20, TimeUnit.SECONDS));
        pool.shutdownNow();

        long wins = results.stream().filter(DispatchResult::dispatched).count();
        long winLeases = results.stream().filter(DispatchResult::dispatched)
                .map(DispatchResult::leaseId).distinct().count();
        assertEquals(1, wins, "单一任务在并发下只能有一个执行者中标");
        assertEquals(1, winLeases, "中标租约必须唯一");
        assertEquals(3, svc.usedQuota("team-y"));
        assertTrue(svc.getJob("only").getState() == JobState.RUNNING);
        assertEquals(results.stream().filter(DispatchResult::dispatched).findFirst().orElseThrow().leaseId(),
                svc.getJob("only").getLeaseId(), "任务上记录的租约即中标租约");
        log.info("[单任务竞争] {}个线程同时抢 only: 中标数={}, 占用={}/10, 唯一leaseId={}",
                threads, wins, svc.usedQuota("team-y"), svc.getJob("only").getLeaseId());
    }
}
