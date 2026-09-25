package com.github.highcumontoa.jobschedulerquotajava.tests;

import com.github.highcumontoa.jobschedulerquotajava.core.JobSchedulerService;
import com.github.highcumontoa.jobschedulerquotajava.core.SchedulerProperties;
import com.github.highcumontoa.jobschedulerquotajava.model.JobRequest;
import com.github.highcumontoa.jobschedulerquotajava.model.JobState;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景六：重试上限。"执行失败"与"租约超时"两条路径共享 attempts 计数，
 * 达到 maxAttempts 后终结为 ABANDONED（明确终结、不再排队），且配额准确归还。
 */
class RetryLimitTests {

    private static final Logger log = LoggerFactory.getLogger(RetryLimitTests.class);

    private static final class MutableClock extends Clock {
        private long millis = 2_000_000L;

        void advance(long d) {
            millis += d;
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

    private JobSchedulerService newService(MutableClock clock, int maxAttempts) {
        SchedulerProperties p = new SchedulerProperties();
        p.setDefaultQuota(10);
        p.setMaxAttempts(maxAttempts);
        p.setLeaseTtlMillis(1_000);
        JobSchedulerService svc = new JobSchedulerService(p);
        svc.setClock(clock);
        return svc;
    }

    @Test
    void mixedFailureAndTimeoutTerminatesExactlyAtMaxAttempts() {
        MutableClock clock = new MutableClock();
        int max = 3;
        JobSchedulerService svc = newService(clock, max);
        svc.submit(new JobRequest("m1", "u", 7, 0));

        // 第 1 次：执行失败 -> 重排
        String l1 = svc.dispatchOne().leaseId();
        svc.complete("m1", l1, false);
        assertEquals(1, svc.getJob("m1").getAttempts());
        assertEquals(JobState.PENDING, svc.getJob("m1").getState());
        assertEquals(0, svc.usedQuota("u"));

        // 第 2 次：租约超时（失联）-> 重排
        String l2 = svc.dispatchOne().leaseId();
        clock.advance(1_000);
        svc.reclaimExpired();
        assertEquals(2, svc.getJob("m1").getAttempts());
        assertEquals(JobState.PENDING, svc.getJob("m1").getState());
        assertEquals(0, svc.usedQuota("u"));

        // 第 3 次：再失败 -> attempts(3) >= max(3) => ABANDONED
        String l3 = svc.dispatchOne().leaseId();
        svc.complete("m1", l3, false);
        assertEquals(3, svc.getJob("m1").getAttempts());
        assertEquals(JobState.ABANDONED, svc.getJob("m1").getState());
        assertEquals(0, svc.usedQuota("u"), "终结后配额必须归零");
        assertTrue(svc.getJob("m1").getFinishReason().contains("重试上限"));
        log.info("[重试上限-混合路径] m1: 失败(1)+超时(2)+失败(3), attempts=3=maxAttempts -> ABANDONED, jobId=m1, 占用=0/10, 依据=两条路径共享attempts");

        // 终结后不再被挑选（store 仍保留终态记录用于解释，但永远不会再次下发）
        var pick = svc.dispatchOne();
        assertTrue(!pick.dispatched());
    }

    @Test
    void successBeforeLimitResetsOutcomeToSucceeded() {
        MutableClock clock = new MutableClock();
        JobSchedulerService svc = newService(clock, 3);
        svc.submit(new JobRequest("m2", "u", 2, 0));

        String l1 = svc.dispatchOne().leaseId();
        svc.complete("m2", l1, false); // 失败1
        String l2 = svc.dispatchOne().leaseId();
        clock.advance(1_000);
        svc.reclaimExpired();          // 超时2
        String l3 = svc.dispatchOne().leaseId();
        svc.complete("m2", l3, true);  // 第3次成功，未超限即成功终结
        assertEquals(JobState.SUCCEEDED, svc.getJob("m2").getState());
        assertEquals(0, svc.usedQuota("u"));
        log.info("[重试后成功] m2: 失败+超时后第3次成功 -> SUCCEEDED, jobId=m2, 占用=0/10");
    }

    @Test
    void abandonedJobKeepsZeroQuotaAcrossFurtherOperations() {
        MutableClock clock = new MutableClock();
        JobSchedulerService svc = newService(clock, 1); // 只允许 1 次
        svc.submit(new JobRequest("m3", "u", 9, 0));
        String l1 = svc.dispatchOne().leaseId();
        svc.complete("m3", l1, false); // 第1次即失败且达上限
        assertEquals(JobState.ABANDONED, svc.getJob("m3").getState());

        // 再回收、再取消都不得重复归还（账本下限保护兜底，正常路径根本不会进入 release）
        clock.advance(10_000);
        assertEquals(0, svc.reclaimExpired(), "终态任务不会被租约回收扫描命中");
        assertEquals(0, svc.usedQuota("u"));
        log.info("[终态幂等] m3 已ABANDONED, 再次回收命中0个任务, jobId=m3, 占用稳定=0/10（无重复归还）");
    }
}
