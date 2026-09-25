package com.github.highcumontoa.jobschedulerquotajava.tests;

import com.github.highcumontoa.jobschedulerquotajava.exception.JobRejectedException;
import com.github.highcumontoa.jobschedulerquotajava.model.JobResult;
import com.github.highcumontoa.jobschedulerquotajava.model.ResourceQuota;
import com.github.highcumontoa.jobschedulerquotajava.model.SubmitRequest;
import com.github.highcumontoa.jobschedulerquotajava.tests.support.TestHarness;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景 4：租约超时回收 / 执行者失联。
 * 用可控 ManualClock 稳定复现：
 * - 租约到期后 reclaimExpired 把 RUNNING 任务回收，配额先归还，任务回到 PENDING；
 * - 重新下发后可成功完成，全程配额占用账实相符；
 * - 租约过期后执行者的迟到回传必须被拒绝（LEASE_EXPIRED），不改变已回收结论；
 * - 执行者失联可通过 reclaim(job) 立即回收，不依赖时钟。
 */
class LeaseReclaimTest {

    private static final Logger log = LoggerFactory.getLogger(LeaseReclaimTest.class);

    @Test
    void expiredLeaseIsReclaimedRequeuedAndThenSucceeds() {
        try (TestHarness h = new TestHarness(1000, 3)) {
            h.limit("team-a", 10, 10, 10);
            h.jobService.submit(new SubmitRequest("job-to", "team-a",
                    new ResourceQuota(2, 1, 1), 5, "never"));

            h.scheduler.tick();
            String lease1 = h.jobService.get("job-to").leaseId();
            assertEquals("RUNNING", h.jobService.get("job-to").state());
            assertEquals(1, h.jobService.get("job-to").attempt());
            assertEquals(new ResourceQuota(2, 1, 1), h.jobService.getUsedQuota("team-a"));
            log.info("[test] job-to 首次下发 leaseId={} used=2cpu", lease1);

            // 推进时钟超过 TTL，触发回收
            h.clock.advanceMs(1001);
            var reclaimed = h.leaseManager.reclaimExpired(h.clock.millis());
            assertEquals(java.util.List.of("job-to"), reclaimed);
            assertEquals("PENDING", h.jobService.get("job-to").state());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));
            assertNull(h.jobService.get("job-to").leaseId());
            log.info("[test] 租约超时回收 job-to -> PENDING used=0 attempt仍={} 判定依据=租约过期,配额归还并重新排队",
                    h.jobService.get("job-to").attempt());

            // 迟到回传（旧 leaseId）必须被拒绝：此时任务已被回收为 PENDING，
            // 按校验链先命中"非RUNNING" → ILLEGAL_STATE_TRANSITION，结论不被覆盖。
            JobRejectedException late = assertThrows(JobRejectedException.class, () ->
                    h.jobService.report(new JobResult("job-to", lease1, h.executor.getId(), true, "迟到的成功")));
            assertEquals("ILLEGAL_STATE_TRANSITION", late.getReason().name());
            log.info("[test] 旧租约迟到回传被拒 reason={} 判定依据=任务已回收为PENDING,结论不被覆盖", late.getReason());

            // 重新下发（注意：不推进时钟回退，新租约从当前时钟计算）
            h.scheduler.tick();
            assertEquals("RUNNING", h.jobService.get("job-to").state());
            assertEquals(2, h.jobService.get("job-to").attempt());
            assertNotEquals(lease1, h.jobService.get("job-to").leaseId());
            assertEquals(new ResourceQuota(2, 1, 1), h.jobService.getUsedQuota("team-a"));
            log.info("[test] job-to 第二次下发 attempt=2 新leaseId={} used=2cpu",
                    h.jobService.get("job-to").leaseId());

            // 运行中持"旧租约id"回传：状态是 RUNNING，但租约不匹配 → LEASE_EXPIRED（原因可区分）
            JobRejectedException stale = assertThrows(JobRejectedException.class, () ->
                    h.jobService.report(new JobResult("job-to", lease1, h.executor.getId(), true, "旧租约回传")));
            assertEquals("LEASE_EXPIRED", stale.getReason().name());
            log.info("[test] RUNNING中持旧leaseId回传被拒 reason={} 判定依据=当前租约已更换", stale.getReason());

            // 持错误执行者回传 → LEASE_OWNER_MISMATCH
            JobRejectedException owner = assertThrows(JobRejectedException.class, () ->
                    h.jobService.report(new JobResult("job-to",
                            h.jobService.get("job-to").leaseId(), "other-executor", true, "冒充执行者")));
            assertEquals("LEASE_OWNER_MISMATCH", owner.getReason().name());
            log.info("[test] 非租约持有者回传被拒 reason={}", owner.getReason());
        }
    }

    @Test
    void lostExecutorIsReclaimedImmediately() {
        try (TestHarness h = new TestHarness(1_000_000, 3)) {
            h.limit("team-a", 10, 10, 10);
            h.jobService.submit(new SubmitRequest("job-lost", "team-a",
                    new ResourceQuota(1, 1, 1), 5, "sleep:500"));
            h.scheduler.tick();
            assertEquals("RUNNING", h.jobService.get("job-lost").state());

            boolean reclaimed = h.leaseManager.reclaim(h.jobStore.getRequired("job-lost"), h.clock.millis());
            assertTrue(reclaimed);
            assertEquals("PENDING", h.jobService.get("job-lost").state());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));
            log.info("[test] 执行者失联 job-lost 立即回收 -> PENDING used=0 判定依据=reclaim不依赖TTL到期");
        }
    }
}
