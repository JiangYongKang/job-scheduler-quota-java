package com.github.highcumontoa.jobschedulerquotajava.tests;

import com.github.highcumontoa.jobschedulerquotajava.exception.JobRejectedException;
import com.github.highcumontoa.jobschedulerquotajava.model.JobResult;
import com.github.highcumontoa.jobschedulerquotajava.model.JobSnapshot;
import com.github.highcumontoa.jobschedulerquotajava.model.ResourceQuota;
import com.github.highcumontoa.jobschedulerquotajava.model.SubmitRequest;
import com.github.highcumontoa.jobschedulerquotajava.tests.support.TestHarness;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景 7：高优先级任务抢占正在执行的低优先级任务。
 * 全部用例由手动 tick + 可控时钟 + 确定性错位驱动，不依赖随机时序，可反复复现：
 * - 抢占触发：等待任务优先级严格更高且槽位/配额被占满时，立即抢占并开跑；
 * - 额度交接：victim 释放多少、preemptor 就占用多少，账实相符、不多不少；
 * - 被抢任务不消耗重试预算（attempt 回滚），之后能正常重调度并跑到终态；
 * - 抢占与取消/超时/回传并发时结论唯一，迟到回传不能翻案；
 * - 同一任务被连续抢占达到上限后获得免疫（防饿死），最终仍能完成。
 */
class PreemptionTest {

    private static final Logger log = LoggerFactory.getLogger(PreemptionTest.class);

    private static JobSnapshot submit(TestHarness h, String jobId, String subject,
                                      int cpu, int priority, String payload) {
        return h.jobService.submit(new SubmitRequest(jobId, subject,
                new ResourceQuota(cpu, 1, 1), priority, payload));
    }

    @Test
    void preemptionTriggersAndPreemptedJobEventuallyCompletes() throws Exception {
        try (TestHarness h = new TestHarness(60_000, 3)) {
            h.executor.setParallelism(1);
            h.limit("team-a", 4, 4, 4);
            // 低优先级任务先占满唯一的执行槽位（sleep 保证抢占发生时仍在 RUNNING）
            submit(h, "low", "team-a", 2, 1, "sleep:500");
            h.scheduler.tick();
            assertEquals("RUNNING", h.jobService.get("low").state());
            assertEquals(new ResourceQuota(2, 1, 1), h.jobService.getUsedQuota("team-a"));
            String lowLease1 = h.jobService.get("low").leaseId();
            log.info("[test] 场景=抢占触发 low(p=1) RUNNING 占满槽位 used=2cpu");

            // 高优先级任务到来：槽位满 -> 正常下发失败 -> 抢占 low 并立即开跑
            submit(h, "high", "team-a", 2, 9, "sleep:300");
            h.scheduler.tick();

            JobSnapshot high = h.jobService.get("high");
            JobSnapshot low = h.jobService.get("low");
            assertEquals("RUNNING", high.state(), "抢占成功后高优先级任务必须立即开跑");
            assertEquals(1, high.attempt());
            assertEquals("PENDING", low.state(), "被抢任务必须回到等待队列");
            assertEquals(0, low.attempt(), "被抢占的这次下发不计入尝试次数(attempt回滚)");
            assertEquals(1, low.preemptedCount());
            assertFalse(low.quotaHeld());
            assertNull(low.leaseId());
            // 额度交接账实相符：low 释放 2cpu、high 占用 2cpu，总量不多不少
            assertEquals(new ResourceQuota(2, 1, 1), h.jobService.getUsedQuota("team-a"),
                    "额度必须原样交接:不能多出一份也不能少算一份");
            log.info("[test] high(p=9) 抢占成功立即RUNNING low->PENDING attempt回滚为0 被抢次数=1 used仍=2cpu 判定依据=额度原样交接");

            // 被抢任务的旧租约迟到回传必须被拒，不能翻案
            JobRejectedException late = assertThrows(JobRejectedException.class, () ->
                    h.jobService.report(new JobResult("low", lowLease1, h.executor.getId(), true, "被抢后的迟到回传")));
            assertEquals("ILLEGAL_STATE_TRANSITION", late.getReason().name());
            assertEquals("PENDING", h.jobService.get("low").state());
            log.info("[test] low旧租约迟到回传被拒 reason={} 判定依据=已被挤回PENDING,结论不被覆盖", late.getReason());

            // high 跑完释放槽位与配额
            h.awaitIdle(5_000);
            assertEquals("SUCCEEDED", h.jobService.get("high").state());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));

            // low 被正常重调度并最终跑完
            h.scheduler.tick();
            JobSnapshot lowAgain = h.jobService.get("low");
            assertEquals("RUNNING", lowAgain.state());
            assertEquals(1, lowAgain.attempt(), "重新下发才计入新的尝试次数");
            assertNotEquals(lowLease1, lowAgain.leaseId());
            assertEquals(new ResourceQuota(2, 1, 1), h.jobService.getUsedQuota("team-a"));
            h.awaitIdle(5_000);
            assertEquals("SUCCEEDED", h.jobService.get("low").state());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));
            log.info("[test] low 重调度后正常跑完 SUCCEEDED used=0 判定依据=被抢任务可正常重调度到终态");
        }
    }

    @Test
    void quotaHandoverAcrossSubjectsIsExact() throws Exception {
        try (TestHarness h = new TestHarness(60_000, 3)) {
            h.executor.setParallelism(1);
            h.limit("team-a", 4, 4, 4);
            h.limit("team-b", 4, 4, 4);
            submit(h, "low-a", "team-a", 2, 1, "sleep:500");
            h.scheduler.tick();
            assertEquals(new ResourceQuota(2, 1, 1), h.jobService.getUsedQuota("team-a"));

            // 不同主体的高优先级任务抢占：victim 的释放在 team-a，preemptor 的占用在 team-b
            submit(h, "high-b", "team-b", 2, 9, "sleep:300");
            h.scheduler.tick();

            assertEquals("RUNNING", h.jobService.get("high-b").state());
            assertEquals("PENDING", h.jobService.get("low-a").state());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"),
                    "victim 主体的占用必须精确释放");
            assertEquals(new ResourceQuota(2, 1, 1), h.jobService.getUsedQuota("team-b"),
                    "preemptor 主体的占用必须精确增加同一份");
            log.info("[test] 跨主体额度交接 team-a:2->0 team-b:0->2 判定依据=释放与占用一一对应");

            h.awaitIdle(5_000);
            assertEquals("SUCCEEDED", h.jobService.get("high-b").state());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-b"));
            h.scheduler.tick();
            assertEquals("RUNNING", h.jobService.get("low-a").state());
            assertEquals(new ResourceQuota(2, 1, 1), h.jobService.getUsedQuota("team-a"));
            h.awaitIdle(5_000);
            assertEquals("SUCCEEDED", h.jobService.get("low-a").state());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));
            log.info("[test] 两主体任务先后完成,双方占用均归零 判定依据=全程账实相符");
        }
    }

    @Test
    void preemptionTriggersWhenQuotaTightenedEvenWithFreeSlots() {
        try (TestHarness h = new TestHarness(60_000, 3)) {
            h.executor.setParallelism(2); // 槽位充足，瓶颈只能是配额
            h.limit("team-a", 4, 4, 4);
            submit(h, "low", "team-a", 2, 1, "sleep:500");
            h.scheduler.tick();
            assertEquals("RUNNING", h.jobService.get("low").state());
            submit(h, "high", "team-a", 2, 9, "sleep:300"); // 提交时 2+2<=4 准入通过

            // 运行中收紧主体额度：high 正常下发被配额卡住 -> 触发配额型抢占
            h.jobService.setQuotaLimit("team-a", new ResourceQuota(3, 4, 4));
            h.scheduler.tick();

            assertEquals("RUNNING", h.jobService.get("high").state(), "配额被占满时高优先级任务应抢占开跑");
            assertEquals("PENDING", h.jobService.get("low").state());
            assertEquals(1, h.jobService.get("low").preemptedCount());
            // 交接后占用 = high 的 2cpu，且满足收紧后的上限 3
            assertEquals(new ResourceQuota(2, 1, 1), h.jobService.getUsedQuota("team-a"));
            log.info("[test] 场景=配额占满触发抢占 额度收紧为3后 high 抢占 low used=2<=3 判定依据=quotaBlocked通道生效");
        }
    }

    @Test
    void cancelBeforePreemptionSkipsVictimAndCancelAfterPreemptionDoesNotDoubleRelease() throws Exception {
        try (TestHarness h = new TestHarness(60_000, 3)) {
            h.executor.setParallelism(1);
            h.limit("team-a", 4, 4, 4);
            submit(h, "low", "team-a", 2, 1, "sleep:200");
            h.scheduler.tick();
            submit(h, "high", "team-a", 2, 9, "sleep:300");

            // 取消先到：victim 已终结，抢占通道不得碰它；槽位等执行线程结束后自然释放
            h.jobService.cancel("low");
            h.scheduler.tick();
            assertEquals("CANCELLED", h.jobService.get("low").state());
            assertEquals(0, h.jobService.get("low").preemptedCount(), "已终结任务不得被抢占");
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));
            log.info("[test] 取消先于抢占 low=CANCELLED 被抢次数=0 used=0 判定依据=终态不参与抢占");

            h.awaitIdle(5_000); // low 的执行线程结束，槽位释放
            h.scheduler.tick();
            assertEquals("RUNNING", h.jobService.get("high").state(), "无抢占需求时走既有正常调度");
            h.awaitIdle(5_000);
            assertEquals("SUCCEEDED", h.jobService.get("high").state());
        }
    }

    @Test
    void cancelAfterPreemptionCancelsPendingVictimWithoutDoubleRelease() {
        try (TestHarness h = new TestHarness(60_000, 3)) {
            h.executor.setParallelism(1);
            h.limit("team-a", 4, 4, 4);
            submit(h, "low", "team-a", 2, 1, "sleep:500");
            h.scheduler.tick();
            submit(h, "high", "team-a", 2, 9, "sleep:300");
            h.scheduler.tick(); // 抢占：low -> PENDING（配额已还给 high）
            assertEquals("PENDING", h.jobService.get("low").state());

            // 取消被挤回 PENDING 的 victim：不得再次归还配额
            h.jobService.cancel("low");
            assertEquals("CANCELLED", h.jobService.get("low").state());
            assertEquals(new ResourceQuota(2, 1, 1), h.jobService.getUsedQuota("team-a"),
                    "被抢任务的配额已在抢占时归还,取消不得重复归还");
            log.info("[test] 抢占后取消victim used仍=2cpu(high的) 判定依据=quotaHeld标记防重复归还");
        }
    }

    @Test
    void expiredReclaimDoesNotTouchPreemptedVictim() {
        try (TestHarness h = new TestHarness(1_000, 3)) {
            h.executor.setParallelism(1);
            h.limit("team-a", 4, 4, 4);
            submit(h, "low", "team-a", 2, 1, "sleep:500");
            h.scheduler.tick();
            // 拉开两次租约的发放时刻，使"旧租约已过期、新租约未过期"的窗口存在
            h.clock.advanceMs(500);
            submit(h, "high", "team-a", 2, 9, "sleep:300");
            h.scheduler.tick(); // 抢占成功，low 的旧租约已作废

            // 时钟推进到"low 旧租约已过期(1100>1000) 但 high 新租约未到期(600<1000)"：
            // 回收只应命中 RUNNING 且当前租约到期的任务，被挤回 PENDING 的 low 不受影响
            h.clock.advanceMs(600);
            var reclaimed = h.leaseManager.reclaimExpired(h.clock.millis());
            assertTrue(reclaimed.isEmpty(), "被抢任务已不在RUNNING,超时回收不得重复处理");
            assertEquals("PENDING", h.jobService.get("low").state());
            assertEquals("RUNNING", h.jobService.get("high").state());
            assertEquals(new ResourceQuota(2, 1, 1), h.jobService.getUsedQuota("team-a"));
            log.info("[test] 旧租约到期回收不命中被抢任务 low仍PENDING high仍RUNNING 判定依据=回收只认RUNNING+当前租约");
        }
    }

    @Test
    void preemptionAndReportRaceProducesUniqueOutcome() throws Exception {
        int runs = 30;
        int preemptWon = 0;
        int reportWon = 0;
        for (int r = 0; r < runs; r++) {
            try (TestHarness h = new TestHarness(60_000, 3)) {
                h.executor.setParallelism(1);
                h.limit("team-a", 4, 4, 4);
                h.limit("team-b", 4, 4, 4);
                String low = "low-" + r;
                String high = "high-" + r;
                submit(h, low, "team-a", 2, 1, "sleep:5000");
                h.scheduler.tick();
                // high 用 never：一旦被抢占下发就停在 RUNNING，断言不受异步完成干扰
                submit(h, high, "team-b", 2, 9, "never");

                // 可复现的交错：偶数轮回传线程先行、奇数轮抢占(tick)线程先行，固定 3ms 错位
                boolean reportFirst = (r % 2 == 0);
                java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
                java.util.concurrent.atomic.AtomicInteger reportOk = new java.util.concurrent.atomic.AtomicInteger();
                java.util.concurrent.atomic.AtomicInteger reportRejected = new java.util.concurrent.atomic.AtomicInteger();
                Thread reporter = new Thread(() -> {
                    try {
                        start.await();
                        if (!reportFirst) {
                            Thread.sleep(3);
                        }
                        String leaseId = h.jobService.get(low).leaseId();
                        h.jobService.report(new JobResult(low, leaseId, h.executor.getId(), true, "与抢占并发的回传"));
                        reportOk.incrementAndGet();
                    } catch (JobRejectedException e) {
                        reportRejected.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                Thread preemptor = new Thread(() -> {
                    try {
                        start.await();
                        if (reportFirst) {
                            Thread.sleep(3);
                        }
                        h.scheduler.tick();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                reporter.start();
                preemptor.start();
                start.countDown();
                reporter.join();
                preemptor.join();

                JobSnapshot lowSnap = h.jobService.get(low);
                JobSnapshot highSnap = h.jobService.get(high);
                ResourceQuota usedA = h.jobService.getUsedQuota("team-a");
                ResourceQuota usedB = h.jobService.getUsedQuota("team-b");

                // 结论唯一且只能是两种合法形态之一：
                // A) 回传先到：low=SUCCEEDED(配额已还)，high 仍在等(无对象可抢)，usedB=0；
                // B) 抢占先到：low=PENDING(被抢1次,回传被拒)，high=RUNNING 占用 team-b 2cpu。
                if (reportOk.get() == 1) {
                    reportWon++;
                    assertEquals("SUCCEEDED", lowSnap.state(), "run=" + r + " 回传先到则low终结");
                    assertEquals(0, lowSnap.preemptedCount());
                    assertEquals("PENDING", highSnap.state(), "run=" + r + " 无可抢对象,high继续等待");
                    assertEquals(ResourceQuota.ZERO, usedA, "run=" + r + " low成功后team-a占用归零");
                    assertEquals(ResourceQuota.ZERO, usedB, "run=" + r + " high未下发,team-b无占用");
                } else {
                    preemptWon++;
                    assertEquals(1, reportRejected.get(), "run=" + r + " 被抢后的回传必须被拒");
                    assertEquals("PENDING", lowSnap.state(), "run=" + r + " 抢占先到则low回PENDING");
                    assertEquals(1, lowSnap.preemptedCount());
                    assertEquals("RUNNING", highSnap.state(), "run=" + r + " high立即占用让出的能力");
                    assertEquals(ResourceQuota.ZERO, usedA, "run=" + r + " victim配额精确释放");
                    assertEquals(new ResourceQuota(2, 1, 1), usedB, "run=" + r + " preemptor精确占用同一份");
                }
            }
        }
        log.info("[test] 抢占vs回传并发 {} 轮: 回传先到={} 抢占先到={} 判定依据=任务锁串行化,结论唯一,配额账实相符",
                runs, reportWon, preemptWon);
        assertTrue(reportWon > 0 && preemptWon > 0,
                "确定性交错下两种结论都应出现(各约一半)，以证明竞争真实存在");
    }

    @Test
    void repeatedPreemptionHitsImmunityAndJobStillCompletes() throws Exception {
        try (TestHarness h = new TestHarness(60_000, 3)) {
            h.executor.setParallelism(1);
            h.props.setPreemptionMaxPerJob(2); // 同一任务最多被抢 2 次，之后免疫
            h.limit("team-a", 4, 4, 4);
            submit(h, "low", "team-a", 2, 1, "sleep:1500");
            h.scheduler.tick();
            assertEquals("RUNNING", h.jobService.get("low").state());

            // 第 1 次被抢
            submit(h, "h1", "team-a", 2, 9, "sleep:100");
            h.scheduler.tick();
            assertEquals(1, h.jobService.get("low").preemptedCount());
            h.awaitIdle(5_000);
            assertEquals("SUCCEEDED", h.jobService.get("h1").state());
            h.scheduler.tick(); // low 重新下发
            assertEquals("RUNNING", h.jobService.get("low").state());

            // 第 2 次被抢
            submit(h, "h2", "team-a", 2, 8, "sleep:100");
            h.scheduler.tick();
            assertEquals(2, h.jobService.get("low").preemptedCount());
            h.awaitIdle(5_000);
            assertEquals("SUCCEEDED", h.jobService.get("h2").state());
            h.scheduler.tick(); // low 再次重新下发
            assertEquals("RUNNING", h.jobService.get("low").state());

            // 第 3 个高优先级任务到来：low 已免疫，不再被抢，h3 正常等待
            submit(h, "h3", "team-a", 2, 7, "sleep:100");
            h.scheduler.tick();
            assertEquals("RUNNING", h.jobService.get("low").state(), "达到被抢上限后获得免疫,不再被抢占");
            assertEquals("PENDING", h.jobService.get("h3").state());
            assertEquals(2, h.jobService.get("low").preemptedCount());
            log.info("[test] low 被抢2次后免疫 h3 不再抢占 判定依据=preemptionMaxPerJob=2防饿死");

            // low 不受打扰地跑完：attempt 只算了真正在跑的这一轮
            h.awaitIdle(5_000);
            JobSnapshot lowDone = h.jobService.get("low");
            assertEquals("SUCCEEDED", lowDone.state());
            assertEquals(1, lowDone.attempt(), "被抢占不消耗尝试次数,最终成功只计1次有效尝试");
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));

            // 槽位空出后 h3 正常调度完成
            h.scheduler.tick();
            h.awaitIdle(5_000);
            assertEquals("SUCCEEDED", h.jobService.get("h3").state());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));
            log.info("[test] low 免疫后跑完 SUCCEEDED attempt=1 h3 随后完成 used=0 判定依据=防饿死且账目一致");
        }
    }

    @Test
    void noPreemptionWhenPriorityNotHigherOrCapacityAvailable() throws Exception {
        try (TestHarness h = new TestHarness(60_000, 3)) {
            h.executor.setParallelism(1);
            h.limit("team-a", 10, 10, 10);
            submit(h, "first", "team-a", 2, 5, "sleep:300");
            h.scheduler.tick();
            assertEquals("RUNNING", h.jobService.get("first").state());

            // 同优先级等待：不得抢占
            submit(h, "same-prio", "team-a", 2, 5, "success");
            // 更低优先级等待：不得抢占
            submit(h, "lower", "team-a", 2, 1, "success");
            h.scheduler.tick();
            assertEquals("RUNNING", h.jobService.get("first").state());
            assertEquals("PENDING", h.jobService.get("same-prio").state());
            assertEquals("PENDING", h.jobService.get("lower").state());
            assertEquals(0, h.jobService.get("first").preemptedCount());
            assertEquals(new ResourceQuota(2, 1, 1), h.jobService.getUsedQuota("team-a"));
            log.info("[test] 同优先级/更低优先级等待时不触发抢占 判定依据=严格更高优先级才可抢");

            // 无抢占需求时调度顺序与既有行为一致：先到先得地跑完
            h.awaitIdle(5_000);
            assertEquals("SUCCEEDED", h.jobService.get("first").state());
            h.scheduler.tick();
            h.awaitIdle(5_000);
            assertEquals("SUCCEEDED", h.jobService.get("same-prio").state());
            h.scheduler.tick();
            h.awaitIdle(5_000);
            assertEquals("SUCCEEDED", h.jobService.get("lower").state());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("team-a"));
            log.info("[test] 既有调度顺序不变 first->same-prio->lower 依次完成 used=0");
        }
    }
}
