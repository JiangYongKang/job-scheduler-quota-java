package com.github.highcumontoa.jobschedulerquotajava.tests;

import com.github.highcumontoa.jobschedulerquotajava.exception.JobRejectedException;
import com.github.highcumontoa.jobschedulerquotajava.model.Job;
import com.github.highcumontoa.jobschedulerquotajava.model.JobResult;
import com.github.highcumontoa.jobschedulerquotajava.model.ResourceQuota;
import com.github.highcumontoa.jobschedulerquotajava.model.SubmitRequest;
import com.github.highcumontoa.jobschedulerquotajava.tests.support.TestHarness;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景 7：高优先级任务抢占低优先级运行任务。
 *
 * 覆盖：
 * 1) 执行能力（槽位）被占满时，高优先级等待任务立即抢占并占用让出的能力；
 * 2) 主体配额被占满时，抢占过程中的额度交接账实相符（不多一份、不少一份）；
 * 3) 被抢占不等于执行失败：本次尝试退还、不消耗失败重试预算，被抢任务之后正常跑完。
 *
 * 所有用例的关键断言都落在调度线程的同步路径上（tick 返回即确定），
 * 不依赖随机时序，可在同一台机器上反复运行稳定通过。
 */
class PreemptionTest {

    private static final Logger log = LoggerFactory.getLogger(PreemptionTest.class);

    /**
     * 抢占被触发：执行者槽位占满时，高优先级等待任务挤下低优先级运行任务并立即开跑；
     * 被挤任务回到 PENDING、本次尝试退还、配额账实相符（始终只有一份占用）。
     */
    @Test
    void preemptTriggeredAndTakesOverCapacityImmediately() throws Exception {
        try (TestHarness h = new TestHarness(60_000, 3)) {
            h.executor.setParallelism(1);
            h.limit("s", 10, 10, 10);
            h.jobService.submit(new SubmitRequest("low", "s", new ResourceQuota(1, 1, 1), 1, "sleep:3000"));
            h.scheduler.tick();
            assertEquals("RUNNING", h.jobService.get("low").state());
            assertEquals(new ResourceQuota(1, 1, 1), h.jobService.getUsedQuota("s"));

            h.jobService.submit(new SubmitRequest("high", "s", new ResourceQuota(1, 1, 1), 9, "sleep:300"));
            List<String> dispatched = h.scheduler.tick();

            // 判定依据：槽位满 -> 高优先级等待任务按规则抢占，本轮即被下发
            assertTrue(dispatched.contains("high"), "高优先级任务应在抢占本轮即被下发");
            assertEquals("RUNNING", h.jobService.get("high").state());

            // 被挤任务：回到 PENDING、尝试次数退还（1->0）、抢占计数 +1
            Job low = h.jobStore.getRequired("low");
            assertEquals("PENDING", h.jobService.get("low").state());
            assertEquals(0, low.getAttempt(), "被抢占应退还本次尝试");
            assertEquals(1, low.getPreemptedCount());
            assertFalse(low.isQuotaHeld());

            // 账实相符：让出的额度原样交接，占用始终只有一份（high 持有）
            assertEquals(new ResourceQuota(1, 1, 1), h.jobService.getUsedQuota("s"),
                    "抢占交接后占用必须恰好等于抢占者的需求，不多不少");

            h.awaitIdle(8_000);
            assertEquals("SUCCEEDED", h.jobService.get("high").state());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("s"));
            log.info("[test] 抢占触发: high(p=9)挤下low(p=1)立即开跑, low退回PENDING且attempt退还, used始终=1份 判定依据=槽位1:1接管+配额先还后入账");
        }
    }

    /**
     * 配额维度账实相符：主体配额被占满时，高优先级任务抢占同主体低优先级任务，
     * 交接全程占用不超过上限、不凭空多出、不丢失；被抢任务释放后重新排队并最终跑完。
     */
    @Test
    void quotaHandoffIsExactDuringPreemption() throws Exception {
        try (TestHarness h = new TestHarness(60_000, 3)) {
            h.executor.setParallelism(4);
            h.limit("s", 4, 0, 0);
            // low 先跑，占用 cpu=2
            h.jobService.submit(new SubmitRequest("low", "s", new ResourceQuota(2, 0, 0), 1, "sleep:3000"));
            h.scheduler.tick();
            assertEquals(new ResourceQuota(2, 0, 0), h.jobService.getUsedQuota("s"));

            // 两个高优先级任务在配额尚有空间时通过提交准入（used=2, 2+2<=4）
            h.jobService.submit(new SubmitRequest("high-1", "s", new ResourceQuota(2, 0, 0), 9, "sleep:300"));
            h.jobService.submit(new SubmitRequest("high-2", "s", new ResourceQuota(2, 0, 0), 8, "sleep:300"));

            // high-1 正常拿走剩余 2 份；high-2 配额不足 -> 抢占 low
            List<String> dispatched = h.scheduler.tick();
            assertTrue(dispatched.contains("high-1") && dispatched.contains("high-2"));
            assertEquals("RUNNING", h.jobService.get("high-1").state());
            assertEquals("RUNNING", h.jobService.get("high-2").state());

            Job low = h.jobStore.getRequired("low");
            assertEquals("PENDING", h.jobService.get("low").state());
            assertEquals(1, low.getPreemptedCount());
            assertFalse(low.isQuotaHeld());

            // 判定依据：交接后占用 = high-1 + high-2 = 4 = 上限，从未出现 6（重复入账）或丢失
            assertEquals(new ResourceQuota(4, 0, 0), h.jobService.getUsedQuota("s"),
                    "配额交接后占用必须恰好等于两个抢占者之和且不超上限");

            h.awaitIdle(8_000);
            // 两个高优先级任务完成、额度释放后，low 被重新调度并跑完
            h.scheduler.tick();
            assertEquals("RUNNING", h.jobService.get("low").state());
            assertEquals(1, low.getAttempt(), "重新下发时 attempt 从退还后的 0 重新计为 1");
            h.awaitIdle(8_000);
            assertEquals("SUCCEEDED", h.jobService.get("low").state());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("s"),
                    "全部终态后占用必须归零（无泄漏、无重复归还）");
            log.info("[test] 配额交接: low被抢占让出cpu=2, high-2原样接手, 全程used<=4, 终态used=0 判定依据=先还后入账+quotaHeld防重");
        }
    }

    /**
     * 被抢占不等于执行失败：maxAttempts=1 的极端预算下，若抢占消耗重试预算任务必然终结；
     * 实际应退还本次尝试，被抢任务重新排队并最终成功。
     */
    @Test
    void preemptedJobKeepsRetryBudgetAndCompletes() throws Exception {
        try (TestHarness h = new TestHarness(60_000, 1)) {
            h.executor.setParallelism(1);
            h.limit("s", 10, 10, 10);
            h.jobService.submit(new SubmitRequest("low", "s", new ResourceQuota(1, 1, 1), 1, "sleep:2000"));
            h.scheduler.tick();
            assertEquals("RUNNING", h.jobService.get("low").state());
            assertEquals(1, h.jobStore.getRequired("low").getAttempt());

            h.jobService.submit(new SubmitRequest("high", "s", new ResourceQuota(1, 1, 1), 9, "success"));
            h.scheduler.tick();

            // 判定依据：maxAttempts=1，若抢占消耗重试预算 low 必已 FAILED；
            // 实际 PENDING 且 attempt 退还为 0，证明被抢占不吃失败重试机会
            Job low = h.jobStore.getRequired("low");
            assertEquals("PENDING", h.jobService.get("low").state(),
                    "maxAttempts=1 下被抢占若消耗重试预算任务将直接 FAILED");
            assertEquals(0, low.getAttempt());
            assertEquals(1, low.getPreemptedCount());

            h.awaitIdle(8_000);
            assertEquals("SUCCEEDED", h.jobService.get("high").state());

            // 被抢任务重新调度：attempt 重新计为 1（不超预算），正常跑完
            h.scheduler.tick();
            assertEquals("RUNNING", h.jobService.get("low").state());
            assertEquals(1, low.getAttempt());
            h.awaitIdle(8_000);
            assertEquals("SUCCEEDED", h.jobService.get("low").state());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("s"));
            log.info("[test] 重试预算保护: maxAttempts=1, low被抢占后attempt退还为0, 重新下发attempt=1并SUCCEEDED 判定依据=被抢占!=执行失败");
        }
    }

    /**
     * 抢占与取消并发：无论谁先拿到任务锁，结论唯一——
     * 取消一定生效（CANCELLED），旧租约的迟到回传被拒且改不回结论，配额账实相符。
     * 前两个阶段为确定性顺序，第三个阶段为 20 轮并发交错（断言在任何交错下都成立，不靠概率）。
     */
    @Test
    void preemptVsCancelHasUniqueOutcome() throws Exception {
        // 阶段 A：先抢占，再取消被挤下的任务 -> 旧租约回传被拒
        try (TestHarness h = new TestHarness(60_000, 3)) {
            h.executor.setParallelism(1);
            h.limit("s", 10, 10, 10);
            h.jobService.submit(new SubmitRequest("low", "s", new ResourceQuota(1, 1, 1), 1, "sleep:3000"));
            h.scheduler.tick();
            String oldLease = h.jobService.get("low").leaseId();
            h.jobService.submit(new SubmitRequest("high", "s", new ResourceQuota(1, 1, 1), 9, "sleep:100"));
            h.scheduler.tick();
            assertEquals("PENDING", h.jobService.get("low").state());

            h.jobService.cancel("low");
            assertEquals("CANCELLED", h.jobService.get("low").state());
            // 判定依据：被挤下后又被取消，旧租约的迟到回传必须被拒且改不回 CANCELLED
            assertThrows(JobRejectedException.class, () -> h.jobService.report(
                    new JobResult("low", oldLease, h.executor.getId(), true, "迟到回传")));
            assertEquals("CANCELLED", h.jobService.get("low").state());
            h.awaitIdle(8_000);
            assertEquals("SUCCEEDED", h.jobService.get("high").state());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("s"));
            log.info("[test] 抢占vs取消-顺序A: low被抢占后取消->CANCELLED, 旧租约回传被拒 判定依据=任务锁串行化,终态不可改");
        }

        // 阶段 B：先取消，再调度 -> 受害者选择无视终态任务，高优先级正常等待空位
        try (TestHarness h = new TestHarness(60_000, 3)) {
            h.executor.setParallelism(2);
            h.limit("s", 10, 10, 10);
            h.jobService.submit(new SubmitRequest("low", "s", new ResourceQuota(1, 1, 1), 1, "sleep:3000"));
            h.scheduler.tick();
            h.jobService.cancel("low");
            assertEquals("CANCELLED", h.jobService.get("low").state());

            h.jobService.submit(new SubmitRequest("high", "s", new ResourceQuota(1, 1, 1), 9, "sleep:100"));
            h.scheduler.tick();
            // 判定依据：已取消任务不会成为受害者，也不会被重新下发；high 使用空闲槽位正常下发
            assertEquals("CANCELLED", h.jobService.get("low").state());
            assertEquals("RUNNING", h.jobService.get("high").state());
            assertEquals(new ResourceQuota(1, 1, 1), h.jobService.getUsedQuota("s"));
            h.awaitIdle(8_000);
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("s"));
            log.info("[test] 抢占vs取消-顺序B: low先取消, high正常下发, low保持CANCELLED 判定依据=终态不参与受害者选择");
        }

        // 阶段 C：20 轮并发交错——tick(可能触发抢占) 与 cancel 同时打向同一任务。
        // 任何交错下不变量都成立：low 必为 CANCELLED、high 最终 SUCCEEDED、占用归零、无线程异常。
        for (int r = 0; r < 20; r++) {
            try (TestHarness h = new TestHarness(60_000, 3)) {
                h.executor.setParallelism(1);
                h.limit("s", 10, 10, 10);
                String low = "low-" + r;
                String high = "high-" + r;
                h.jobService.submit(new SubmitRequest(low, "s", new ResourceQuota(1, 1, 1), 1, "sleep:400"));
                h.scheduler.tick();
                h.jobService.submit(new SubmitRequest(high, "s", new ResourceQuota(1, 1, 1), 9, "sleep:100"));

                CountDownLatch start = new CountDownLatch(1);
                AtomicReference<Throwable> error = new AtomicReference<>();
                Thread ticker = new Thread(() -> {
                    try {
                        start.await();
                        h.scheduler.tick();
                    } catch (Throwable t) {
                        error.compareAndSet(null, t);
                    }
                });
                Thread canceller = new Thread(() -> {
                    try {
                        start.await();
                        h.jobService.cancel(low);
                    } catch (Throwable t) {
                        error.compareAndSet(null, t);
                    }
                });
                ticker.start();
                canceller.start();
                start.countDown();
                ticker.join();
                canceller.join();
                assertNull(error.get(), "run=" + r + " 并发抢占/取消不应产生异常(含配额不变量)");

                // 判定依据：取消一定生效（RUNNING 时直接取消，或被挤回 PENDING 后取消）
                assertEquals("CANCELLED", h.jobService.get(low).state(), "run=" + r);
                // 收尾：high 最终必须跑完，占用归零
                long guard = System.currentTimeMillis() + 10_000;
                while (System.currentTimeMillis() < guard) {
                    h.awaitIdle(2_000);
                    if ("SUCCEEDED".equals(h.jobService.get(high).state())) {
                        break;
                    }
                    h.scheduler.tick();
                }
                assertEquals("SUCCEEDED", h.jobService.get(high).state(), "run=" + r);
                assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("s"), "run=" + r);
            }
        }
        log.info("[test] 抢占vs取消-并发20轮: 每轮low=CANCELLED且high=SUCCEEDED且used=0 判定依据=任务锁串行化,结论唯一");
    }

    /**
     * 抢占与结果回传 / 租约超时并发：结论唯一。
     * 回传先到则任务 SUCCEEDED（抢占不再发生）；抢占先到则迟到回传被拒、任务重新排队后正常跑完。
     * 租约超时与抢占无论谁先，被挤/被回收只发生一次，配额不重复归还。
     */
    @Test
    void preemptVsReportAndLeaseTimeoutHaveUniqueOutcome() throws Exception {
        // 阶段 A：回传先到 -> SUCCEEDED 终态，抢占不再发生
        try (TestHarness h = new TestHarness(60_000, 3)) {
            h.executor.setParallelism(1);
            h.limit("s", 10, 10, 10);
            h.jobService.submit(new SubmitRequest("low", "s", new ResourceQuota(1, 1, 1), 1, "sleep:400"));
            h.scheduler.tick();
            String lease = h.jobService.get("low").leaseId();
            h.jobService.report(new JobResult("low", lease, h.executor.getId(), true, "回传先到"));
            assertEquals("SUCCEEDED", h.jobService.get("low").state());

            h.jobService.submit(new SubmitRequest("high", "s", new ResourceQuota(1, 1, 1), 9, "sleep:100"));
            h.scheduler.tick();
            // 判定依据：终态任务不会成为受害者；high 等执行者槽位释放后正常下发
            assertEquals("SUCCEEDED", h.jobService.get("low").state());
            assertEquals("PENDING", h.jobService.get("high").state());
            h.awaitIdle(8_000);
            h.scheduler.tick();
            h.awaitIdle(8_000);
            assertEquals("SUCCEEDED", h.jobService.get("high").state());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("s"));
            log.info("[test] 抢占vs回传-顺序A: 回传先到low=SUCCEEDED, high等待空位后完成 判定依据=终态不参与受害者选择");
        }

        // 阶段 B：抢占先到 -> 迟到回传被拒，low 重新排队后正常跑完
        try (TestHarness h = new TestHarness(60_000, 3)) {
            h.executor.setParallelism(1);
            h.limit("s", 10, 10, 10);
            h.jobService.submit(new SubmitRequest("low", "s", new ResourceQuota(1, 1, 1), 1, "sleep:400"));
            h.scheduler.tick();
            String oldLease = h.jobService.get("low").leaseId();
            h.jobService.submit(new SubmitRequest("high", "s", new ResourceQuota(1, 1, 1), 9, "sleep:100"));
            h.scheduler.tick();
            assertEquals("PENDING", h.jobService.get("low").state());

            // 判定依据：旧租约的迟到回传必须被拒，且改不回"已被挤下"的结论
            assertThrows(JobRejectedException.class, () -> h.jobService.report(
                    new JobResult("low", oldLease, h.executor.getId(), true, "迟到回传")));
            assertEquals("PENDING", h.jobService.get("low").state());

            h.awaitIdle(8_000);
            assertEquals("SUCCEEDED", h.jobService.get("high").state());
            h.scheduler.tick();
            assertEquals("RUNNING", h.jobService.get("low").state());
            h.awaitIdle(8_000);
            assertEquals("SUCCEEDED", h.jobService.get("low").state());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("s"));
            log.info("[test] 抢占vs回传-顺序B: 抢占先到, 旧租约回传被拒, low重排后SUCCEEDED 判定依据=租约id校验+任务锁串行化");
        }

        // 阶段 C：租约超时先回收 -> 抢占不再发生（任务已 PENDING），回收只生效一次
        try (TestHarness h = new TestHarness(500, 3)) {
            h.executor.setParallelism(2);
            h.limit("s", 10, 10, 10);
            h.jobService.submit(new SubmitRequest("low", "s", new ResourceQuota(1, 1, 1), 1, "never"));
            h.scheduler.tick();
            h.clock.advanceMs(600);
            h.leaseManager.reclaimExpired(h.clock.millis());
            // 判定依据：超时回收消耗一次尝试（不退还），任务回 PENDING
            assertEquals("PENDING", h.jobService.get("low").state());
            assertEquals(1, h.jobStore.getRequired("low").getAttempt());
            assertEquals(0, h.jobStore.getRequired("low").getPreemptedCount());

            h.jobService.submit(new SubmitRequest("high", "s", new ResourceQuota(1, 1, 1), 9, "sleep:100"));
            h.scheduler.tick();
            assertEquals("RUNNING", h.jobService.get("high").state());
            assertEquals(0, h.jobStore.getRequired("low").getPreemptedCount(), "已回收任务不应再被抢占");
            awaitState(h, "high", "SUCCEEDED", 8_000);
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("s"));
            log.info("[test] 抢占vs超时-顺序C: 超时先回收(attempt=1), high正常下发, low未被抢占 判定依据=回收与抢占互斥");
        }

        // 阶段 D：抢占先到 -> 之后的租约超时回收是 no-op，旧租约回传被拒
        try (TestHarness h = new TestHarness(500, 3)) {
            h.executor.setParallelism(1);
            h.limit("s", 10, 10, 10);
            h.jobService.submit(new SubmitRequest("low", "s", new ResourceQuota(1, 1, 1), 1, "never"));
            h.scheduler.tick();
            String oldLease = h.jobService.get("low").leaseId();
            h.jobService.submit(new SubmitRequest("high", "s", new ResourceQuota(1, 1, 1), 9, "sleep:100"));
            h.scheduler.tick();
            assertEquals("PENDING", h.jobService.get("low").state());
            assertEquals(0, h.jobStore.getRequired("low").getAttempt(), "被抢占退还本次尝试");

            // 判定依据：抢占后 low 的租约已清，超时回收对它不再生效（不会二次归还配额）。
            // 先等 high 自然跑完（其自身租约了结），再推进时钟越过 low 旧租约的到期点。
            awaitState(h, "high", "SUCCEEDED", 8_000);
            h.clock.advanceMs(600);
            assertTrue(h.leaseManager.reclaimExpired(h.clock.millis()).isEmpty(),
                    "low 已被抢占回到 PENDING, 超时回收不应再命中任何任务");
            assertEquals("PENDING", h.jobService.get("low").state());
            assertThrows(JobRejectedException.class, () -> h.jobService.report(
                    new JobResult("low", oldLease, h.executor.getId(), true, "迟到回传")));
            assertEquals(new ResourceQuota(0, 0, 0), h.jobService.getUsedQuota("s"));
            log.info("[test] 抢占vs超时-顺序D: 抢占先到, 超时回收no-op, 旧租约回传被拒, used=0 判定依据=quotaHeld防重复归还");
        }
    }

    /**
     * 防饿死：同一低优先级任务被连续抢占达到上限（默认 3 次）后获得免疫，
     * 之后的高优先级任务只能等它跑完；被抢任务最终到达终态。
     */
    @Test
    void repeatedPreemptionEventuallyGrantsImmunityAndCompletes() throws Exception {
        try (TestHarness h = new TestHarness(60_000, 3)) {
            h.executor.setParallelism(1);
            h.props.setMaxPreemptionsPerJob(3);
            h.limit("s", 10, 10, 10);
            h.jobService.submit(new SubmitRequest("low", "s", new ResourceQuota(1, 1, 1), 1, "sleep:300"));
            h.scheduler.tick();
            Job low = h.jobStore.getRequired("low");

            // 连续 3 次被更高优先级任务抢占：每次都退还尝试、重新排队
            for (int i = 1; i <= 3; i++) {
                h.jobService.submit(new SubmitRequest("high-" + i, "s", new ResourceQuota(1, 1, 1), 9, "success"));
                h.scheduler.tick();
                assertEquals("PENDING", h.jobService.get("low").state(), "第" + i + "次被抢占");
                assertEquals(i, low.getPreemptedCount());
                assertEquals(0, low.getAttempt(), "第" + i + "次被抢占后尝试次数应退还");
                h.awaitIdle(8_000);
                assertEquals("SUCCEEDED", h.jobService.get("high-" + i).state());
                if (i < 3) {
                    h.scheduler.tick();
                    assertEquals("RUNNING", h.jobService.get("low").state());
                }
            }

            // 第 3 次被抢后重新开跑；此时已免疫，新的高优先级任务抢不动它
            h.scheduler.tick();
            assertEquals("RUNNING", h.jobService.get("low").state());
            h.jobService.submit(new SubmitRequest("high-4", "s", new ResourceQuota(1, 1, 1), 9, "success"));
            h.scheduler.tick();
            // 判定依据：达到抢占免疫上限后不再被选为受害者，高优先级任务只能等待
            assertEquals("PENDING", h.jobService.get("high-4").state(), "low 已免疫, high-4 不应抢占成功");
            assertEquals("RUNNING", h.jobService.get("low").state());
            assertEquals(3, low.getPreemptedCount());

            // low 跑完后 high-4 才得以下发：低优先级任务最终到达终态，不被饿死
            h.awaitIdle(8_000);
            assertEquals("SUCCEEDED", h.jobService.get("low").state());
            assertEquals(1, low.getAttempt(), "被连续抢占不消耗尝试预算, 最终第1次尝试即成功");
            h.scheduler.tick();
            h.awaitIdle(8_000);
            assertEquals("SUCCEEDED", h.jobService.get("high-4").state());
            assertEquals(ResourceQuota.ZERO, h.jobService.getUsedQuota("s"));
            log.info("[test] 防饿死: low连续被抢3次后免疫, high-4等待其完成后才下发, low最终SUCCEEDED(attempt=1) 判定依据=抢占免疫上限");
        }
    }

    /**
     * 受害者选择确定性：优先级最低者优先；并列时租约发放最晚者优先；再并列按 jobId 升序。
     */
    @Test
    void victimSelectionIsDeterministic() {
        try (TestHarness h = new TestHarness(60_000, 3)) {
            h.executor.setParallelism(4);
            h.limit("s", 10, 10, 10);
            h.jobService.submit(new SubmitRequest("z-low", "s", new ResourceQuota(1, 1, 1), 0, "sleep:2000"));
            h.scheduler.tick();
            h.clock.advanceMs(50);
            h.jobService.submit(new SubmitRequest("b-low", "s", new ResourceQuota(1, 1, 1), 0, "sleep:2000"));
            h.jobService.submit(new SubmitRequest("a-low", "s", new ResourceQuota(1, 1, 1), 0, "sleep:2000"));
            h.scheduler.tick();
            h.clock.advanceMs(50);
            // 优先级更高(p=1)但租约最新：若规则不是"优先级最低优先"，它会被错选
            h.jobService.submit(new SubmitRequest("zz-mid", "s", new ResourceQuota(1, 1, 1), 1, "sleep:2000"));
            h.scheduler.tick();

            h.jobService.submit(new SubmitRequest("urgent", "s", new ResourceQuota(1, 1, 1), 9, "sleep:100"));
            h.scheduler.tick();

            // 判定依据：p=0 最低 -> 其中租约最晚(t1)的是 a-low/b-low -> jobId 升序取 a-low
            assertEquals("PENDING", h.jobService.get("a-low").state(), "应选中 a-low(最低优先级+租约最新+jobId最小)");
            assertEquals(1, h.jobStore.getRequired("a-low").getPreemptedCount());
            assertEquals("RUNNING", h.jobService.get("z-low").state());
            assertEquals("RUNNING", h.jobService.get("b-low").state());
            assertEquals("RUNNING", h.jobService.get("zz-mid").state());
            assertEquals("RUNNING", h.jobService.get("urgent").state());
            log.info("[test] 受害者选择: 候选z-low(p0,t0)/b-low(p0,t1)/a-low(p0,t1)/zz-mid(p1,t2) -> 选中a-low 判定依据=优先级最低→租约最新→jobId最小");
        }
    }

    /**
     * 兼容性：等待任务优先级不高于运行任务时，抢占完全不触发，调度行为与引入抢占前一致。
     */
    @Test
    void noPreemptionWhenPriorityNotHigher() {
        try (TestHarness h = new TestHarness(60_000, 3)) {
            h.executor.setParallelism(1);
            h.limit("s", 10, 10, 10);
            h.jobService.submit(new SubmitRequest("running", "s", new ResourceQuota(1, 1, 1), 5, "sleep:2000"));
            h.scheduler.tick();

            h.jobService.submit(new SubmitRequest("equal", "s", new ResourceQuota(1, 1, 1), 5, "success"));
            h.jobService.submit(new SubmitRequest("lower", "s", new ResourceQuota(1, 1, 1), 4, "success"));
            h.scheduler.tick();

            // 判定依据：优先级相等或更低的等待任务不允许抢占，只能排队等待
            assertEquals("RUNNING", h.jobService.get("running").state());
            assertEquals("PENDING", h.jobService.get("equal").state());
            assertEquals("PENDING", h.jobService.get("lower").state());
            assertEquals(0, h.jobStore.getRequired("running").getPreemptedCount());
            assertEquals(new ResourceQuota(1, 1, 1), h.jobService.getUsedQuota("s"));
            log.info("[test] 兼容: equal(p=5)/lower(p=4)等待时不触发抢占, running(p=5)继续跑 判定依据=严格更高优先级才允许抢占");
        }
    }

    /** 轮询等待任务到达指定状态（用于工作线程按预期长期占槽的场景，不能等槽位空闲）。 */
    private static void awaitState(TestHarness h, String jobId, String state, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (state.equals(h.jobService.get(jobId).state())) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        assertEquals(state, h.jobService.get(jobId).state(), "任务未在超时内到达状态 " + state);
    }
}
