package com.github.highcumontoa.jobschedulerquotajava.tests;

import com.github.highcumontoa.jobschedulerquotajava.model.ResourceQuota;
import com.github.highcumontoa.jobschedulerquotajava.model.SubmitRequest;
import com.github.highcumontoa.jobschedulerquotajava.tests.support.TestHarness;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 调度挑选的可解释性：
 * 1) 优先级高的先下发；
 * 2) 同优先级时，历史下发更少的主体先被服务（公平性）；
 * 3) 再相同则提交时间早者优先。
 */
class SchedulingOrderTest {

    private static final Logger log = LoggerFactory.getLogger(SchedulingOrderTest.class);

    private SubmitRequest req(String id, String subject, int prio, String payload) {
        return new SubmitRequest(id, subject, new ResourceQuota(1, 1, 1), prio, payload);
    }

    @Test
    void priorityThenFairnessThenFifo() throws Exception {
        try (TestHarness h = new TestHarness(60_000, 3)) {
            // 把执行者槽位限制为 1，使一轮 tick 只能下发 1 个，顺序可观测
            h.executor.setParallelism(1);
            h.limit("a", 100, 100, 100);
            h.limit("b", 100, 100, 100);

            // low: 优先级 1；high-a/high-b: 优先级 9（a、b 两个主体，历史下发均为 0）
            // sleep:400 让任务在断言窗口内占着唯一槽位
            h.jobService.submit(req("low", "a", 1, "sleep:400"));
            h.jobService.submit(req("high-b", "b", 9, "sleep:400"));
            h.jobService.submit(req("high-a", "a", 9, "sleep:400"));

            // 仅排序，不全量下发，检查候选顺序：
            // high-a/high-b 同优先级、同主体历史计数(0)、且 ManualClock 下 createdAt 相同，
            // 故由 jobId 升序兜底（high-a < high-b），保证排序完全确定可复现。
            List<?> candidates = h.scheduler.selectCandidates(
                    new java.util.ArrayList<>(h.jobStore.all()), h.clock.millis());
            assertEquals(List.of("high-a", "high-b", "low"),
                    candidates.stream().map(j -> ((com.github.highcumontoa.jobschedulerquotajava.model.Job) j).getJobId()).toList());
            log.info("[test] 候选顺序 high-a,high-b,low 判定依据=优先级9>1;同级同计数同提交时间时按jobId兜底,完全确定");

            // 逐一下发以观察公平性：唯一槽位每轮只能放一个，sleep:400 保证断言时仍被占用
            assertEquals(List.of("high-a"), h.scheduler.tick());
            assertEquals("RUNNING", h.jobService.get("high-a").state());
            // 唯一执行者槽位被占，本轮无任务可下发
            assertEquals(List.of(), h.scheduler.tick());

            // 等槽位释放后，下一轮应下发 high-b(prio9) 而非 low(prio1)
            h.awaitIdle(3000);
            assertEquals(List.of("high-b"), h.scheduler.tick());
            h.awaitIdle(3000);
            assertEquals(List.of("low"), h.scheduler.tick());
            h.awaitIdle(3000);
            log.info("[test] 逐轮下发顺序 high-a -> high-b -> low 与排序解释一致");
        }
    }

    @Test
    void fairnessFavorsLessServedSubjectAtSamePriority() {
        try (TestHarness h = new TestHarness(60_000, 3)) {
            h.executor.setParallelism(1);
            h.limit("served", 100, 100, 100);
            h.limit("fresh", 100, 100, 100);

            // 预置历史：主体 served 历史下发 2 次，fresh 为 0
            h.fairnessTracker.recordDispatch("served");
            h.fairnessTracker.recordDispatch("served");

            h.jobService.submit(req("new-1", "fresh", 5, "sleep:100"));
            h.jobService.submit(req("old-3", "served", 5, "sleep:100"));

            List<?> candidates = h.scheduler.selectCandidates(
                    new java.util.ArrayList<>(h.jobStore.all()), h.clock.millis());
            assertEquals("new-1",
                    ((com.github.highcumontoa.jobschedulerquotajava.model.Job) candidates.get(0)).getJobId());
            log.info("[test] 同优先级 fresh主体(new-1)排在served主体(old-3)之前 判定依据=历史下发次数fresh=0 < served=2,公平性优先");
        }
    }
}
