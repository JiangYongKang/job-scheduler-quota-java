package com.github.highcumontoa.jobschedulerquotajava.tests;

import com.github.highcumontoa.jobschedulerquotajava.core.JobSchedulerService;
import com.github.highcumontoa.jobschedulerquotajava.core.SchedulerProperties;
import com.github.highcumontoa.jobschedulerquotajava.model.DispatchResult;
import com.github.highcumontoa.jobschedulerquotajava.model.JobRequest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景三：挑选顺序可解释——优先级高者优先，同优先级按入队顺序 FIFO；
 * 队首任务配额不足时其后任务继续等待，不发生"绕过队首"的越权/饥饿下发。
 */
class PriorityFairnessTests {

    private static final Logger log = LoggerFactory.getLogger(PriorityFairnessTests.class);

    private JobSchedulerService newService(int quota) {
        SchedulerProperties p = new SchedulerProperties();
        p.setDefaultQuota(quota);
        p.setMaxAttempts(3);
        p.setLeaseTtlMillis(5_000);
        return new JobSchedulerService(p);
    }

    @Test
    void higherPriorityFirstThenFifoWithinSamePriority() {
        JobSchedulerService svc = newService(100);
        // 入队顺序: low(pr=1), highA(pr=9), highB(pr=9,稍后), mid(pr=5)
        svc.submit(new JobRequest("low", "u", 1, 1));
        svc.submit(new JobRequest("highA", "u", 1, 9));
        svc.submit(new JobRequest("mid", "u", 1, 5));
        svc.submit(new JobRequest("highB", "u", 1, 9));

        List<String> order = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            DispatchResult r = svc.dispatchOne();
            assertTrue(r.dispatched());
            order.add(r.jobId());
        }
        // 期望: highA, highB（同优先级 FIFO）, mid, low
        assertEquals(List.of("highA", "highB", "mid", "low"), order);
        log.info("[优先级+FIFO] 下发顺序={}，依据=priority降序后按enqueueSeq升序", order);

        // 队列空：不再下发
        DispatchResult empty = svc.dispatchOne();
        assertTrue(!empty.dispatched());
        assertNull(empty.jobId());
    }

    @Test
    void blockedHeadKeepsFollowingJobsWaitingWithoutBypassing() {
        JobSchedulerService svc = newService(5);
        // big 需要 4，small 需要 2。先下发 big 占用 4
        svc.submit(new JobRequest("big", "u", 4, 9));
        svc.submit(new JobRequest("small", "u", 2, 1));
        assertTrue(svc.dispatchOne().dispatched());
        // 队首 PENDING 是 small（big 已 RUNNING）；4+2>5 => small 等待
        DispatchResult waiting = svc.dispatchOne();
        assertTrue(!waiting.dispatched());
        assertEquals("small", waiting.jobId());
        assertEquals(4, svc.usedQuota("u"));
        log.info("[队首阻塞] big 占用4/5，队首small需要2 => 继续等待，占用仍=4，不绕过、不越权");

        // big 完成释放后，small 可被下发，占用精确变为 2
        svc.complete("big", svc.getJob("big").getLeaseId(), true);
        DispatchResult go = svc.dispatchOne();
        assertTrue(go.dispatched());
        assertEquals("small", go.jobId());
        assertEquals(2, svc.usedQuota("u"));
        log.info("[阻塞解除] big 终结释放后 small 下发，占用=2/5");
    }
}
