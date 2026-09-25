package com.github.highcumontoa.jobschedulerquotajava.web;

import com.github.highcumontoa.jobschedulerquotajava.lease.LeaseManager;
import com.github.highcumontoa.jobschedulerquotajava.scheduler.Scheduler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.util.List;
import java.util.Map;

/**
 * 运维入口：手动触发租约回收与一轮调度（本地复现 / 演示用）。
 */
@RestController
@RequestMapping("/api/admin")
public class AdminController {

    private final LeaseManager leaseManager;
    private final Scheduler scheduler;
    private final Clock clock;

    public AdminController(LeaseManager leaseManager, Scheduler scheduler, Clock clock) {
        this.leaseManager = leaseManager;
        this.scheduler = scheduler;
        this.clock = clock;
    }

    @PostMapping("/reclaim")
    public Map<String, Object> reclaim() {
        List<String> reclaimed = leaseManager.reclaimExpired(clock.millis());
        return Map.of("reclaimed", reclaimed);
    }

    @PostMapping("/tick")
    public Map<String, Object> tick() {
        leaseManager.reclaimExpired(clock.millis());
        List<String> dispatched = scheduler.tick();
        return Map.of("dispatched", dispatched);
    }
}
