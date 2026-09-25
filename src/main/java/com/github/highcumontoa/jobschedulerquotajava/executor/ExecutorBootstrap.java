package com.github.highcumontoa.jobschedulerquotajava.executor;

import com.github.highcumontoa.jobschedulerquotajava.scheduler.Scheduler;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 本机执行者装配：按 scheduler.local-executor.count 启动若干 {@link LocalTaskExecutor}
 * 并注册到调度器。整个过程只使用本机线程，无外部依赖。
 */
@Component
public class ExecutorBootstrap {

    private final Scheduler scheduler;
    private final LocalTaskExecutorFactory factory;

    @Value("${scheduler.local-executor.count:1}")
    private int executorCount;

    private final List<TaskExecutor> created = new ArrayList<>();

    public ExecutorBootstrap(Scheduler scheduler, LocalTaskExecutorFactory factory) {
        this.scheduler = scheduler;
        this.factory = factory;
    }

    @PostConstruct
    public void register() {
        int n = Math.max(1, executorCount);
        for (int i = 0; i < n; i++) {
            TaskExecutor executor = factory.create();
            created.add(executor);
            scheduler.registerExecutor(executor);
        }
    }

    public List<TaskExecutor> getCreated() {
        return List.copyOf(created);
    }
}
