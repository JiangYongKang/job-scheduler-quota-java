package com.github.highcumontoa.jobschedulerquotajava.executor;

import com.github.highcumontoa.jobschedulerquotajava.service.JobService;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * LocalTaskExecutor 工厂：并行度等配置在创建实例时注入
 * （{@code @Value} 不作用于直接 new 的对象，故由工厂读取后构造）。
 */
@Component
public class LocalTaskExecutorFactory {

    private final ObjectProvider<JobService> jobServiceProvider;

    @Value("${scheduler.local-executor.parallelism:4}")
    private int parallelism;

    public LocalTaskExecutorFactory(ObjectProvider<JobService> jobServiceProvider) {
        this.jobServiceProvider = jobServiceProvider;
    }

    public LocalTaskExecutor create() {
        LocalTaskExecutor executor = new LocalTaskExecutor(jobServiceProvider);
        executor.setParallelism(parallelism);
        return executor;
    }
}
