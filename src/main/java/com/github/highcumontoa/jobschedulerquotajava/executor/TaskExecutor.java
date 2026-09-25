package com.github.highcumontoa.jobschedulerquotajava.executor;

import com.github.highcumontoa.jobschedulerquotajava.model.Job;
import com.github.highcumontoa.jobschedulerquotajava.model.Lease;

/**
 * 本地执行者抽象：任务下发后在本机线程内模拟执行并回传结果。
 * 容量（可同时运行的租约数）有限，调度器据此决定是否继续下发。
 */
public interface TaskExecutor {

    String getId();

    /** 当前是否还能再接收一个任务（与 {@link #reserveSlot()} 语义联动）。 */
    boolean hasCapacity();

    /**
     * 原子占用一个执行槽位；无容量时返回 false。
     * 任务执行结束（无论回传是否被接受）必须调用 {@link #releaseSlot()} 归还槽位。
     */
    boolean reserveSlot();

    void releaseSlot();

    /** 在已占用槽位的前提下异步执行任务，完成后回传结果。 */
    void submit(Job job, Lease lease);
}
