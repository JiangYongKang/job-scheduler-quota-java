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

    /**
     * 抢占移交：把 leaseId 占用的槽位标记为"被调度器征用"。
     * 成功后槽位计数保持不变、归属转交调用方（该租约的工作线程结束时不再自行归还槽位），
     * 调用方随后可直接 {@link #submit} 新任务使用这个槽位，或在放弃抢占时自行 {@link #releaseSlot()}。
     * 本方法只完成记账移交，不中断工作线程；状态迁移提交后应调用 {@link #interruptWorker} 让其退出。
     *
     * @return false 表示该租约的工作线程已结束（槽位已归还），调用方应改用 {@link #reserveSlot()}
     */
    boolean seizeSlot(String leaseId);

    /** 中断指定租约的工作线程（配合 {@link #seizeSlot} 使用）；线程不存在时为空操作。 */
    void interruptWorker(String leaseId);
}
