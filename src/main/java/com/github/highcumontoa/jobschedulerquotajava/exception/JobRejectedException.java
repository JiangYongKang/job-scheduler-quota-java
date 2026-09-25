package com.github.highcumontoa.jobschedulerquotajava.exception;

import com.github.highcumontoa.jobschedulerquotajava.model.RejectReason;

/**
 * 业务拒绝异常：携带可区分的 {@link RejectReason} 与判定依据说明。
 * 抛出该异常的操作都不会改变既有占用（在锁内校验通过后才写入）。
 */
public class JobRejectedException extends RuntimeException {

    private final RejectReason reason;

    public JobRejectedException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason getReason() {
        return reason;
    }
}
