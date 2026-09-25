package com.github.highcumontoa.jobschedulerquotajava.model;

/**
 * 准入被拒绝的可区分原因。
 * <ul>
 *   <li>{@link #QUOTA_EXCEEDED}：提交主体运行中资源已达配额上限；</li>
 *   <li>{@link #DUPLICATE_SUBMISSION}：同一任务标识存在未终结的既有提交（冲突）。</li>
 * </ul>
 */
public enum RejectReason {
    QUOTA_EXCEEDED("quota_exceeded", "提交主体运行中资源占用已达配额上限，拒绝准入且不改变既有占用"),
    DUPLICATE_SUBMISSION("duplicate_submission", "存在相同任务标识且尚未终结的既有提交，冲突拒绝");

    private final String code;
    private final String description;

    RejectReason(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public String getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }
}
