package com.xxl.job.core.enums;

/**
 * 作者: Mr.Z
 * 时间: 2025-03-10 13:48
 */
public enum ExecutionStatus {

    NOT_EXECUTED(0, "未执行"),
    SUCCESS(1, "执行成功"),
    FAILED(2, "执行失败"),
    NOT_TRIGGER(3, "调用失败"),
    TRIGGERRING(4, "调用中");

    private final int code;
    private final String description;

    // 构造函数
    ExecutionStatus(int code, String description) {
        this.code = code;
        this.description = description;
    }

    // 获取状态码
    public int getCode() {
        return code;
    }

    // 获取状态描述
    public String getDescription() {
        return description;
    }

    // 根据状态码获取枚举实例
    public static ExecutionStatus getByCode(int code) {
        for (ExecutionStatus status : ExecutionStatus.values()) {
            if (status.getCode() == code) {
                return status;
            }
        }
        throw new IllegalArgumentException("无效的状态码: " + code);
    }


}
