package com.xxl.job.admin.core.util;

import java.time.LocalDateTime;

/**
 * 时间范围类
 * 作者: Mr.Z
 * 时间: 2025-03-05 11:27
 */
public class TimeRange {
    public String getStart() {
        return start;
    }

    public String getEnd() {
        return end;
    }

    private final String start;
    private final String end;

    public TimeRange(String start, String end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public String toString() {
        return start + " - " + end;
    }
}
