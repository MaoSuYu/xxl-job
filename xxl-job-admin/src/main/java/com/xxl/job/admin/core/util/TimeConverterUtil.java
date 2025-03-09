package com.xxl.job.admin.core.util;


import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * 作者: Mr.Z
 * 时间: 2025-03-06 21:03
 */
public class TimeConverterUtil {


    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static long convertToTimestamp(String timeStr) {
        try {
            // 将字符串解析为 LocalDateTime 对象
            LocalDateTime localDateTime = LocalDateTime.parse(timeStr, formatter);
            // 转换为时间戳（毫秒数）
            return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (Exception e) {
            throw new IllegalArgumentException("时间格式不正确: " + timeStr, e);
        }
    }



    public static boolean isNextValidTimeExceedDeadline(Date nextValidTime, String schedulingDeadline) {
        // 将 nextValidTime 转换为时间戳
        long nextValidTimestamp = nextValidTime.getTime();

        // 将 schedulingDeadline 转换为时间戳（以 Java 8+ 为例）
        LocalDateTime deadlineLDT = LocalDateTime.parse(schedulingDeadline, formatter);
        long deadlineTimestamp = deadlineLDT.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

        // 比较时间戳
        return nextValidTimestamp < deadlineTimestamp;
    }


    /**
     * 根据时间单位和数量计算时间戳
     *
     * @param amount 数量
     * @param unit   时间单位
     * @return 计算后的时间戳（毫秒）
     */
    public static long calculateTimestamp(long amount, String unit) {
        switch (unit.toUpperCase()) {
            case "MINUTE":
                return TimeUnit.MINUTES.toMillis(amount);
            case "HOUR":
                return TimeUnit.HOURS.toMillis(amount);
            case "DAY":
                return TimeUnit.DAYS.toMillis(amount);
            case "WEEK":
                return TimeUnit.DAYS.toMillis(amount * 7); // 1周=7天
            case "MONTH":
                return TimeUnit.DAYS.toMillis(amount * 30); // 假设1个月=30天
            default:
                throw new IllegalArgumentException("Unsupported time unit: " + unit);
        }

    }
}
