package com.xxl.job.core.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 日期分片工具类
 * 支持按分钟、小时、天、周、月对时间范围进行分片
 * 
 * @author symao
 */
public class DateShardingUtil {
    
    /**
     * 输出格式化器，用于统一输出格式
     */
    private static final DateTimeFormatter OUTPUT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
    
    /**
     * 日期格式缓存，避免重复创建DateTimeFormatter
     */
    private static final Map<String, DateTimeFormatter> FORMAT_CACHE = new HashMap<>();
    
    static {
        // 初始化常用格式
        String[] patterns = {
            // 基本格式，精确到分钟
            "yyyy-MM-dd HH:mm",   // 标准格式，如 2025-03-11 19:38
            "yyyy-M-dd HH:mm",    // 月份无前导零，如 2025-3-11 19:38
            "yyyy-MM-d HH:mm",    // 日期无前导零，如 2025-03-1 19:38
            "yyyy-M-d HH:mm",     // 月份和日期都无前导零，如 2025-3-1 19:38
            
            // 带秒的格式
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-M-dd HH:mm:ss",
            "yyyy-MM-d HH:mm:ss",
            "yyyy-M-d HH:mm:ss",
            
            // 带毫秒的格式
            "yyyy-MM-dd HH:mm:ss.SSS",
            "yyyy-M-dd HH:mm:ss.SSS",
            "yyyy-MM-d HH:mm:ss.SSS",
            "yyyy-M-d HH:mm:ss.SSS"
        };
        
        for (String pattern : patterns) {
            FORMAT_CACHE.put(pattern, DateTimeFormatter.ofPattern(pattern));
        }
    }

    /**
     * 时间单位枚举，用于日期分片
     */
    public enum TimeUnit {
        /**
         * 分钟级别分片
         */
        MINUTE(ChronoUnit.MINUTES),
        
        /**
         * 小时级别分片
         */
        HOUR(ChronoUnit.HOURS),
        
        /**
         * 天级别分片
         */
        DAY(ChronoUnit.DAYS),
        
        /**
         * 周级别分片
         */
        WEEK(ChronoUnit.WEEKS),
        
        /**
         * 月级别分片
         */
        MONTH(ChronoUnit.MONTHS);
        
        private final ChronoUnit chronoUnit;
        
        TimeUnit(ChronoUnit chronoUnit) {
            this.chronoUnit = chronoUnit;
        }
        
        public ChronoUnit getChronoUnit() {
            return chronoUnit;
        }
    }
    
    /**
     * 解析时间字符串为LocalDateTime，支持灵活的日期格式
     * 只保留到分钟精度，忽略秒和毫秒部分
     * 
     * @param timeStr 时间字符串，如 "2025-03-11 19:38" 或 "2025-3-11 19:38" 或 "2025-03-11 19:38:45"
     * @return 解析后的LocalDateTime对象（精确到分钟）
     */
    private static LocalDateTime parseDateTime(String timeStr) {
        if (timeStr == null || timeStr.trim().isEmpty()) {
            throw new IllegalArgumentException("时间字符串不能为空");
        }
        
        // 先尝试ISO格式
        try {
            return LocalDateTime.parse(timeStr).truncatedTo(ChronoUnit.MINUTES);
        } catch (DateTimeParseException e) {
            // 继续尝试其他格式
        }
        
        // 尝试缓存的格式
        for (Map.Entry<String, DateTimeFormatter> entry : FORMAT_CACHE.entrySet()) {
            try {
                LocalDateTime dateTime = LocalDateTime.parse(timeStr, entry.getValue());
                return dateTime.truncatedTo(ChronoUnit.MINUTES);
            } catch (DateTimeParseException e) {
                // 尝试下一种格式
            }
        }
        
        throw new IllegalArgumentException("无法解析时间字符串: " + timeStr + 
                "，支持的格式包括精确到分钟、秒或毫秒的多种日期时间格式");
    }
    
    /**
     * 根据指定的单位和值将时间范围拆分为多个分片
     * 
     * @param startTime 开始时间，支持多种格式，且只精确到分钟
     * @param endTime 结束时间，支持多种格式，且只精确到分钟
     * @param unit 分片的时间单位
     * @param value 分片的值（例如，5表示5分钟）
     * @return 时间分片列表，每个分片为一个起止时间对
     */
    public static List<TimeShard> shardTimeRange(String startTime, String endTime, TimeUnit unit, int value) {
        // 参数验证
        if (startTime == null || endTime == null) {
            throw new IllegalArgumentException("开始时间和结束时间不能为null");
        }
        if (unit == null) {
            throw new IllegalArgumentException("时间单位不能为null");
        }
        if (value <= 0) {
            throw new IllegalArgumentException("分片值必须大于0");
        }
        
        LocalDateTime start = parseDateTime(startTime);
        LocalDateTime end = parseDateTime(endTime);
        
        // 确保开始时间不晚于结束时间
        if (start.isAfter(end)) {
            throw new IllegalArgumentException("开始时间 " + startTime + " 不能晚于结束时间 " + endTime);
        }
        
        List<TimeShard> shards = new ArrayList<>();
        
        LocalDateTime currentStart = start;
        while (currentStart.isBefore(end)) {
            LocalDateTime currentEnd = currentStart.plus(value, unit.getChronoUnit());
            
            // 确保最后一个分片不会超过结束时间
            if (currentEnd.isAfter(end)) {
                currentEnd = end;
            }
            
            shards.add(new TimeShard(currentStart, currentEnd));
            currentStart = currentEnd;
        }
        
        return shards;
    }
    
    /**
     * 表示带有开始和结束时间的时间分片的类
     */
    public static class TimeShard {
        private final LocalDateTime startTime;
        private final LocalDateTime endTime;
        
        public TimeShard(LocalDateTime startTime, LocalDateTime endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
        }
        
        public LocalDateTime getStartTime() {
            return startTime;
        }
        
        public LocalDateTime getEndTime() {
            return endTime;
        }
        
        /**
         * 获取分片的持续时间（分钟）
         */
        public long getDurationMinutes() {
            return ChronoUnit.MINUTES.between(startTime, endTime);
        }
        
        /**
         * 获取分片的持续时间（小时）
         */
        public long getDurationHours() {
            return ChronoUnit.HOURS.between(startTime, endTime);
        }
        
        /**
         * 获取分片的持续时间（天）
         */
        public long getDurationDays() {
            return ChronoUnit.DAYS.between(startTime, endTime);
        }
        
        @Override
        public String toString() {
            return startTime.format(OUTPUT_FORMATTER) + " - " + endTime.format(OUTPUT_FORMATTER) 
                + " (持续" + getDurationMinutes() + "分钟)";
        }
    }
} 