package com.xxl.job.admin.core.util;

import cn.hutool.core.util.StrUtil;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 作者: Mr.Z
 * 时间: 2025-03-05 11:19
 */
public class TimeRangeSplitterUtils {


    private static final Map<String, ChronoUnit> TIME_UNIT_MAP = new HashMap<>();

    static {
        TIME_UNIT_MAP.put("MINUTE", ChronoUnit.MINUTES);
        TIME_UNIT_MAP.put("HOUR", ChronoUnit.HOURS);
        TIME_UNIT_MAP.put("DAY", ChronoUnit.DAYS);
        TIME_UNIT_MAP.put("WEEK", ChronoUnit.WEEKS);
        TIME_UNIT_MAP.put("MONTH", ChronoUnit.MONTHS);
    }

    public static ChronoUnit getChronoUnit(String timeUnitStr) {
        if (StrUtil.isBlank(timeUnitStr)){
            throw new IllegalArgumentException("Time unit cannot be empty！");
        }
        ChronoUnit orDefault = TIME_UNIT_MAP.getOrDefault(timeUnitStr.toUpperCase(), null);
        if (ObjectUtils.isEmpty(orDefault)){
          throw new IllegalArgumentException("Invalid time unit:" + timeUnitStr);
        }
        return orDefault;
    }


    /**
     * 拆分时间范围
     *
     * @param startStr    开始时间
     * @param endStr      结束时间
     * @param interval 时间间隔
     * @param unit     时间单位（ChronoUnit.MINUTES, HOURS, DAYS, MONTHS, YEARS）
     * @return 拆分后的时间范围列表
     */
    public static List<TimeRange> splitTimeRange(String startStr, String endStr, int interval, String unit) {
        // 解析时间字符串为 LocalDateTime
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime start = LocalDateTime.parse(startStr, formatter);
        LocalDateTime end = LocalDateTime.parse(endStr, formatter);
        List<TimeRange> timeRanges = new ArrayList<>();

        LocalDateTime currentStart = start;

        while (currentStart.isBefore(end)) {
            ChronoUnit chronoUnit = getChronoUnit(unit);
            LocalDateTime currentEnd = currentStart.plus(interval, chronoUnit);

            // 如果当前结束时间超过了总结束时间，则调整为总结束时间
            if (currentEnd.isAfter(end)) {
                currentEnd = end;
            }
            // 将 LocalDateTime 格式化为字符串
            String formattedStart = currentStart.format(formatter);
            String formattedEnd = currentEnd.format(formatter);

            timeRanges.add(new TimeRange(formattedStart, formattedEnd));

            // 更新下一个时间范围的开始时间
            currentStart = currentEnd;
        }

        return timeRanges;
    }
}
