package com.xxl.job.core.enums;

/**
 * Created by xuxueli on 17/5/9.
 */
public enum ExecutorBlockStrategyEnum {

    SERIAL_EXECUTION("Serial execution"),
    /*CONCURRENT_EXECUTION("并行"),*/
    DISCARD_LATER("Discard Later"),
    COVER_EARLY("Cover Early");

    private String title;
    private ExecutorBlockStrategyEnum (String title) {
        this.title = title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
    public String getTitle() {
        return title;
    }

    /**
     * 根据给定的名称匹配阻塞策略枚举。
     * 
     * @param name 阻塞策略的名称
     * @param defaultItem 默认的阻塞策略
     * @return 匹配到的阻塞策略枚举，如果没有匹配到则返回默认策略
     * 
     * 该方法用于根据名称查找对应的阻塞策略枚举，
     * 如果找不到匹配的策略，则返回提供的默认策略。
     */
    public static ExecutorBlockStrategyEnum match(String name, ExecutorBlockStrategyEnum defaultItem) {
        if (name != null) {
            for (ExecutorBlockStrategyEnum item:ExecutorBlockStrategyEnum.values()) {
                if (item.name().equals(name)) {
                    return item;
                }
            }
        }
        return defaultItem;
    }
}
