package com.xxl.job.core.biz.model;
import com.xxl.job.core.enums.TimeUnit;


/**
 * 作者: Mr.Z
 * 时间: 2025-03-06 13:47
 */
public class HandleShardingParam {

    /**
     * 任务id
     */
    private Long id;

    /**
     * 执行器服务名称
     */
    private String appName;

    /**
     * 执行方法
     */
    private String executeHandle;

    /**
     * *是否手动触发/0,自动触发1
     * @return
     */
    private int isAutomatic;

    /**
     * 优先级 越低越高
     * @return
     */
    private int priority;

    /**
     * 调度周期
     */
    private TimeUnit schedulingCycle;

    /**
     * 调度间隔
     */
    private int schedulingInterval;

    /**
     * 调度的首次时间
     */
    private String firstSchedulingTime;

    /**
     * 调度的截止时间
     */
    private String schedulingDeadline;

    /**
     * 数据的开始时间
     */
    private String startTimeOfData;

    /**
     * 数据的截止时间
     */
    private String endTimeOfData;

    /**
     * 数据时间间隔
     */
    private int dataInterval;

    /**
     * 数据时间间隔单位
     */
    private TimeUnit timeUnit;


    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public int getIsAutomatic() {
        return isAutomatic;
    }

    public void setIsAutomatic(int isAutomatic) {
        this.isAutomatic = isAutomatic;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getExecuteHandle() {
        return executeHandle;
    }

    public void setExecuteHandle(String executeHandle) {
        this.executeHandle = executeHandle;
    }

    public TimeUnit getSchedulingCycle() {
        return schedulingCycle;
    }

    public void setSchedulingCycle(TimeUnit schedulingCycle) {
        this.schedulingCycle = schedulingCycle;
    }

    public int getSchedulingInterval() {
        return schedulingInterval;
    }

    public void setSchedulingInterval(int schedulingInterval) {
        this.schedulingInterval = schedulingInterval;
    }

    public String getFirstSchedulingTime() {
        return firstSchedulingTime;
    }

    public void setFirstSchedulingTime(String firstSchedulingTime) {
        this.firstSchedulingTime = firstSchedulingTime;
    }

    public String getSchedulingDeadline() {
        return schedulingDeadline;
    }

    public void setSchedulingDeadline(String schedulingDeadline) {
        this.schedulingDeadline = schedulingDeadline;
    }

    public String getStartTimeOfData() {
        return startTimeOfData;
    }

    public void setStartTimeOfData(String startTimeOfData) {
        this.startTimeOfData = startTimeOfData;
    }

    public String getEndTimeOfData() {
        return endTimeOfData;
    }

    public void setEndTimeOfData(String endTimeOfData) {
        this.endTimeOfData = endTimeOfData;
    }

    public int getDataInterval() {
        return dataInterval;
    }

    public void setDataInterval(int dataInterval) {
        this.dataInterval = dataInterval;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }


}
