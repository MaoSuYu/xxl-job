package com.xxl.job.admin.core.model;

import java.util.Date;

/**
 * 作者: Mr.Z
 * 时间: 2025-03-05 21:32
 */
public class XxlJobShardingInfo {
    private Long id; // 主键雪花id
    private Long parentJobId; // 父任务id
    private String params; // 子任务执行参数json
    private Date triggerTime; // 执行时间
    private int deleteFlag; // 删除标志
    private int executeBatch; // 执行批次
    private int executeState; //执行状态
    private int executeNumber; // 执行次数

    public int getExecuteNumber() {
        return executeNumber;
    }

    public void setExecuteNumber(int executeNumber) {
        this.executeNumber = executeNumber;
    }

    public int getExecuteState() {
        return executeState;
    }

    public void setExecuteState(int executeState) {
        this.executeState = executeState;
    }
    public int getDeleteFlag() {
        return deleteFlag;
    }

    public void setDeleteFlag(int deleteFlag) {
        this.deleteFlag = deleteFlag;
    }



    public int getExecuteBatch() {
        return executeBatch;
    }

    public void setExecuteBatch(int executeBatch) {
        this.executeBatch = executeBatch;
    }

    public Date getTriggerTime() {
        return triggerTime;
    }

    public void setTriggerTime(Date updateTime) {
        this.triggerTime = updateTime;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getParentJobId() {
        return parentJobId;
    }

    public void setParentJobId(Long parentJobId) {
        this.parentJobId = parentJobId;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }
}
