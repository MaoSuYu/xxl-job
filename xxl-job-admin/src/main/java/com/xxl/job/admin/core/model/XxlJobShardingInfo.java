package com.xxl.job.admin.core.model;

/**
 * 作者: Mr.Z
 * 时间: 2025-03-05 21:32
 */
public class XxlJobShardingInfo {
    private Long id;
    private Long parentJobId;
    private String params;

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
