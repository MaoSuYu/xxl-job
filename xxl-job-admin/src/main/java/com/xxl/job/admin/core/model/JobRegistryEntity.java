package com.xxl.job.admin.core.model;

public class JobRegistryEntity {

    private String registryValue;
    private int threadRunningCount;
    private int maxThreadCount;
    private int remainingThreadCount;

    // 无参构造函数
    public JobRegistryEntity() {
    }

    // 全参构造函数
    public JobRegistryEntity(String registryValue, int threadRunningCount, int maxThreadCount, int remainingThreadCount) {
        this.registryValue = registryValue;
        this.threadRunningCount = threadRunningCount;
        this.maxThreadCount = maxThreadCount;
        this.remainingThreadCount = remainingThreadCount;
    }

    // Getter 和 Setter 方法
    public String getRegistryValue() {
        return registryValue;
    }

    public void setRegistryValue(String registryValue) {
        this.registryValue = registryValue;
    }

    public int getThreadRunningCount() {
        return threadRunningCount;
    }

    public void setThreadRunningCount(int threadRunningCount) {
        this.threadRunningCount = threadRunningCount;
    }

    public int getMaxThreadCount() {
        return maxThreadCount;
    }

    public void setMaxThreadCount(int maxThreadCount) {
        this.maxThreadCount = maxThreadCount;
    }

    public int getRemainingThreadCount() {
        return remainingThreadCount;
    }

    public void setRemainingThreadCount(int remainingThreadCount) {
        this.remainingThreadCount = remainingThreadCount;
    }

    @Override
    public String toString() {
        return "JobRegistryEntity{" +
                "registryValue='" + registryValue + '\'' +
                ", threadRunningCount=" + threadRunningCount +
                ", maxThreadCount=" + maxThreadCount +
                ", remainingThreadCount=" + remainingThreadCount +
                '}';
    }
}
