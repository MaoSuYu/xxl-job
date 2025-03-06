package com.xxl.job.core.biz.model;

/**
 * 执行器状态信息
 */
public class ExecutorStatus {
    
    /**
     * 作业线程数
     */
    private int threadCount;
    
    /**
     * 正在执行的任务数
     */
    private int runningTaskCount;
    
    /**
     * 等待执行的任务数
     */
    private int pendingTaskCount;

    public ExecutorStatus() {
    }

    public ExecutorStatus(int threadCount, int runningTaskCount, int pendingTaskCount) {
        this.threadCount = threadCount;
        this.runningTaskCount = runningTaskCount;
        this.pendingTaskCount = pendingTaskCount;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public int getRunningTaskCount() {
        return runningTaskCount;
    }

    public void setRunningTaskCount(int runningTaskCount) {
        this.runningTaskCount = runningTaskCount;
    }

    public int getPendingTaskCount() {
        return pendingTaskCount;
    }

    public void setPendingTaskCount(int pendingTaskCount) {
        this.pendingTaskCount = pendingTaskCount;
    }

    @Override
    public String toString() {
        return "ExecutorStatus{" +
                "threadCount=" + threadCount +
                ", runningTaskCount=" + runningTaskCount +
                ", pendingTaskCount=" + pendingTaskCount +
                '}';
    }
} 