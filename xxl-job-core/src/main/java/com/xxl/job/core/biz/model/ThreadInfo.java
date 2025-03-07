package com.xxl.job.core.biz.model;

import java.util.List;

public class ThreadInfo {
    private Long jobId;
    private String threadState;
    
    // 执行器基本信息
    private String appName;        // 执行器名称
    private String ip;             // 执行器IP
    private int port;              // 执行器端口
    private String title;          // 执行器标题
    private String executorTitle;  // 执行器组标题
    private long startTime;        // 线程启动时间
    private long runningTime;      // 线程运行时长(毫秒)

    // 无参构造函数
    public ThreadInfo() {
    }

    // 基本构造函数
    public ThreadInfo(Long jobId, String threadState) {
        this.jobId = jobId;
        this.threadState = threadState;
    }
    
    // 完整构造函数
    public ThreadInfo(Long jobId, String threadState, String appName, String ip, int port, String title, long startTime) {
        this.jobId = jobId;
        this.threadState = threadState;
        this.appName = appName;
        this.ip = ip;
        this.port = port;
        this.title = title;
        this.startTime = startTime;
        this.runningTime = System.currentTimeMillis() - startTime;
    }

    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    public String getThreadState() {
        return threadState;
    }

    public void setThreadState(String threadState) {
        this.threadState = threadState;
    }
    
    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
    
    public String getExecutorTitle() {
        return executorTitle;
    }

    public void setExecutorTitle(String executorTitle) {
        this.executorTitle = executorTitle;
    }
    
    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }
    
    public long getRunningTime() {
        return runningTime;
    }

    public void setRunningTime(long runningTime) {
        this.runningTime = runningTime;
    }
    
    @Override
    public String toString() {
        return "ThreadInfo{" +
                "jobId='" + jobId + '\'' +
                ", threadState='" + threadState + '\'' +
                ", appName='" + appName + '\'' +
                ", ip='" + ip + '\'' +
                ", port=" + port +
                ", title='" + title + '\'' +
                ", executorTitle='" + executorTitle + '\'' +
                ", startTime=" + startTime +
                ", runningTime=" + runningTime +
                '}';
    }
} 