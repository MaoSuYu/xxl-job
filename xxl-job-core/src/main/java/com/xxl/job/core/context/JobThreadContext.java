package com.xxl.job.core.context;

import java.util.concurrent.ConcurrentHashMap;

public class JobThreadContext {

    /**
     * 所有执行工作任务的线程都会注册到其中
     */
    private static ConcurrentHashMap<String, Thread> JOB_THREAD_CONTEXT_MAP = new ConcurrentHashMap<>();

    public static ConcurrentHashMap<String, Thread> getJobThreadContextMap() {
        return JOB_THREAD_CONTEXT_MAP;
    }

    public static void setJobThreadContextMap(Object jobId, Thread thread) {
        JOB_THREAD_CONTEXT_MAP.put(String.valueOf(jobId), thread);
    }

    /**
     * 从线程上下文映射中移除指定任务ID的线程
     * 
     * @param jobId 任务ID
     * @return 被移除的线程，如果不存在则返回null
     */
    public static Thread removeJobThread(Object jobId) {
        return JOB_THREAD_CONTEXT_MAP.remove(String.valueOf(jobId));
    }

}
