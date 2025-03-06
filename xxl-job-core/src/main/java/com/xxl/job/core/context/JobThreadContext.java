package com.xxl.job.core.context;

import java.util.concurrent.ConcurrentHashMap;

public class JobThreadContext {
    public static ConcurrentHashMap<String, Thread> JOB_THREAD_CONTEXT_MAP = new ConcurrentHashMap<>();

    public static ConcurrentHashMap<String, Thread> getJobThreadContextMap() {
        return JOB_THREAD_CONTEXT_MAP;
    }

    public static void setJobThreadContextMap(Object jobId, Thread thread) {
        JOB_THREAD_CONTEXT_MAP.put(String.valueOf(jobId), thread);
    }
}
