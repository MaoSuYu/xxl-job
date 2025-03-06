package com.xxl.job.core.enums;

public class ThreadConstant {

    /**
     * job thread repository最多存多少任务
     */
    public static final int MAX_THREAD_COUNT = 2;

    /**
     * job thread最多存活多久
     */
    public static final int ATTEMPTS = 1;
    
    /**
     * 扫描本节点一共有多少线程正在执行任务,非队列中排队的任务（秒）
     */
    public static final int MONITOR_SCAN_INTERVAL = 5;

}
