package com.xxl.job.core.thread;

import com.xxl.job.core.context.JobThreadContext;
import com.xxl.job.core.enums.ThreadConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 任务线程监控助手
 * 用于监控当前正在运行的任务线程
 */
public class JobThreadMonitorHelper {
    private static Logger logger = LoggerFactory.getLogger(JobThreadMonitorHelper.class);

    private static JobThreadMonitorHelper instance = new JobThreadMonitorHelper();
    
    public static JobThreadMonitorHelper getInstance() {
        return instance;
    }

    private Thread monitorThread;
    private volatile boolean toStop = false;

    /**
     * 启动监控线程
     */
    public void start() {
        monitorThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!toStop) {
                    try {
                        // 获取当前正在运行的任务线程
                        ConcurrentHashMap<String, Thread> jobThreadMap = JobThreadContext.getJobThreadContextMap();
                        
                        // 记录当前运行的任务数量
                        int runningJobCount = jobThreadMap.size();
                        if (runningJobCount > 0) {
                            StringBuilder sb = new StringBuilder();
                            sb.append("运行中任务[").append(runningJobCount).append("]: ");
                            
                            // 遍历并记录每个任务的信息，使用简洁格式
                            for (Map.Entry<String, Thread> entry : jobThreadMap.entrySet()) {
                                String jobId = entry.getKey();
                                Thread jobThread = entry.getValue();
                                
                                // 添加任务信息: jobId(线程状态)
                                sb.append(jobId).append("(").append(jobThread.getState().name()).append(") ");
                            }
                            
                            // 输出一条简洁的日志
                            logger.info("{}", sb.toString());
                        }
                        
                        // 使用常量定义的扫描间隔
                        TimeUnit.SECONDS.sleep(ThreadConstant.MONITOR_SCAN_INTERVAL);
                    } catch (Throwable e) {
                        if (!toStop) {
                            logger.error("任务监控异常: {}", e.getMessage());
                        }
                    }
                }
                
                logger.info("任务线程监控已停止");
            }
        });
        
        monitorThread.setDaemon(true);
        monitorThread.setName("xxl-job-monitor");
        monitorThread.start();
        
        logger.info("任务线程监控已启动");
    }

    /**
     * 停止监控线程
     */
    public void toStop() {
        toStop = true;
        
        if (monitorThread != null) {
            monitorThread.interrupt();
            try {
                monitorThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
} 