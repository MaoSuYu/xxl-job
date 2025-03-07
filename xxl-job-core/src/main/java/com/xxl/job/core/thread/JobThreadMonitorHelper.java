package com.xxl.job.core.thread;

import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.client.AdminBizClient;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.ThreadInfo;
import com.xxl.job.core.context.JobThreadContext;
import com.xxl.job.core.context.XxlJobContext;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.enums.ThreadConstant;
import com.xxl.job.core.executor.XxlJobExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

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
                        int runningJobCount = jobThreadMap.size();
                        
                        if (runningJobCount > 0) {
                            List<ThreadInfo> threadInfoList = new ArrayList<>();
                            StringBuilder sb = new StringBuilder();
                            sb.append("运行中任务[").append(runningJobCount).append("]: ");
                            
                            // 一次遍历完成所有操作
                            for (Map.Entry<String, Thread> entry : jobThreadMap.entrySet()) {
                                String jobId = entry.getKey();
                                Thread thread = entry.getValue();
                                String threadState = thread.getState().name();
                                
                                // 获取JobThread实例（如果可能）
                                long startTime = System.currentTimeMillis();
                                if (thread instanceof JobThread) {
                                    JobThread jobThread = (JobThread) thread;
                                    startTime = jobThread.getStartTime();
                                }
                                
                                // 获取执行器信息
                                String appName = XxlJobExecutor.getStaticAppname();
                                String ip = XxlJobExecutor.getStaticIp();
                                int port = XxlJobExecutor.getStaticPort();

                                // 收集线程信息用于上报，添加执行器信息
                                ThreadInfo threadInfo = new ThreadInfo(
                                    Long.valueOf(jobId),
                                    threadState,
                                    appName,
                                    ip,
                                    port,
                                    startTime
                                );
                                threadInfoList.add(threadInfo);
                                
                                // 同时构建日志信息
                                sb.append(jobId).append("(").append(threadState).append(") ");
                            }
                            
                            // 上报线程信息
                            logger.info("准备上报线程信息，共 {} 个线程：{}", threadInfoList.size(), 
                                threadInfoList.stream()
                                    .map(info -> {
                                        StringBuilder logSb = new StringBuilder();
                                        logSb.append("线程ID:").append(info.getJobId())
                                          .append("(状态:").append(info.getThreadState()).append(")");
                                        
                                        // 只有当执行器信息不为空时才添加
                                        if (info.getAppName() != null && !info.getAppName().isEmpty()) {
                                            logSb.append(",执行器:").append(info.getAppName());
                                        }
                                        
                                        if (info.getIp() != null && !info.getIp().isEmpty()) {
                                            logSb.append(",IP:").append(info.getIp());
                                            if (info.getPort() > 0) {
                                                logSb.append(":").append(info.getPort());
                                            }
                                        }
                                        
                                        return logSb.toString();
                                    })
                                    .reduce((a, b) -> a + ", " + b)
                                    .orElse(""));
                            List<AdminBiz> adminBizList = XxlJobExecutor.getAdminBizList();
                            for (AdminBiz adminBiz : adminBizList) {
                                if (adminBiz != null) {
                                    try {
                                        ReturnT<String> result = adminBiz.reportRunningThreads(threadInfoList);
                                        if (result.getCode() == ReturnT.SUCCESS_CODE) {
                                            logger.info("线程信息上报成功 - [code:{}, msg:{}]", result.getCode(), result.getMsg());
                                        } else {
                                            logger.warn("线程信息上报失败 - [code:{}, msg:{}]", result.getCode(), result.getMsg());
                                        }
                                    } catch (Exception e) {
                                        logger.error("上报线程信息异常: {}", e.getMessage());
                                    }
                                } else {
                                    logger.warn("adminBizClient未初始化，无法上报线程信息");
                                }
                            }
                            // 输出日志
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