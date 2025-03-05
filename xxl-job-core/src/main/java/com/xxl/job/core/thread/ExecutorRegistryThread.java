package com.xxl.job.core.thread;

import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.enums.RegistryConfig;
import com.xxl.job.core.enums.ThreadConstant;
import com.xxl.job.core.executor.XxlJobExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 执行器注册线程
 * 
 * 职责：
 * 1. 维护执行器到调度中心的心跳连接
 * 2. 定期发送注册请求，保持执行器存活状态
 * 3. 执行器优雅下线，主动发送注销请求
 * 
 * 工作流程：
 * 1. 执行器启动时，启动注册线程
 * 2. 定期向调度中心发送心跳包（注册请求）
 * 3. 执行器关闭时，发送注销请求并终止线程
 * 
 * @author xuxueli 2017-03-02
 */
public class ExecutorRegistryThread {
    // 日志记录器
    private static Logger logger = LoggerFactory.getLogger(ExecutorRegistryThread.class);

    // 单例实例
    private static ExecutorRegistryThread instance = new ExecutorRegistryThread();
    
    /**
     * 获取ExecutorRegistryThread单例
     * @return ExecutorRegistryThread实例
     */
    public static ExecutorRegistryThread getInstance(){
        return instance;
    }

    // 注册线程
    private Thread registryThread;
    // 线程停止标志
    private volatile boolean toStop = false;

    /**
     * 启动注册线程
     * 
     * @param appname 执行器名称，用于标识执行器
     * @param address 执行器地址，格式为 "IP:PORT"
     */
    public void start(final String appname, final String address){

        // 参数校验
        if (appname==null || appname.trim().length()==0) {
            logger.warn(">>>>>>>>>>> xxl-job, 执行器注册配置失败, 应用名为空.");
            return;
        }
        if (XxlJobExecutor.getAdminBizList() == null) {
            logger.warn(">>>>>>>>>>> xxl-job, 执行器注册配置失败, 调度中心地址为空.");
            return;
        }

        registryThread = new Thread(new Runnable() {
            @Override
            public void run() {

                // 注册循环：定期发送心跳包
                while (!toStop) {
                    try {
                        // 打印当前执行器的任务状态
                        int threadCount = XxlJobExecutor.getRunningJobThreadCount();
                        int runningTaskCount = XxlJobExecutor.getRunningTaskCount();
                        int pendingTaskCount = XxlJobExecutor.getPendingTaskCount();

                        // 构建注册参数
                        RegistryParam registryParam = new RegistryParam(RegistryConfig.RegistType.EXECUTOR.name(), appname, address, runningTaskCount, ThreadConstant.MAX_THREAD_COUNT);
                        logger.debug(">>>>>>>>>>> xxl-job, 执行器当前状态->作业线程数={}, 正在执行的任务数={}, 等待执行的任务数={}, 执行器最大线程容量={}",
                                threadCount, runningTaskCount, pendingTaskCount, ThreadConstant.MAX_THREAD_COUNT);
                        
                        // 遍历所有配置的调度中心地址
                        for (AdminBiz adminBiz: XxlJobExecutor.getAdminBizList()) {
                            try {
                                // 发送注册请求（心跳包）
                                ReturnT<String> registryResult = adminBiz.registry(registryParam);
                                // 注册成功则跳出循环，注册失败则尝试下一个调度中心地址
                                if (registryResult!=null && ReturnT.SUCCESS_CODE == registryResult.getCode()) {
                                    registryResult = ReturnT.SUCCESS;
                                    logger.debug(">>>>>>>>>>> xxl-job 注册成功, 注册参数:{}, 注册结果:{}", new Object[]{registryParam, registryResult});
                                    break;
                                } else {
                                    logger.info(">>>>>>>>>>> xxl-job 注册失败, 注册参数:{}, 注册结果:{}", new Object[]{registryParam, registryResult});
                                }
                            } catch (Throwable e) {
                                logger.info(">>>>>>>>>>> xxl-job 注册异常, 注册参数:{}", registryParam, e);
                            }
                        }
                    } catch (Throwable e) {
                        if (!toStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }

                    try {
                        // 休眠等待下一次心跳
                        if (!toStop) {
                            TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
                        }
                    } catch (Throwable e) {
                        if (!toStop) {
                            logger.warn(">>>>>>>>>>> xxl-job, 执行器注册线程被中断, 错误信息:{}", e.getMessage());
                        }
                    }
                }

                // 执行器注销：发送注销请求
                try {
                    // 构建注销参数
                    RegistryParam registryParam = new RegistryParam(RegistryConfig.RegistType.EXECUTOR.name(), appname, address);
                    // 遍历所有配置的调度中心地址
                    for (AdminBiz adminBiz: XxlJobExecutor.getAdminBizList()) {
                        try {
                            // 发送注销请求
                            ReturnT<String> registryResult = adminBiz.registryRemove(registryParam);
                            // 注销成功则跳出循环，注销失败则尝试下一个调度中心地址
                            if (registryResult!=null && ReturnT.SUCCESS_CODE == registryResult.getCode()) {
                                registryResult = ReturnT.SUCCESS;
                                logger.info(">>>>>>>>>>> xxl-job 注销成功, 注销参数:{}, 注销结果:{}", new Object[]{registryParam, registryResult});
                                break;
                            } else {
                                logger.info(">>>>>>>>>>> xxl-job 注销失败, 注销参数:{}, 注销结果:{}", new Object[]{registryParam, registryResult});
                            }
                        } catch (Throwable e) {
                            if (!toStop) {
                                logger.info(">>>>>>>>>>> xxl-job 注销异常, 注销参数:{}", registryParam, e);
                            }
                        }
                    }
                } catch (Throwable e) {
                    if (!toStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, 执行器注册线程已销毁.");
            }
        });
        // 设置为守护线程
        registryThread.setDaemon(true);
        // 设置线程名称
        registryThread.setName("xxl-job, executor ExecutorRegistryThread");
        // 启动线程
        registryThread.start();
    }

    /**
     * 停止注册线程
     * 
     * 执行流程：
     * 1. 设置停止标志
     * 2. 中断线程
     * 3. 等待线程结束
     * 
     * 注意：该方法会触发注销流程，向调度中心发送注销请求
     */
    public void toStop() {
        toStop = true;

        // 中断线程并等待结束
        if (registryThread != null) {
            registryThread.interrupt();
            try {
                registryThread.join();
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);
            }
        }
    }


}
