package com.xxl.job.admin.core.scheduler;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.thread.*;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.client.ExecutorBizClient;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * XXL-JOB 调度器核心类
 * 负责调度系统的初始化、启动和销毁
 * 被 XxlJobAdminConfig 通过 Spring 容器初始化时调用
 * 
 * @author xuxueli 2018-10-28 00:18:17
 */
public class XxlJobScheduler  {
    private static final Logger logger = LoggerFactory.getLogger(XxlJobScheduler.class);

    /**
     * 初始化调度器
     * 由 XxlJobAdminConfig.afterPropertiesSet() 在 Spring 容器启动时调用
     * 按顺序启动各个组件
     *
     * @throws Exception 初始化过程中的异常
     */
    public void init() throws Exception {
        // 初始化国际化资源
        initI18n();

        // 启动任务触发线程池
        // 用于执行任务触发操作
        JobTriggerPoolHelper.toStart();

        // 启动执行器注册监控线程
        // 负责检测执行器的注册状态
        JobRegistryHelper.getInstance().start();

        // 启动失败任务监控线程
        // 负责检测执行失败的任务并进行重试
        JobFailMonitorHelper.getInstance().start();

        // 启动任务完成监控线程
        // 依赖于触发线程池，处理任务执行结果
        JobCompleteHelper.getInstance().start();

        // 启动任务日志报告线程
        // 统计任务执行日志
        JobLogReportHelper.getInstance().start();

        // 启动任务调度线程
        // 依赖于触发线程池，负责任务的调度触发
        JobScheduleHelper.getInstance().start();

        logger.info(">>>>>>>>> init xxl-job admin success.");
    }

    /**
     * 销毁调度器
     * 由 XxlJobAdminConfig.destroy() 在 Spring 容器关闭时调用
     * 按照与初始化相反的顺序停止各个组件
     *
     * @throws Exception 销毁过程中的异常
     */
    public void destroy() throws Exception {
        // 停止调度线程
        JobScheduleHelper.getInstance().toStop();

        // 停止日志报告线程
        JobLogReportHelper.getInstance().toStop();

        // 停止任务完成监控线程
        JobCompleteHelper.getInstance().toStop();

        // 停止失败任务监控线程
        JobFailMonitorHelper.getInstance().toStop();

        // 停止执行器注册监控线程
        JobRegistryHelper.getInstance().toStop();

        // 停止任务触发线程池
        JobTriggerPoolHelper.toStop();
    }

    /**
     * 初始化国际化资源
     * 在 init() 方法中被调用
     * 主要用于初始化执行器阻塞策略的国际化显示
     */
    private void initI18n(){
        for (ExecutorBlockStrategyEnum item:ExecutorBlockStrategyEnum.values()) {
            item.setTitle(I18nUtil.getString("jobconf_block_".concat(item.name())));
        }
    }

    // ---------------------- executor-client ----------------------
    private static ConcurrentMap<String, ExecutorBiz> executorBizRepository = new ConcurrentHashMap<String, ExecutorBiz>();
    public static ExecutorBiz getExecutorBiz(String address) throws Exception {
        // valid
        if (address==null || address.trim().length()==0) {
            return null;
        }

        // load-cache
        address = address.trim();
        ExecutorBiz executorBiz = executorBizRepository.get(address);
        if (executorBiz != null) {
            return executorBiz;
        }

        // set-cache
        executorBiz = new ExecutorBizClient(address,
                XxlJobAdminConfig.getAdminConfig().getAccessToken(),
                XxlJobAdminConfig.getAdminConfig().getTimeout());

        executorBizRepository.put(address, executorBiz);
        return executorBiz;
    }

}
