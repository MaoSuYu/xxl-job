package com.xxl.job.admin.core.route.strategy;

import cn.hutool.core.util.StrUtil;
import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobGroup;
import com.xxl.job.admin.core.model.XxlJobTaskExecutorMapping;
import com.xxl.job.admin.core.route.ExecutorRouter;
import com.xxl.job.admin.core.scheduler.XxlJobScheduler;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.admin.dao.XxlJobGroupDao;
import com.xxl.job.admin.dao.XxlJobTaskExecutorMappingMapper;
import com.xxl.job.admin.util.SpringContextUtil;
import com.xxl.job.core.biz.model.ExecutorStatus;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.CollectionUtils;

import java.sql.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;

public class IdleThreadBasedTaskAllocator extends ExecutorRouter {

    private static ThreadPoolTaskExecutor taskExecutor;
    private static XxlJobTaskExecutorMappingMapper xxlJobTaskExecutorMappingMapper;
    private static XxlJobGroupDao xxlJobGroupDao;

    static {
        // 初始化线程池
        taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(1);
        taskExecutor.setMaxPoolSize(3);
        taskExecutor.setQueueCapacity(20);
        taskExecutor.setThreadNamePrefix("TaskExecutorMapper-");
        taskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        taskExecutor.initialize();

        // 获取Mapper
        xxlJobTaskExecutorMappingMapper = SpringContextUtil.getBean(XxlJobTaskExecutorMappingMapper.class);
        xxlJobGroupDao = SpringContextUtil.getBean(XxlJobGroupDao.class);
    }

    /**
     * 异步更新任务执行器映射
     */
    private void asyncUpdateTaskExecutorMapping(Long jobId, String executorAddress) {
        taskExecutor.execute(() -> {
            try {
                List<Map<String, Object>> maps = xxlJobGroupDao.selectByAddressList(executorAddress);
                // 执行器组id
                Long groupId = null;
                // 执行器appname
                String appName = null;
                // 执行器名称
                String title = null;

                if (Objects.nonNull(maps)) {
                    Map<String, Object> group = maps.get(0);
                    groupId = (Long) group.get("id");
                    appName = (String) group.get("app_name");
                    title = (String) group.get("title");
                }

                XxlJobTaskExecutorMapping mapping = new XxlJobTaskExecutorMapping();
                mapping.setJobId(jobId);
                mapping.setExecutorAddress(executorAddress);
                mapping.setUpdateTime(new Date(System.currentTimeMillis()));
                // 设置新增字段
                mapping.setGroupId(groupId);
                mapping.setAppName(appName);
                mapping.setTitle(title);

                xxlJobTaskExecutorMappingMapper.saveOrUpdate(mapping);
                logger.info("更新任务执行器映射 [任务ID:{}] [执行器:{}] [执行器组ID:{}] [AppName:{}] [名称:{}]",
                        jobId, executorAddress, groupId, appName, title);
            } catch (Exception e) {
                logger.error("更新任务执行器映射失败 [任务ID:{}] [执行器:{}] [异常:{}]", jobId, executorAddress, e.getMessage());
            }
        });
    }

    /**
     * 根据执行器组选择ip地址
     *
     * @param groupName
     * @return
     */
    public static String choiceIP(String groupName) {
        if (StrUtil.isBlank(groupName)) {
            return null;
        }
        XxlJobGroup xxlJobGroup = XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().loadByAppName(groupName);
        if (Objects.isNull(xxlJobGroup)) {
            return null;
        }
        // 拿到所有IP地址
        List<String> registryList = xxlJobGroup.getRegistryList();
        if (CollectionUtils.isEmpty(registryList)) {
            return null;
        }

        return findIdleExecutor(registryList);
    }

    /**
     * 从执行器列表中查找有空闲线程的执行器IP
     *
     * @param registryList 执行器IP列表
     * @return 有空闲线程的执行器IP，如果没有找到则返回null
     */
    public static String findIdleExecutor(List<String> registryList) {
        if (CollectionUtils.isEmpty(registryList)) {
            return null;
        }

        String ip = null;
        try {
            // 遍历ip，选择ip
            for (String address : registryList) {
                ReturnT<ExecutorStatus> executorStatusResult = XxlJobScheduler.getExecutorBiz(address).status();
                ExecutorStatus content = executorStatusResult.getContent();
                int threadCount = content.getThreadCount();
                int pendingTaskCount = content.getPendingTaskCount();
                int runningTaskCount = content.getRunningTaskCount();
                int remainingThreadCount = threadCount - (pendingTaskCount + runningTaskCount);
                // 有空闲节点直接退出
                if (remainingThreadCount > 0) {
                    ip = address;
                    break;
                }
                // 继续检查下一个IP
            }
            return ip;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
        StringBuffer idleBeatResultSB = new StringBuffer();
        Long jobId = triggerParam.getJobId();
        //logger.info("任务分配开始 [任务ID:{}] [执行器列表:{}]", jobId, addressList);

        for (String address : addressList) {
            ReturnT<ExecutorStatus> executorStatusResult;
            try {
                executorStatusResult = XxlJobScheduler.getExecutorBiz(address).status();
            } catch (Exception e) {
                logger.error("获取执行器状态失败 [任务ID:{}] [地址:{}] [异常:{}]", jobId, address, e.getMessage());
                return new ReturnT<String>(ReturnT.FAIL_CODE, "" + e);
            }

            ExecutorStatus content = executorStatusResult.getContent();
            int threadCount = content.getThreadCount();
            int pendingTaskCount = content.getPendingTaskCount();
            int runningTaskCount = content.getRunningTaskCount();
            int remainingThreadCount = threadCount - (pendingTaskCount + runningTaskCount);

            logger.info("执行器状态 [任务ID:{}] [地址:{}] [总线程:{}] [运行:{}] [等待:{}] [可用:{}]",
                    jobId, address, threadCount, runningTaskCount, pendingTaskCount, remainingThreadCount);

            idleBeatResultSB.append((idleBeatResultSB.length() > 0) ? "<br><br>" : "")
                    .append(I18nUtil.getString("jobconf_idleBeat") + "：")
                    .append("<br>address：").append(address)
                    .append("<br>code：").append(executorStatusResult.getCode())
                    .append("<br>msg：").append(executorStatusResult.getMsg())
                    .append("<br>threadCount：").append(threadCount)
                    .append("<br>runningTaskCount：").append(runningTaskCount)
                    .append("<br>pendingTaskCount：").append(pendingTaskCount)
                    .append("<br>remainingThreadCount：").append(remainingThreadCount);

            if (remainingThreadCount > 0) {
                logger.info("选择执行器 [任务ID:{}] [地址:{}] [可用线程:{}]", jobId, address, remainingThreadCount);
                // 异步更新任务执行器映射
                asyncUpdateTaskExecutorMapping(jobId, address);
                return new ReturnT<String>(address);
            } else {
                logger.info("执行器繁忙转移 [任务ID:{}] [地址:{}] [原因:无可用线程]", jobId, address);
            }
        }

        logger.warn("任务分配失败 [任务ID:{}] [原因:无可用线程]", jobId);
        return new ReturnT<String>(ReturnT.FAIL_CODE, idleBeatResultSB.toString());
    }

}
