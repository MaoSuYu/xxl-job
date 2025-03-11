package com.xxl.job.admin.service.impl;

import com.alibaba.fastjson.JSON;
import com.xxl.job.admin.core.thread.JobCompleteHelper;
import com.xxl.job.admin.core.thread.JobRegistryHelper;
import com.xxl.job.admin.dao.XxlJobInfoDao;
import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.ThreadInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author xuxueli 2017-07-27 21:54:20
 */
@Service
public class AdminBizImpl implements AdminBiz {

    private static final Logger logger = LoggerFactory.getLogger(AdminBizImpl.class);

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private XxlJobInfoDao xxlJobInfoDao;

    @Override
    public ReturnT<String> callback(List<HandleCallbackParam> callbackParamList) {
        return JobCompleteHelper.getInstance().callback(callbackParamList);
    }

    @Override
    public ReturnT<String> registry(RegistryParam registryParam) {
        return JobRegistryHelper.getInstance().registry(registryParam);
    }

    @Override
    public ReturnT<String> registryRemove(RegistryParam registryParam) {
        return JobRegistryHelper.getInstance().registryRemove(registryParam);
    }

    @Override
    public ReturnT<String> reportRunningThreads(List<ThreadInfo> threadInfoList) {
        if (CollectionUtils.isEmpty(threadInfoList)) {
            return ReturnT.SUCCESS;
        }

        logger.info("接收到执行器线程信息上报，共 {} 个线程", threadInfoList.size());

        // 记录线程详细信息（使用条件判断避免不必要的字符串拼接）
        logger.debug("线程详细信息: {}",
                threadInfoList.stream()
                        .map(info -> String.format("线程ID:%s(状态:%s)", info.getJobId(), info.getThreadState()))
                        .collect(Collectors.joining(", ")));

        // 直接提取所有jobId，不需要类型转换
        List<Long> jobIds = threadInfoList.stream()
                .map(ThreadInfo::getJobId)
                .distinct() // 去重，减少数据库查询量
                .collect(Collectors.toList());

        // 批量查询所有jobId对应的title
        Map<Long, String> jobTitleMap = new HashMap<>(jobIds.size() * 4 / 3 + 1); // 预分配合适的容量
        if (!CollectionUtils.isEmpty(jobIds)) {
            List<Map<String, Object>> titleList = xxlJobInfoDao.batchGetGroupTitleByJobIds(jobIds);
            if (!CollectionUtils.isEmpty(titleList)) {
                titleList.forEach(map -> {
                    Long jobId = ((Number) map.get("jobId")).longValue();
                    String title = (String) map.get("title");
                    jobTitleMap.put(jobId, title);
                });
            }
        }

        // 使用Redis的管道操作批量写入数据
        stringRedisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (ThreadInfo threadInfo : threadInfoList) {
                // 获取任务对应的执行器组title
                String title = jobTitleMap.getOrDefault(threadInfo.getJobId(), "未知执行器");
                threadInfo.setExecutorTitle(title);

                // 构建redisKey，使用StringBuilder的预分配容量
                String redisKey = new StringBuilder(128)
                        .append("current_running_task:")
                        .append(threadInfo.getAppName())
                        .append(':')
                        .append(threadInfo.getIp())
                        .append('_')
                        .append(threadInfo.getPort())
                        .append(':')
                        .append(threadInfo.getJobId())
                        .toString();

                // 将线程信息序列化为JSON
                String jsonValue = JSON.toJSONString(threadInfo);

                // 使用管道批量写入Redis (使用字节数组方式)
                byte[] keyBytes = redisKey.getBytes();
                byte[] valueBytes = jsonValue.getBytes();
                connection.setEx(keyBytes, 10, valueBytes);

                // 记录每个线程的处理情况（使用条件判断避免不必要的字符串拼接）
                if (logger.isDebugEnabled()) {
                    logger.debug("线程信息已存入Redis，key={}, title={}, state={}, 过期时间=10秒",
                            redisKey, title, threadInfo.getThreadState());
                }
            }
            return null;
        });

        logger.info("成功将 {} 个线程信息批量存入Redis，过期时间=10秒", threadInfoList.size());

        return ReturnT.SUCCESS;
    }
}
