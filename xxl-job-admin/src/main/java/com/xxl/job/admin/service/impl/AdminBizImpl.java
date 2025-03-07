package com.xxl.job.admin.service.impl;

import com.alibaba.fastjson.JSON;
import com.xxl.job.admin.core.thread.JobCompleteHelper;
import com.xxl.job.admin.core.thread.JobRegistryHelper;
import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.ThreadInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author xuxueli 2017-07-27 21:54:20
 */
@Service
public class AdminBizImpl implements AdminBiz {

    private static final Logger logger = LoggerFactory.getLogger(AdminBizImpl.class);

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

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
        logger.info("接收到执行器线程信息上报，共 {} 个线程", threadInfoList.size());
        logger.debug("线程详细信息: {}",
                threadInfoList.stream()
                        .map(info -> String.format("线程ID:%s(状态:%s)", info.getJobId(), info.getThreadState()))
                        .reduce((a, b) -> a + ", " + b)
                        .orElse("无"));
        if (!CollectionUtils.isEmpty(threadInfoList)) {
            for (ThreadInfo threadInfo : threadInfoList) {
                // xxl-job-executor-sample:172.27.208.1_1000:4
                StringBuilder redisKey = new StringBuilder();
                redisKey.append(threadInfo.getAppName());
                redisKey.append(":");
                redisKey.append(threadInfo.getIp());
                redisKey.append("_");
                redisKey.append(threadInfo.getPort());
                redisKey.append(":");
                redisKey.append(threadInfo.getJobId());

                // 将线程信息存储到Redis中，设置过期时间为3秒
                stringRedisTemplate.opsForValue().set(
                        redisKey.toString(),
                        JSON.toJSONString(threadInfo),
                        10,
                        TimeUnit.SECONDS
                );

                logger.debug("线程信息已存入Redis，key={}, value={}, 过期时间=10秒",
                        redisKey.toString(), threadInfo.getThreadState());
            }
        }
        return ReturnT.SUCCESS;
    }

}
