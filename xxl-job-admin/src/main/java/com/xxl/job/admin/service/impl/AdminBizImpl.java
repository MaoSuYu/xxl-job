package com.xxl.job.admin.service.impl;

import com.xxl.job.admin.core.thread.JobCompleteHelper;
import com.xxl.job.admin.core.thread.JobRegistryHelper;
import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.ThreadInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author xuxueli 2017-07-27 21:54:20
 */
@Service
public class AdminBizImpl implements AdminBiz {

    private static final Logger logger = LoggerFactory.getLogger(AdminBizImpl.class);

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
                
        // 处理线程信息的逻辑
        // 这里可以将线程信息存储到数据库或进行其他处理
        return ReturnT.SUCCESS;
    }

}
