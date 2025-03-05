package com.xxl.job.admin.core.route.strategy;

import com.xxl.job.admin.core.route.ExecutorRouter;
import com.xxl.job.admin.core.scheduler.XxlJobScheduler;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.model.ExecutorStatus;
import com.xxl.job.core.biz.model.IdleBeatParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;

import java.io.Serial;
import java.util.List;

public class IdleThreadBasedTaskAllocator extends ExecutorRouter {

    @Override
    public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
        StringBuffer idleBeatResultSB = new StringBuffer();
        for (String address : addressList) {
            ReturnT<ExecutorStatus> executorStatusResult = null;
            int remainingThreadCount = 0;
            try {
                ExecutorBiz executorBiz = XxlJobScheduler.getExecutorBiz(address);
                executorStatusResult = executorBiz.status();
                ExecutorStatus executorStatusContent = executorStatusResult.getContent();
                System.err.println(executorStatusContent);
                // 拿到客户端的线程信息
                remainingThreadCount = executorStatusContent.getThreadCount() - executorStatusContent.getRunningTaskCount();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return new ReturnT<String>(ReturnT.FAIL_CODE, "" + e);
            }
            idleBeatResultSB.append( (idleBeatResultSB.length()>0)?"<br><br>":"")
                    .append(I18nUtil.getString("jobconf_idleBeat") + "：")
                    .append("<br>address：").append(address)
                    .append("<br>code：").append(executorStatusResult.getCode())
                    .append("<br>msg：").append(executorStatusResult.getMsg());
            // 如果还存在可调度的线程
            if (remainingThreadCount > 0) {
                logger.info("当前可调度的节点：{}，节点支持的最大线程数：{}，节点正在运行的线程数：{}，节点可用线程数：{}", address, executorStatusResult.getContent().getThreadCount(), executorStatusResult.getContent().getRunningTaskCount(), remainingThreadCount);
                return new ReturnT<String>(ReturnT.SUCCESS_CODE, idleBeatResultSB.toString(), address);
            } else {
                System.err.println("serr");
            }
        }

        return new ReturnT<String>(ReturnT.FAIL_CODE, idleBeatResultSB.toString());
    }

}
