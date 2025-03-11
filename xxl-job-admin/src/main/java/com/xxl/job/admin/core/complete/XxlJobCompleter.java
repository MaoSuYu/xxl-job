package com.xxl.job.admin.core.complete;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.model.XxlJobLog;
import com.xxl.job.admin.core.thread.JobTriggerPoolHelper;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.context.XxlJobContext;
import com.xxl.job.core.enums.ExecutionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ObjectUtils;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;

/**
 * 任务完成处理器
 *
 * 该类负责处理任务执行完成后的相关操作，包括：
 * 1. 更新任务执行日志的处理信息
 * 2. 处理任务执行成功后的子任务触发
 * 3. 完成任务的收尾工作
 *
 * @author xuxueli 2020-10-30 20:43:10
 */
public class XxlJobCompleter {
    /**
     * 日志记录器
     */
    private static Logger logger = LoggerFactory.getLogger(XxlJobCompleter.class);

    /**
     * 任务完成处理的统一入口
     *
     * 该方法是任务完成处理的主要入口，负责：
     * 1. 调用finishJob完成任务的收尾工作（如触发子任务等）
     * 2. 截断过长的处理消息，避免数据库存储问题
     * 3. 更新任务日志的处理信息到数据库
     *
     * 注意：该方法确保任务完成处理只执行一次
     *
     * @param xxlJobLog 任务日志对象，包含任务执行的相关信息
     * @return 数据库更新影响的行数，通常返回1表示更新成功
     */
    public static int updateHandleInfoAndFinish(XxlJobLog xxlJobLog) {

        // 调用finishJob方法完成任务的收尾工作，如触发子任务等
        finishJob(xxlJobLog);

        // 处理消息长度限制：数据库text字段最大64kb，这里限制为15000字符，避免数据库存储问题
        if (xxlJobLog.getHandleMsg().length() > 15000) {
            xxlJobLog.setHandleMsg( xxlJobLog.getHandleMsg().substring(0, 15000) );
        }
        // 更新子任务的状态信息等。。
        int statusCode = (xxlJobLog.getHandleCode() == 200) ? ExecutionStatus.SUCCESS.getCode() : ExecutionStatus.FAILED.getCode();
        XxlJobAdminConfig.getAdminConfig().getXxlJobShardingInfoDao().updateExecuteInfo(statusCode,xxlJobLog.getJobId());

        // 调用DAO层方法更新任务日志的处理信息到数据库，并返回影响的行数
        return XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().updateHandleInfo(xxlJobLog);
    }


    /**
     * 完成任务的收尾工作
     *
     * 该方法负责处理任务执行完成后的后续操作，主要包括：
     * 1. 如果任务执行成功，检查并触发配置的子任务
     * 2. 为任务日志添加子任务触发的相关信息
     * 3. 处理定时触发下一次任务的逻辑（待实现）
     *
     * @param xxlJobLog 任务日志对象，包含任务执行的相关信息
     */
    private static void finishJob(XxlJobLog xxlJobLog){

        // 1、处理任务执行成功的情况，触发子任务
        String triggerChildMsg = null;
        // 检查任务是否执行成功
        if (XxlJobContext.HANDLE_CODE_SUCCESS == xxlJobLog.getHandleCode()) {
            // 根据任务ID加载任务信息
            XxlJobInfo xxlJobInfo = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().loadById(xxlJobLog.getJobId());
            // 检查任务信息是否存在且配置了子任务ID
            if (xxlJobInfo!=null && xxlJobInfo.getChildJobId()!=null && xxlJobInfo.getChildJobId().trim().length()>0) {
                // 初始化子任务触发的消息头部，使用国际化资源获取文本
                triggerChildMsg = "<br><br><span style=\"color:#00c0ef;\" > >>>>>>>>>>>"+ I18nUtil.getString("jobconf_trigger_child_run") +"<<<<<<<<<<< </span><br>";

                // 将子任务ID字符串按逗号分割成数组
                String[] childJobIds = xxlJobInfo.getChildJobId().split(",");
                // 检查子任务ID数组是否有效
                if (!ObjectUtils.isEmpty(childJobIds)&&childJobIds.length!=0){
                    // 将子任务ID数组转换为List
                    List<String> list = Arrays.asList(childJobIds);
                    // 批量加载子任务信息
                    List<XxlJobInfo> childs = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().loadByIds(list);
                    // 遍历所有子任务
                    for (int i = 0; i < childs.size(); i++) {
                        XxlJobInfo childxxlJobInfo = childs.get(i);
                        // 检查子任务的子任务ID是否为数字，如果是则转换为整数，否则设为-1
                        int childJobId = (childxxlJobInfo.getChildJobId()!=null && childxxlJobInfo.getChildJobId().trim().length()>0 && isNumeric(childxxlJobInfo.getChildJobId()))?Integer.valueOf(childxxlJobInfo.getChildJobId()):-1;
                        // 检查子任务ID是否有效（大于0）
                        if (childJobId > 0) {
                            // 验证子任务ID不是当前任务ID，避免循环触发
                            if (childJobId == xxlJobLog.getJobId()) {
                                logger.debug(">>>>>>>>>>> xxl-job, XxlJobCompleter-finishJob ignore childJobId,  childJobId {} is self.", childJobId);
                                continue;
                            }

                            // 通过任务触发池帮助类触发子任务，使用分片触发方式
                            // 参数说明：子任务信息、触发类型为父任务触发、分片索引为-1（不分片）、无分片总数、无分片参数、无执行参数
                            JobTriggerPoolHelper.triggerSharding((long)childJobId, TriggerTypeEnum.PARENT, -1, null, null, null,childxxlJobInfo.getIsAutomatic());
                            // 设置触发结果为成功
                            ReturnT<String> triggerChildResult = ReturnT.SUCCESS;

                            // 添加子任务触发结果消息
                            // 使用MessageFormat格式化国际化消息，包含：当前子任务索引、子任务总数、子任务ID、触发结果状态、触发结果消息
                            triggerChildMsg += MessageFormat.format(I18nUtil.getString("jobconf_callback_child_msg1"),
                                    (i+1),
                                    childJobIds.length,
                                    childJobIds[i],
                                    (triggerChildResult.getCode()==ReturnT.SUCCESS_CODE?I18nUtil.getString("system_success"):I18nUtil.getString("system_fail")),
                                    triggerChildResult.getMsg());
                        } else {
                            // 子任务ID无效的情况，添加无效子任务ID的消息
                            // 包含：当前子任务索引、子任务总数、无效的子任务ID
                            triggerChildMsg += MessageFormat.format(I18nUtil.getString("jobconf_callback_child_msg2"),
                                    (i+1),
                                    childJobIds.length,
                                    childJobIds[i]);
                        }
                    }
                }
            }
        }

        // 如果有子任务触发消息，将其添加到任务日志的处理消息中
        if (triggerChildMsg != null) {
            xxlJobLog.setHandleMsg( xxlJobLog.getHandleMsg() + triggerChildMsg );
        }

        // 2、固定延迟触发下一次任务的逻辑（待实现）
        // on the way

    }

    /**
     * 判断字符串是否为数字
     *
     * 该方法用于检查给定的字符串是否可以转换为整数，主要用于验证子任务ID的有效性。
     *
     * @param str 需要检查的字符串
     * @return 如果字符串可以转换为整数则返回true，否则返回false
     */
    private static boolean isNumeric(String str){
        try {
            // 尝试将字符串转换为整数
            int result = Integer.valueOf(str);
            // 转换成功则返回true
            return true;
        } catch (NumberFormatException e) {
            // 转换失败（抛出NumberFormatException异常）则返回false
            return false;
        }
    }

}
