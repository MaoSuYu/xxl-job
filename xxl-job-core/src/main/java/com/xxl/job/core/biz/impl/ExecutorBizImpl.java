package com.xxl.job.core.biz.impl;

import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.model.*;
import com.xxl.job.core.context.JobThreadContext;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import com.xxl.job.core.enums.ThreadConstant;
import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.glue.GlueFactory;
import com.xxl.job.core.glue.GlueTypeEnum;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.impl.GlueJobHandler;
import com.xxl.job.core.handler.impl.ScriptJobHandler;
import com.xxl.job.core.log.XxlJobFileAppender;
import com.xxl.job.core.thread.JobThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * 执行器实现类
 * <p>
 * 该类实现了ExecutorBiz接口，是执行器核心功能的具体实现。
 * 主要职责包括：
 * 1. 处理调度中心的心跳检测
 * 2. 接收并执行调度中心下发的任务
 * 3. 管理任务线程的生命周期
 * 4. 处理任务日志的查询请求
 */
public class ExecutorBizImpl implements ExecutorBiz {
    private static Logger logger = LoggerFactory.getLogger(ExecutorBizImpl.class);

    /**
     * 心跳检测
     * <p>
     * 用于调度中心检测执行器是否在线
     * 简单返回成功标识，代表执行器正常运行
     */
    @Override
    public ReturnT<String> beat() {
        return ReturnT.SUCCESS;
    }

    /**
     * 空闲检测
     *
     * @param idleBeatParam 空闲检测参数，包含任务ID
     * @return ReturnT<String> 检测结果
     * <p>
     * 检查指定任务是否处于运行中或者在队列中
     * 如果任务正在运行或在队列中等待，返回失败，表示任务非空闲
     * 否则返回成功，表示任务处于空闲状态
     */
    @Override
    public ReturnT<String> idleBeat(IdleBeatParam idleBeatParam) {

        // 初始化运行状态标志，默认假设任务不在运行中
        boolean isRunningOrHasQueue = false;
        // 根据任务ID获取对应的执行线程，确保任务的唯一性和状态可控
        JobThread jobThread = XxlJobExecutor.loadJobThread(idleBeatParam.getJobId());
        // 检查线程是否存在且正在运行或队列中有待执行的任务，确保任务不会重复执行
        if (jobThread != null && jobThread.isRunningOrHasQueue()) {
            isRunningOrHasQueue = true;
        }

        // 如果任务正在运行或队列中有任务，返回失败状态，避免重复调度
        if (isRunningOrHasQueue) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "job thread is running or has trigger queue.");
        }
        return ReturnT.SUCCESS;
    }

    /**
     * 执行任务
     *
     * @param triggerParam 触发参数，包含任务执行的所有必要信息
     * @return ReturnT<String> 执行结果
     * <p>
     * 这是执行器的核心方法，负责：
     * 1. 根据任务类型（Bean、Glue、Script）加载或创建相应的任务处理器
     * 2. 处理任务阻塞策略（丢弃后续、覆盖之前）
     * 3. 维护任务线程的生命周期
     * 4. 将触发参数推送到任务线程的执行队列
     * <p>
     * 执行流程：
     * 1. 检查已存在的JobThread和JobHandler
     * 2. 根据GlueType创建或获取JobHandler
     * 3. 根据阻塞策略处理任务并发
     * 4. 注册或复用JobThread
     * 5. 将任务推送到执行队列
     */
    @Override
    public ReturnT<String> run(TriggerParam triggerParam) {
        // 获取已存在的任务线程和处理器，确保任务的连续性和状态管理
        JobThread jobThread = XxlJobExecutor.loadJobThread(triggerParam.getJobId());
        // 如果线程存在则获取其处理器，否则为null，确保处理器的正确性
        IJobHandler jobHandler = jobThread != null ? jobThread.getHandler() : null;
        // 用于记录需要移除旧线程的原因，便于调试和日志记录
        String removeOldReason = null;

        // 根据任务参数获取任务类型，确保任务处理的灵活性和扩展性
        GlueTypeEnum glueTypeEnum = GlueTypeEnum.match(triggerParam.getGlueType());
        if (GlueTypeEnum.BEAN == glueTypeEnum) {
            // BEAN类型任务：直接使用Spring容器中的Bean作为任务处理器，利用Spring的依赖注入特性

            // 加载新的任务处理器，确保处理器的最新性和正确性
            IJobHandler newJobHandler = XxlJobExecutor.loadJobHandler(triggerParam.getExecutorHandler());

            // 如果已存在的处理器与新的处理器不同，需要终止旧线程，避免资源浪费和状态不一致
            if (jobThread != null && jobHandler != newJobHandler) {
                removeOldReason = "change jobhandler or glue type, and terminate the old job thread.";
                // 清空当前线程和处理器，确保新的任务环境
                jobThread = null;
                jobHandler = null;
            }

            // 如果没有处理器，使用新加载的处理器，确保任务能够正常执行
            if (jobHandler == null) {
                jobHandler = newJobHandler;
                if (jobHandler == null) {
                    // 如果处理器仍为空，说明没有找到对应的处理器，返回错误，确保系统的健壮性
                    return new ReturnT<String>(ReturnT.FAIL_CODE, "job handler [" + triggerParam.getExecutorHandler() + "] not found.");
                }
            }

        } else if (GlueTypeEnum.GLUE_GROOVY == glueTypeEnum) {
            // Groovy动态脚本任务处理，支持动态更新和灵活的任务逻辑

            // 检查已存在的线程是否需要更新，确保任务逻辑的最新性
            if (jobThread != null &&
                    !(jobThread.getHandler() instanceof GlueJobHandler
                            && ((GlueJobHandler) jobThread.getHandler()).getGlueUpdatetime() == triggerParam.getGlueUpdatetime())) {
                // 如果脚本已更新，需要终止旧线程，避免执行旧逻辑
                removeOldReason = "change job source or glue type, and terminate the old job thread.";
                jobThread = null;
                jobHandler = null;
            }

            // 如果没有处理器，创建新的Groovy处理器，确保任务能够执行最新的脚本逻辑
            if (jobHandler == null) {
                try {
                    // 加载并实例化Groovy脚本，支持动态任务逻辑
                    IJobHandler originJobHandler = GlueFactory.getInstance().loadNewInstance(triggerParam.getGlueSource());
                    jobHandler = new GlueJobHandler(originJobHandler, triggerParam.getGlueUpdatetime());
                } catch (Exception e) {
                    // 脚本加载失败记录日志并返回错误，确保问题可追溯
                    logger.error(e.getMessage(), e);
                    return new ReturnT<String>(ReturnT.FAIL_CODE, e.getMessage());
                }
            }
        } else if (glueTypeEnum != null && glueTypeEnum.isScript()) {
            // 脚本类型任务（Shell、Python等），支持多种脚本语言

            // 检查脚本是否需要更新，确保执行最新的脚本
            if (jobThread != null &&
                    !(jobThread.getHandler() instanceof ScriptJobHandler
                            && ((ScriptJobHandler) jobThread.getHandler()).getGlueUpdatetime() == triggerParam.getGlueUpdatetime())) {
                // 脚本已更新，终止旧线程，避免执行旧脚本
                removeOldReason = "change job source or glue type, and terminate the old job thread.";
                jobThread = null;
                jobHandler = null;
            }

            // 如果没有处理器，创建新的脚本处理器，确保任务能够执行
            if (jobHandler == null) {
                jobHandler = new ScriptJobHandler(triggerParam.getJobId(), triggerParam.getGlueUpdatetime(), triggerParam.getGlueSource(), GlueTypeEnum.match(triggerParam.getGlueType()));
            }
        } else {
            // 不支持的任务类型，返回错误，确保系统的健壮性
            return new ReturnT<String>(ReturnT.FAIL_CODE, "glueType[" + triggerParam.getGlueType() + "] is not valid.");
        }

        // 处理任务阻塞策略，确保任务调度的合理性
        if (jobThread != null) {
            // 获取阻塞策略，确保任务调度的灵活性
            ExecutorBlockStrategyEnum blockStrategy = ExecutorBlockStrategyEnum.match(triggerParam.getExecutorBlockStrategy(), null);
            if (ExecutorBlockStrategyEnum.DISCARD_LATER == blockStrategy) {
                // 丢弃后续调度：如果任务在执行或队列中有任务，直接返回失败，避免资源浪费
                if (jobThread.isRunningOrHasQueue()) {
                    return new ReturnT<String>(ReturnT.FAIL_CODE, "block strategy effect：" + ExecutorBlockStrategyEnum.DISCARD_LATER.getTitle());
                }
            } else if (ExecutorBlockStrategyEnum.COVER_EARLY == blockStrategy) {
                // 覆盖之前调度：如果任务在执行，终止当前任务，确保新任务的优先级
                if (jobThread.isRunningOrHasQueue()) {
                    removeOldReason = "block strategy effect：" + ExecutorBlockStrategyEnum.COVER_EARLY.getTitle();
                    jobThread = null;
                }
            } else {
                // 串行执行：将任务加入队列，确保任务按顺序执行
            }
        }

        // 如果没有可用线程，注册新线程，确保任务能够被执行
        if (jobThread == null) {
            jobThread = XxlJobExecutor.registJobThread(triggerParam.getJobId(), jobHandler, removeOldReason);
        }

        // 将触发参数推送到任务线程的执行队列，确保任务能够被调度执行
        ReturnT<String> pushResult = jobThread.pushTriggerQueue(triggerParam);
        return pushResult;
    }

    /**
     * 终止任务
     *
     * @param killParam 终止参数，包含要终止的任务ID
     * @return ReturnT<String> 终止结果
     * <p>
     * 用于强制终止正在运行的任务：
     * 1. 根据任务ID获取对应的JobThread
     * 2. 如果线程存在，则移除该线程并终止任务
     * 3. 如果线程不存在，返回任务已终止的提示
     */
    @Override
    public ReturnT<String> kill(KillParam killParam) {
        // 根据任务ID获取任务线程，确保任务的唯一性和状态可控
        JobThread jobThread = XxlJobExecutor.loadJobThread(killParam.getJobId());
        if (jobThread != null) {
            // 如果线程存在，从执行器中移除并终止线程，释放资源
            XxlJobExecutor.removeJobThread(killParam.getJobId(), "scheduling center kill job.");
            return ReturnT.SUCCESS;
        }

        // 如果线程不存在，返回已终止的提示，确保系统的健壮性
        return new ReturnT<String>(ReturnT.SUCCESS_CODE, "job thread already killed.");
    }

    /**
     * 查询任务日志
     *
     * @param logParam 日志查询参数，包含日志ID、时间戳和起始行号
     * @return ReturnT<LogResult> 日志查询结果
     * <p>
     * 用于读取任务执行的日志信息：
     * 1. 根据时间和日志ID构建日志文件名
     * 2. 从指定行号开始读取日志内容
     * 3. 返回日志内容和下一次读取的起始行号
     */
    @Override
    public ReturnT<LogResult> log(LogParam logParam) {
        // 根据时间和日志ID生成日志文件名，确保日志的唯一性和可追溯性
        String logFileName = XxlJobFileAppender.makeLogFileName(new Date(logParam.getLogDateTim()), logParam.getLogId());

        // 从指定行号开始读取日志内容，支持日志的增量读取
        LogResult logResult = XxlJobFileAppender.readLog(logFileName, logParam.getFromLineNum());
        return new ReturnT<LogResult>(logResult);
    }

    @Override
    public ReturnT<ExecutorStatus> status() {
        ExecutorStatus status = new ExecutorStatus(
                ThreadConstant.MAX_THREAD_COUNT,
                XxlJobExecutor.getRunningTaskCount(),
                XxlJobExecutor.getPendingTaskCount()
        );
        return new ReturnT<ExecutorStatus>(status);
    }

    @Override
    public ReturnT<String> forceKill(Long jobId) {
        logger.info("开始执行强制终止任务, 任务ID: {}", jobId);

        // 获取任务线程
        JobThread jobThread = XxlJobExecutor.loadJobThread(jobId);
        if (jobThread == null) {
            logger.info("任务线程不存在，任务ID: {}, 可能已经停止", jobId);
            return new ReturnT<>(ReturnT.SUCCESS_CODE, "任务线程不存在，可能已经停止");
        }

        logger.info("获取到任务线程, 任务ID: {}, 线程名称: {}", jobId, jobThread.getName());

        Thread thread = JobThreadContext.getJobThreadContextMap().get(jobId);
        if (null != thread) {
            logger.info("中断任务上下文线程, 任务ID: {}, 线程名称: {}", jobId, thread.getName());
            thread.interrupt();
        }

        // 停止任务线程
        logger.info("开始停止任务线程, 任务ID: {}", jobId);
        jobThread.toStop("force kill by admin");
        jobThread.interrupt();
        XxlJobExecutor.removeJobThread(jobId, "用户强行中断！");
        JobThreadContext.removeJobThread(jobId);
        logger.info("强制终止任务完成, 任务ID: {}", jobId);
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> offline(String id) {
        return null;
    }

}
