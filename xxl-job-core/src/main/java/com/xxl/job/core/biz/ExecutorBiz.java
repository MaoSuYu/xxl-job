package com.xxl.job.core.biz;

import com.xxl.job.core.biz.model.*;

/**
 * Created by xuxueli on 17/3/1.
 */
public interface ExecutorBiz {

    /**
     * beat
     * @return
     */
    public ReturnT<String> beat();

    /**
     * idle beat
     *
     * @param idleBeatParam
     * @return
     */
    public ReturnT<String> idleBeat(IdleBeatParam idleBeatParam);

    /**
     * run
     * @param triggerParam
     * @return
     */
    public ReturnT<String> run(TriggerParam triggerParam);

    /**
     * kill
     * @param killParam
     * @return
     */
    public ReturnT<String> kill(KillParam killParam);

    /**
     * log
     * @param logParam
     * @return
     */
    public ReturnT<LogResult> log(LogParam logParam);

    /**
     * 获取执行器状态
     *
     * @return 执行器状态信息，包括作业线程数、正在执行的任务数和等待执行的任务数
     */
    public ReturnT<ExecutorStatus> status();

    /**
     * 强制打断任务
     * 如果任务在等待队列中，则从队列移除
     * 如果任务正在执行，则强制打断
     *
     * @param jobId 任务ID
     * @return 打断结果
     */
    public ReturnT<String> forceKill(Long jobId);

}
