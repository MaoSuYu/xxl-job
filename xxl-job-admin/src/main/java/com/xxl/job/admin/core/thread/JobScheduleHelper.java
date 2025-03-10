package com.xxl.job.admin.core.thread;

import cn.hutool.extra.spring.SpringUtil;
import com.xuxueli.springbootpriorityqueue.model.Task;
import com.xuxueli.springbootpriorityqueue.service.SortedTaskService;
import com.xuxueli.springbootpriorityqueue.service.TaskService;
import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.cron.CronExpression;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.scheduler.MisfireStrategyEnum;
import com.xxl.job.admin.core.scheduler.ScheduleTypeEnum;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import com.xxl.job.admin.core.util.TimeConverterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 任务调度助手类
 * 负责任务的调度和触发，包含两个核心线程：
 * 1. scheduleThread：负责扫描待调度任务，预读取待执行任务，并将任务推入时间轮
 * 2. ringThread：负责执行时间轮中到期的任务
 * @author xuxueli 2019-05-21
 */
public class JobScheduleHelper {
    private static Logger logger = LoggerFactory.getLogger(JobScheduleHelper.class);

    // 单例模式
    private static JobScheduleHelper instance = new JobScheduleHelper();
    public static JobScheduleHelper getInstance(){
        return instance;
    }

    // 预读取任务的时间阈值，提前5秒读取待执行的任务
    public static final long PRE_READ_MS = 5000;    // pre read

    // 调度线程
    private Thread scheduleThread;
    // 时间轮执行线程
    private Thread ringThread;
    // 调度线程停止标志
    private volatile boolean scheduleThreadToStop = false;
    // 时间轮线程停止标志
    private volatile boolean ringThreadToStop = false;
    // 时间轮数据结构：key为秒数(0-59)，value为该秒需要触发的任务ID列表
    private volatile static Map<Integer, List<Long>> ringData = new ConcurrentHashMap<>();

    public void start(){
        // 初始化并启动调度线程
        scheduleThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // 等待至下一个整秒，保证调度对齐
                    TimeUnit.MILLISECONDS.sleep(5000 - System.currentTimeMillis()%1000 );
                } catch (Throwable e) {
                    if (!scheduleThreadToStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
                logger.info(">>>>>>>>> init xxl-job admin scheduler success.");

                // 预读任务数量 = (快线程池大小 + 慢线程池大小) * 每秒触发频率
                // 假设每个任务触发耗时50ms，则每秒可触发20个任务
                int preReadCount = (XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax() + XxlJobAdminConfig.getAdminConfig().getTriggerPoolSlowMax()) * 20;
                TaskService taskService = (TaskService)SpringUtil.getBean("taskService");
                while (!scheduleThreadToStop) {
                    // 扫描任务
                    long start = System.currentTimeMillis();

                    Connection conn = null;
                    Boolean connAutoCommit = null;
                    PreparedStatement preparedStatement = null;

                    boolean preReadSuc = true;
                    try {
                        // 获取数据库连接
                        conn = XxlJobAdminConfig.getAdminConfig().getDataSource().getConnection();
                        connAutoCommit = conn.getAutoCommit();
                        conn.setAutoCommit(false);

                        // 获取调度锁，保证集群中只有一个节点执行调度
                        preparedStatement = conn.prepareStatement(  "select * from xxl_job_lock where lock_name = 'schedule_lock' for update" );
                        preparedStatement.execute();

                        // 1、预读待调度任务
                        long nowTime = System.currentTimeMillis();
                        List<XxlJobInfo> scheduleList = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleJobQuery(nowTime + PRE_READ_MS, preReadCount);
                        if (scheduleList!=null && scheduleList.size()>0) {
                            // 2、遍历任务列表，推送到时间轮
                            for (XxlJobInfo jobInfo: scheduleList) {
                                // 检查任务是否过期
                                if (nowTime > jobInfo.getTriggerNextTime() + PRE_READ_MS) {
                                    // 2.1、任务过期超过5秒：根据过期策略处理
                                    logger.warn(">>>>>>>>>>> xxl-job, schedule misfire, jobId = " + jobInfo.getId());

                                    // 处理过期任务
                                    MisfireStrategyEnum misfireStrategyEnum = MisfireStrategyEnum.match(jobInfo.getMisfireStrategy(), MisfireStrategyEnum.DO_NOTHING);
                                    if (MisfireStrategyEnum.FIRE_ONCE_NOW == misfireStrategyEnum) {
                                        taskService.addTask(new Task(jobInfo.getId().toString(),jobInfo.getJobDesc(),jobInfo.getJobDesc()),jobInfo.getPriority());
                                        // 立即执行一次
                                        //JobTriggerPoolHelper.triggerSharding(jobInfo, TriggerTypeEnum.MISFIRE, -1, null, null, null);
                                        logger.debug(">>>>>>>>>>> xxl-job, schedule push trigger : jobId = " + jobInfo.getId() );
                                    }

                                    // 刷新下次触发时间
                                    refreshNextValidTime(jobInfo, new Date());

                                } else if (nowTime > jobInfo.getTriggerNextTime()) {
                                    // 2.2、任务过期小于5秒：直接触发一次，并更新下次触发时间

                                    taskService.addTask(new Task(jobInfo.getId().toString(),jobInfo.getJobDesc(),jobInfo.getJobDesc()),jobInfo.getPriority());
                                    //JobTriggerPoolHelper.triggerSharding(jobInfo, TriggerTypeEnum.CRON, -1, null, null, null);
                                    logger.debug(">>>>>>>>>>> xxl-job, schedule push trigger : jobId = " + jobInfo.getId() );

                                    // 刷新下次触发时间
                                    refreshNextValidTime(jobInfo, new Date());

                                    // 如果下次触发时间在5秒内，则再次预读
                                    if (jobInfo.getTriggerStatus()==1 && nowTime + PRE_READ_MS > jobInfo.getTriggerNextTime()) {
                                        // 计算时间轮槽位
                                        int ringSecond = (int)((jobInfo.getTriggerNextTime()/1000)%60);
                                        // 推入时间轮
                                        pushTimeRing(ringSecond, jobInfo.getId());
                                        // 刷新下次触发时间
                                        refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));
                                    }

                                } else {
                                    // 2.3、未过期任务：推入时间轮，等待触发
                                    int ringSecond = (int)((jobInfo.getTriggerNextTime()/1000)%60);
                                    pushTimeRing(ringSecond, jobInfo.getId());
                                    refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));
                                }
                            }

                            // 3、批量更新任务触发信息
                            for (XxlJobInfo jobInfo: scheduleList) {
                                XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleUpdate(jobInfo);
                            }

                        } else {
                            preReadSuc = false;
                        }

                    } catch (Throwable e) {
                        if (!scheduleThreadToStop) {
                            logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread error:{}", e);
                        }
                    } finally {
                        // 提交事务
                        if (conn != null) {
                            try {
                                conn.commit();
                            } catch (Throwable e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                            try {
                                conn.setAutoCommit(connAutoCommit);
                            } catch (Throwable e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                            try {
                                conn.close();
                            } catch (Throwable e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                        }

                        // 关闭PreparedStatement
                        if (null != preparedStatement) {
                            try {
                                preparedStatement.close();
                            } catch (Throwable e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                        }
                    }
                    long cost = System.currentTimeMillis()-start;

                    // 对齐到下一秒
                    if (cost < 1000) {  // 如果扫描耗时小于1秒，则等待到下一秒
                        try {
                            // 预读成功：每秒扫描一次，预读失败：等待5秒后重试
                            TimeUnit.MILLISECONDS.sleep((preReadSuc?1000:PRE_READ_MS) - System.currentTimeMillis()%1000);
                        } catch (Throwable e) {
                            if (!scheduleThreadToStop) {
                                logger.error(e.getMessage(), e);
                            }
                        }
                    }
                }

                logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread stop");
            }
        });
        scheduleThread.setDaemon(true);
        scheduleThread.setName("xxl-job, admin JobScheduleHelper#scheduleThread");
        scheduleThread.start();

        // 初始化并启动时间轮线程
        ringThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!ringThreadToStop) {
                    try {
                        // 等待到下一秒
                        TimeUnit.MILLISECONDS.sleep(1000 - System.currentTimeMillis() % 1000);
                    } catch (Throwable e) {
                        if (!ringThreadToStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }

                    try {
                        // 获取当前秒对应的待触发任务
                        List<Long> ringItemData = new ArrayList<>();
                        int nowSecond = Calendar.getInstance().get(Calendar.SECOND);
                        // 避免处理耗时过长跨过刻度，额外检查前一个刻度的任务
                        for (int i = 0; i < 2; i++) {
                            List<Long> tmpData = ringData.remove( (nowSecond+60-i)%60 );
                            if (tmpData != null) {
                                ringItemData.addAll(tmpData);
                            }
                        }

                        // 触发时间轮中的任务
                        logger.debug(">>>>>>>>>>> xxl-job, time-ring beat : " + nowSecond + " = " + Arrays.asList(ringItemData) );
                        if (ringItemData.size() > 0) {
                            // 执行触发操作
                            List<String> stringList = ringItemData.stream()
                                    .map(String::valueOf)
                                    .collect(Collectors.toList());
                            List<XxlJobInfo> xxlJobInfos = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().loadByIds(stringList);
                            for (XxlJobInfo xxlJobInfo: xxlJobInfos) {
                                // 触发任务
                                TaskService taskService = (TaskService)SpringUtil.getBean("taskService");
                                taskService.addTask(new Task(xxlJobInfo.getId().toString(),xxlJobInfo.getJobDesc(),xxlJobInfo.getJobDesc()),xxlJobInfo.getPriority());
                                //JobTriggerPoolHelper.triggerSharding(xxlJobInfo, TriggerTypeEnum.CRON, -1, null, null, null);
                            }
                            ringItemData.clear();
                        }
                    } catch (Throwable e) {
                        if (!ringThreadToStop) {
                            logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread error:{}", e);
                        }
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread stop");
            }
        });
        ringThread.setDaemon(true);
        ringThread.setName("xxl-job, admin JobScheduleHelper#ringThread");
        ringThread.start();
    }

    /**
     * 刷新任务的下一次触发时间
     * @param jobInfo 任务信息
     * @param fromTime 开始计算的时间点
     */
    private void refreshNextValidTime(XxlJobInfo jobInfo, Date fromTime) {
        try {
            // 计算下一次触发时间
            Date nextValidTime = generateNextValidTime(jobInfo, fromTime);
            if (nextValidTime != null) {
                jobInfo.setTriggerStatus(-1);                               // 标记为已处理
                jobInfo.setTriggerLastTime(jobInfo.getTriggerNextTime());  // 更新上次触发时间
                jobInfo.setTriggerNextTime(nextValidTime.getTime());       // 更新下次触发时间
            } else {
                // 计算下次触发时间失败，停止任务
                jobInfo.setTriggerStatus(0);
                jobInfo.setTriggerLastTime(0);
                jobInfo.setTriggerNextTime(0);
                logger.error(">>>>>>>>>>> xxl-job, refreshNextValidTime fail for job: jobId={}, scheduleType={}, scheduleConf={}",
                        jobInfo.getId(), jobInfo.getScheduleType(), jobInfo.getScheduleConf());
            }
        } catch (Throwable e) {
            // 发生异常，停止任务
            jobInfo.setTriggerStatus(0);
            jobInfo.setTriggerLastTime(0);
            jobInfo.setTriggerNextTime(0);

            logger.error(">>>>>>>>>>> xxl-job, refreshNextValidTime error for job: jobId={}, scheduleType={}, scheduleConf={}",
                    jobInfo.getId(), jobInfo.getScheduleType(), jobInfo.getScheduleConf(), e);
        }
    }

    /**
     * 将任务推入时间轮
     * @param ringSecond 时间轮槽位（秒）
     * @param jobId 任务ID
     */
    private void pushTimeRing(int ringSecond, Long jobId){
        // 获取或创建该秒对应的任务列表
        List<Long> ringItemData = ringData.get(ringSecond);
        if (ringItemData == null) {
            ringItemData = new ArrayList<Long>();
            ringData.put(ringSecond, ringItemData);
        }
        ringItemData.add(jobId);

        logger.debug(">>>>>>>>>>> xxl-job, schedule push time-ring : " + ringSecond + " = " + Arrays.asList(ringItemData) );
    }

    /**
     * 停止调度器
     * 包括停止调度线程和时间轮线程
     */
    public void toStop(){
        // 1、停止调度线程
        scheduleThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);  // 等待线程停止
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        if (scheduleThread.getState() != Thread.State.TERMINATED){
            // 中断并等待线程结束
            scheduleThread.interrupt();
            try {
                scheduleThread.join();
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);
            }
        }

        // 检查时间轮中是否还有待处理的任务
        boolean hasRingData = false;
        if (!ringData.isEmpty()) {
            for (int second : ringData.keySet()) {
                List<Long> tmpData = ringData.get(second);
                if (tmpData!=null && tmpData.size()>0) {
                    hasRingData = true;
                    break;
                }
            }
        }
        // 如果还有待处理任务，等待8秒让任务处理完
        if (hasRingData) {
            try {
                TimeUnit.SECONDS.sleep(8);
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);
            }
        }

        // 停止时间轮线程
        ringThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        if (ringThread.getState() != Thread.State.TERMINATED){
            // 中断并等待线程结束
            ringThread.interrupt();
            try {
                ringThread.join();
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);
            }
        }

        logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper stop");
    }

    /**
     * 根据调度类型和配置生成下次触发时间
     * @param jobInfo 任务信息
     * @param fromTime 开始计算的时间点
     * @return 下次触发时间
     */
    public static Date generateNextValidTime(XxlJobInfo jobInfo, Date fromTime) throws Exception {
        ScheduleTypeEnum scheduleTypeEnum = ScheduleTypeEnum.match(jobInfo.getScheduleType(), null);
        if (ScheduleTypeEnum.CRON == scheduleTypeEnum) {
            // CRON表达式调度
            Date nextValidTime = new CronExpression(jobInfo.getScheduleConf()).getNextValidTimeAfter(fromTime);
            return nextValidTime;
        } else if (ScheduleTypeEnum.FIX_RATE == scheduleTypeEnum /*|| ScheduleTypeEnum.FIX_DELAY == scheduleTypeEnum*/) {
            // 固定速率调度
            return new Date(fromTime.getTime() + Integer.valueOf(jobInfo.getScheduleConf())*1000 );
        }else if (ScheduleTypeEnum.PERIOD == scheduleTypeEnum){
            // 周期性调度
            int dataInterval = jobInfo.getDataInterval();
            String timeUnit = jobInfo.getTimeUnit();
            long l = TimeConverterUtil.calculateTimestamp(dataInterval, timeUnit);
            Date date = new Date(jobInfo.getTriggerNextTime() + l);
            if (TimeConverterUtil.isNextValidTimeExceedDeadline(new Date(jobInfo.getTriggerNextTime() + l),jobInfo.getSchedulingDeadline())){
                return date;
            }
        }
        return null;
    }
}
