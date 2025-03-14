package com.xxl.job.admin.core.thread;

import cn.hutool.extra.spring.SpringUtil;
import com.xuxueli.springbootpriorityqueue.service.SortedTaskService;
import com.xuxueli.springbootpriorityqueue.service.TaskService;
import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import com.xxl.job.admin.core.trigger.XxlJobTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 任务触发线程池助手
 * <p>
 * 该类负责管理两个线程池：
 * 1. 快速触发线程池(fastTriggerPool)：用于处理普通任务
 * 2. 慢速触发线程池(slowTriggerPool)：用于处理执行时间较长的任务
 * <p>
 * 当任务在1分钟内出现超过10次超时(超过500ms)时，会被自动切换到慢速线程池处理
 *
 * @author xuxueli 2018-07-03 21:08:07
 */
public class JobTriggerPoolHelper {
    private static Logger logger = LoggerFactory.getLogger(JobTriggerPoolHelper.class);

    // ---------------------- trigger pool ----------------------

    /**
     * 快速触发线程池
     * 核心线程数：10
     * 最大线程数：由配置文件指定
     * 队列容量：2000
     */
    private ThreadPoolExecutor fastTriggerPool = null;

    /**
     * 慢速触发线程池
     * 核心线程数：10
     * 最大线程数：由配置文件指定
     * 队列容量：5000
     */
    private ThreadPoolExecutor slowTriggerPool = null;

    /**
     * 启动触发线程池
     * 初始化快速和慢速两个线程池
     */
    public void start() {
        // 初始化快速触发线程池
        fastTriggerPool = new ThreadPoolExecutor(
                10,
                XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "xxl-job, admin JobTriggerPoolHelper-fastTriggerPool-" + r.hashCode());
                    }
                },
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        logger.error(">>>>>>>>>>> xxl-job, admin JobTriggerPoolHelper-fastTriggerPool execute too fast, Runnable=" + r.toString());
                    }
                });

        // 初始化慢速触发线程池
        slowTriggerPool = new ThreadPoolExecutor(
                10,
                XxlJobAdminConfig.getAdminConfig().getTriggerPoolSlowMax(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(5000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "xxl-job, admin JobTriggerPoolHelper-slowTriggerPool-" + r.hashCode());
                    }
                },
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        logger.error(">>>>>>>>>>> xxl-job, admin JobTriggerPoolHelper-slowTriggerPool execute too fast, Runnable=" + r.toString());
                    }
                });
    }

    /**
     * 停止触发线程池
     * 立即关闭快速和慢速两个线程池
     */
    public void stop() {
        fastTriggerPool.shutdownNow();
        slowTriggerPool.shutdownNow();
        logger.info(">>>>>>>>> xxl-job trigger thread pool shutdown success.");
    }

    // 任务超时计数相关变量
    /**
     * 当前分钟数，用于每分钟重置超时计数
     */
    private volatile long minTim = System.currentTimeMillis() / 60000;     // ms > min

    /**
     * 任务超时计数Map
     * key: jobId
     * value: 该任务在当前分钟内的超时次数
     */
    private volatile ConcurrentMap<Long, AtomicInteger> jobTimeoutCountMap = new ConcurrentHashMap<>();

    /**
     * add trigger
     */
    public void addTrigger(final Long jobId,
                           final TriggerTypeEnum triggerType,
                           final int failRetryCount,
                           final String executorShardingParam,
                           final String executorParam,
                           final String addressList) {

        // choose thread pool
        ThreadPoolExecutor triggerPool_ = fastTriggerPool;
        AtomicInteger jobTimeoutCount = jobTimeoutCountMap.get(jobId);
        if (jobTimeoutCount != null && jobTimeoutCount.get() > 10) {      // job-timeout 10 times in 1 min
            triggerPool_ = slowTriggerPool;
        }

        // trigger
        triggerPool_.execute(new Runnable() {
            @Override
            public void run() {

                long start = System.currentTimeMillis();

                try {
                    // do trigger
                    XxlJobTrigger.trigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
                } catch (Throwable e) {
                    logger.error(e.getMessage(), e);
                } finally {

                    // check timeout-count-map
                    long minTim_now = System.currentTimeMillis() / 60000;
                    if (minTim != minTim_now) {
                        minTim = minTim_now;
                        jobTimeoutCountMap.clear();
                    }

                    // incr timeout-count-map
                    long cost = System.currentTimeMillis() - start;
                    if (cost > 500) {       // ob-timeout threshold 500ms
                        AtomicInteger timeoutCount = jobTimeoutCountMap.putIfAbsent(jobId, new AtomicInteger(1));
                        if (timeoutCount != null) {
                            timeoutCount.incrementAndGet();
                        }
                    }

                }

            }

            @Override
            public String toString() {
                return "Job Runnable, jobId:" + jobId;
            }
        });
    }


    /**
     * 添加任务触发请求到线程池
     * 根据任务的超时情况选择合适的线程池执行
     *
     * @param jobId                 任务
     * @param triggerType           触发类型
     * @param failRetryCount        失败重试次数
     * @param executorShardingParam 分片参数
     * @param executorParam         执行参数
     * @param addressList           执行器地址列表
     */
    public void addTriggerSharding(final Long jobId,
                                   final TriggerTypeEnum triggerType,
                                   final int failRetryCount,
                                   final String executorShardingParam,
                                   final String executorParam,
                                   final String addressList,
                                   int isAutomatic) {

        // 根据任务超时情况选择线程池
        ThreadPoolExecutor triggerPool_ = fastTriggerPool;
        AtomicInteger jobTimeoutCount = jobTimeoutCountMap.get(jobId);
        if (jobTimeoutCount != null && jobTimeoutCount.get() > 10) {      // 任务超时次数>10，使用慢速线程池
            triggerPool_ = slowTriggerPool;
        }

        // 提交任务到选定的线程池
        triggerPool_.execute(new Runnable() {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                try {
                    // 触发任务执行
                    XxlJobTrigger.triggerSharding(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList, isAutomatic);
                } catch (Throwable e) {
                    logger.error(e.getMessage(), e);
                } finally {
                    // 检查是否需要重置超时计数（每分钟重置一次）
                    long minTim_now = System.currentTimeMillis() / 60000;
                    if (minTim != minTim_now) {
                        minTim = minTim_now;
                        jobTimeoutCountMap.clear();
                    }

                    // 检查任务执行时间是否超过500ms，超过则增加超时计数
                    long cost = System.currentTimeMillis() - start;
                    if (cost > 500) {
                        AtomicInteger timeoutCount = jobTimeoutCountMap.putIfAbsent(jobId, new AtomicInteger(1));
                        if (timeoutCount != null) {
                            timeoutCount.incrementAndGet();
                        }
                    }
                }
            }

            @Override
            public String toString() {
                return "Job Runnable, jobId:" + jobId;
            }
        });
    }

    // ---------------------- helper ----------------------

    /**
     * 单例实例
     */
    private static JobTriggerPoolHelper helper = new JobTriggerPoolHelper();

    /**
     * 启动触发线程池
     */
    public static void toStart() {
        helper.start();
    }

    /**
     * 停止触发线程池
     */
    public static void toStop() {
        helper.stop();
    }

    /**
     * 添加任务触发请求的静态方法
     * 通过单例实例调用addTrigger方法
     */
    public static void triggerSharding(Long jobId, TriggerTypeEnum triggerType, int failRetryCount, String executorShardingParam, String executorParam, String addressList, int isAutomatic) {

        helper.addTriggerSharding(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList, isAutomatic);
    }

    /**
     * @param jobId
     * @param triggerType
     * @param failRetryCount        >=0: use this param
     *                              &lt;0: use param from job info config
     * @param executorShardingParam
     * @param executorParam         null: use job param
     *                              not null: cover job param
     */
    public static void trigger(Long jobId, TriggerTypeEnum triggerType, int failRetryCount, String executorShardingParam, String executorParam, String addressList) {
        helper.addTrigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
    }

}
