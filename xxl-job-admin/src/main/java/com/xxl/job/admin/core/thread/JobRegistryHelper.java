package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobGroup;
import com.xxl.job.admin.core.model.XxlJobRegistry;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.enums.RegistryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.*;

/**
 * 执行器注册管理助手类
 * 负责处理执行器的注册、注销请求，并维护执行器的在线状态
 * 包含注册线程池和注册监控线程两个核心组件
 *
 * @author xuxueli 2016-10-02 19:10:24
 */
public class JobRegistryHelper {
    // 日志记录器
    private static Logger logger = LoggerFactory.getLogger(JobRegistryHelper.class);

    // 单例模式实例
    private static JobRegistryHelper instance = new JobRegistryHelper();

    public static JobRegistryHelper getInstance() {
        return instance;
    }

    // 用于处理注册和注销请求的线程池
    private ThreadPoolExecutor registryOrRemoveThreadPool = null;
    // 注册监控线程，用于检测执行器状态
    private Thread registryMonitorThread;
    // 停止标志，用于优雅停止线程
    private volatile boolean toStop = false;

    public void start() {
        // 初始化注册/注销处理线程池
        // 核心线程数2，最大线程数10，线程存活时间30秒
        // 使用容量为2000的阻塞队列
        registryOrRemoveThreadPool = new ThreadPoolExecutor(
                2,
                10,
                30L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        // 自定义线程名称，方便问题排查
                        return new Thread(r, "xxl-job, admin JobRegistryMonitorHelper-registryOrRemoveThreadPool-" + r.hashCode());
                    }
                },
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        // 队列满时直接在当前线程执行任务
                        r.run();
                        logger.warn(">>>>>>>>>>> xxl-job, registry or remove too fast, match threadpool rejected handler(run now).");
                    }
                });

        // 初始化并启动注册监控线程
        registryMonitorThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!toStop) {
                    try {
                        // 查询所有自动注册类型的执行器组
                        List<XxlJobGroup> groupList = XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().findByAddressType(0);
                        if (groupList != null && !groupList.isEmpty()) {

                            // 清理已死亡的执行器地址
                            // 超过DEAD_TIMEOUT时间未更新的认为已死亡
                            List<Integer> ids = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().findDead(RegistryConfig.DEAD_TIMEOUT, new Date());
                            if (ids != null && ids.size() > 0) {
                                XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().removeDead(ids);
                            }

                            // 查询所有存活的执行器地址并刷新
                            HashMap<String, List<String>> appAddressMap = new HashMap<String, List<String>>();
                            List<XxlJobRegistry> list = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().findAll(RegistryConfig.DEAD_TIMEOUT, new Date());
                            if (list != null) {
                                // 按执行器AppName分组整理地址列表
                                for (XxlJobRegistry item : list) {
                                    if (RegistryConfig.RegistType.EXECUTOR.name().equals(item.getRegistryGroup())) {
                                        String appname = item.getRegistryKey();
                                        List<String> registryList = appAddressMap.get(appname);
                                        if (registryList == null) {
                                            registryList = new ArrayList<String>();
                                        }

                                        if (!registryList.contains(item.getRegistryValue())) {
                                            registryList.add(item.getRegistryValue());
                                        }
                                        appAddressMap.put(appname, registryList);
                                    }
                                }
                            }

                            // 更新执行器组的地址列表
                            for (XxlJobGroup group : groupList) {
                                // 获取该执行器组下的所有机器地址
                                List<String> registryList = appAddressMap.get(group.getAppname());
                                String addressListStr = null;
                                if (registryList != null && !registryList.isEmpty()) {
                                    // 将地址列表转换为逗号分隔的字符串
                                    Collections.sort(registryList);
                                    StringBuilder addressListSB = new StringBuilder();
                                    for (String item : registryList) {
                                        addressListSB.append(item).append(",");
                                    }
                                    addressListStr = addressListSB.toString();
                                    addressListStr = addressListStr.substring(0, addressListStr.length() - 1);
                                }
                                group.setAddressList(addressListStr);
                                group.setUpdateTime(new Date());

                                // 更新数据库中的执行器组信息
                                XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().update(group);
                            }
                        }
                    } catch (Throwable e) {
                        if (!toStop) {
                            logger.error(">>>>>>>>>>> xxl-job, job registry monitor thread error:{}", e);
                        }
                    }
                    try {
                        // 休眠等待下一次检查，间隔时间为心跳超时时间
                        TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
                    } catch (Throwable e) {
                        if (!toStop) {
                            logger.error(">>>>>>>>>>> xxl-job, job registry monitor thread error:{}", e);
                        }
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, job registry monitor thread stop");
            }
        });
        registryMonitorThread.setDaemon(true);
        registryMonitorThread.setName("xxl-job, admin JobRegistryMonitorHelper-registryMonitorThread");
        registryMonitorThread.start();
    }

    /**
     * 停止注册管理器
     * 包括停止注册线程池和监控线程
     */
    public void toStop() {
        // 设置停止标志
        toStop = true;

        // 关闭注册线程池
        registryOrRemoveThreadPool.shutdownNow();

        // 停止监控线程
        registryMonitorThread.interrupt();
        try {
            registryMonitorThread.join();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
    }

    // ---------------------- 注册相关方法 ----------------------

    /**
     * 处理执行器注册请求
     *
     * @param registryParam 注册参数，包含注册组、注册键、注册值
     * @return 注册结果
     */
    public ReturnT<String> registry(RegistryParam registryParam) {
        // 参数校验
        if (!StringUtils.hasText(registryParam.getRegistryGroup())
                || !StringUtils.hasText(registryParam.getRegistryKey())
                || !StringUtils.hasText(registryParam.getRegistryValue())) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "Illegal Argument.");
        }

        // 异步执行注册操作
        registryOrRemoveThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                // 保存或更新注册信息
                // 返回值说明：0-失败；1-新增成功；2-更新成功
                int ret = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().registrySaveOrUpdate(
                        registryParam.getRegistryGroup(),
                        registryParam.getRegistryKey(),
                        registryParam.getRegistryValue(),
                        new Date(),
                        registryParam.getThreadRunningCount(),
                        registryParam.getMaxThreadCount()
                );

                // 如果是新注册的执行器，刷新执行器组信息
                if (ret == 1) {
                    freshGroupRegistryInfo(registryParam);
                }
            }
        });

        return ReturnT.SUCCESS;
    }

    /**
     * 处理执行器注销请求
     *
     * @param registryParam 注销参数
     * @return 注销结果
     */
    public ReturnT<String> registryRemove(RegistryParam registryParam) {
        // 参数校验
        if (!StringUtils.hasText(registryParam.getRegistryGroup())
                || !StringUtils.hasText(registryParam.getRegistryKey())
                || !StringUtils.hasText(registryParam.getRegistryValue())) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "Illegal Argument.");
        }

        // 异步执行注销操作
        registryOrRemoveThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                // 从数据库中删除注册信息
                int ret = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().registryDelete(
                        registryParam.getRegistryGroup(),
                        registryParam.getRegistryKey(),
                        registryParam.getRegistryValue()
                );

                // 如果成功删除，刷新执行器组信息
                if (ret > 0) {
                    freshGroupRegistryInfo(registryParam);
                }
            }
        });

        return ReturnT.SUCCESS;
    }

    /**
     * 刷新执行器组注册信息
     * 该方法预留，目前未实现具体逻辑，避免影响核心表
     */
    private void freshGroupRegistryInfo(RegistryParam registryParam) {
        // Under consideration, prevent affecting core tables
    }
}
