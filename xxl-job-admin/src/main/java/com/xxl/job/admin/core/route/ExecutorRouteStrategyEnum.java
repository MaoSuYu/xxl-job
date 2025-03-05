package com.xxl.job.admin.core.route;

import com.xxl.job.admin.core.route.strategy.*;
import com.xxl.job.admin.core.util.I18nUtil;

/**
 * 执行器路由策略枚举
 * 
 * Created by xuxueli on 17/3/10.
 */
public enum ExecutorRouteStrategyEnum {

    /**
     * 第一个
     * 固定选择第一个机器
     */
    FIRST(I18nUtil.getString("jobconf_route_first"), new ExecutorRouteFirst()),

    /**
     * 最后一个
     * 固定选择最后一个机器
     */
    LAST(I18nUtil.getString("jobconf_route_last"), new ExecutorRouteLast()),

    /**
     * 轮询
     * 按顺序轮询选择在线的机器
     */
    ROUND(I18nUtil.getString("jobconf_route_round"), new ExecutorRouteRound()),

    /**
     * 随机
     * 随机选择在线的机器
     */
    RANDOM(I18nUtil.getString("jobconf_route_random"), new ExecutorRouteRandom()),

    /**
     * 一致性HASH
     * 根据任务参数的Hash值来选择机器，相同参数的任务固定调度到同一台机器
     */
    CONSISTENT_HASH(I18nUtil.getString("jobconf_route_consistenthash"), new ExecutorRouteConsistentHash()),

    /**
     * 最不经常使用
     * 使用频率最低的机器优先被选举
     */
    LEAST_FREQUENTLY_USED(I18nUtil.getString("jobconf_route_lfu"), new ExecutorRouteLFU()),

    /**
     * 最近最久未使用
     * 最久未使用的机器优先被选举
     */
    LEAST_RECENTLY_USED(I18nUtil.getString("jobconf_route_lru"), new ExecutorRouteLRU()),

    /**
     * 故障转移
     * 按照顺序依次进行心跳检测，第一个心跳检测成功的机器选定为目标执行器并发起调度
     */
    FAILOVER(I18nUtil.getString("jobconf_route_failover"), new ExecutorRouteFailover()),

    /**
     * 忙碌转移
     * 按照顺序依次进行空闲检测，第一个空闲检测成功的机器选定为目标执行器并发起调度
     */
    BUSYOVER(I18nUtil.getString("jobconf_route_busyover"), new ExecutorRouteBusyover()),

    /**
     * 分片广播
     * 广播触发对应集群中所有机器执行一次任务，同时系统自动传递分片参数
     */
    SHARDING_BROADCAST(I18nUtil.getString("jobconf_route_shard"), null),

    /**
     * 根据是否有空闲线程来调度
     */
    IDLE_THREAD_BASED_TASK_ALLOCATOR(I18nUtil.getString("idle_thread_based_task_allocator"), new IdleThreadBasedTaskAllocator());

    ExecutorRouteStrategyEnum(String title, ExecutorRouter router) {
        this.title = title;
        this.router = router;
    }

    private String title;
    private ExecutorRouter router;

    public String getTitle() {
        return title;
    }
    public ExecutorRouter getRouter() {
        return router;
    }

    public static ExecutorRouteStrategyEnum match(String name, ExecutorRouteStrategyEnum defaultItem){
        if (name != null) {
            for (ExecutorRouteStrategyEnum item: ExecutorRouteStrategyEnum.values()) {
                if (item.name().equals(name)) {
                    return item;
                }
            }
        }
        return defaultItem;
    }

}
