package com.xuxueli.springbootpriorityqueue.service;

import com.xuxueli.springbootpriorityqueue.model.SortedTask;
import com.xuxueli.springbootpriorityqueue.queue.RedisSortedQueue;
import com.xuxueli.springbootpriorityqueue.queue.RedisSortedQueueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Set;

/**
 * 排序任务服务类
 * 
 * 该服务类封装了RedisSortedQueue的操作，提供了一系列方法来管理排序任务。
 * 排序任务按照优先级（分数）进行排序，分数越小的任务优先级越高。
 * 
 * 主要功能包括：
 * 1. 添加任务到队列
 * 2. 获取并移除优先级最高的任务
 * 3. 查看下一个任务但不移除
 * 4. 按分数范围获取任务
 * 5. 更新任务优先级
 * 6. 从队列中移除指定任务
 */
@Service
public class SortedTaskService {
    private static final Logger logger = LoggerFactory.getLogger(SortedTaskService.class);
    
    private final RedisSortedQueue<SortedTask> taskQueue;
    
    /**
     * 构造函数，通过工厂获取或创建一个SortedTask类型的有序队列
     * 
     * @param queueFactory Redis有序队列工厂
     */
    public SortedTaskService(RedisSortedQueueFactory queueFactory) {
        this.taskQueue = queueFactory.getQueue("sorted_tasks", SortedTask.class);
        logger.info("已初始化排序任务服务");
    }
    
    /**
     * 添加任务到队列
     * 
     * @param task 任务对象
     * @return 添加是否成功
     */
    public boolean addTask(SortedTask task) {
        logger.debug("添加任务到队列: {}, 优先级: {}", task.getId(), task.getPriority());
        return taskQueue.enqueue(task, task.getPriority());
    }
    
    /**
     * 获取并移除优先级最高的任务
     * 
     * @return 优先级最高的任务，如果队列为空则返回null
     */
    public SortedTask getNextTask() {
        SortedTask task = taskQueue.dequeue();
        if (task != null) {
            logger.debug("获取到优先级最高的任务: {}", task.getId());
        } else {
            logger.debug("队列为空，没有获取到任务");
        }
        return task;
    }
    
    /**
     * 查看下一个要处理的任务但不移除
     * 
     * @return 优先级最高的任务，如果队列为空则返回null
     */
    public SortedTask peekNextTask() {
        SortedTask task = taskQueue.peek();
        if (task != null) {
            logger.debug("查看到优先级最高的任务: {}", task.getId());
        } else {
            logger.debug("队列为空，没有查看到任务");
        }
        return task;
    }
    
    /**
     * 按分数范围获取任务
     * 
     * @param minScore 最小分数（包含）
     * @param maxScore 最大分数（包含）
     * @return 指定分数范围内的任务集合
     */
    public Set<SortedTask> getTasksByScoreRange(double minScore, double maxScore) {
        logger.debug("获取分数范围[{} - {}]内的任务", minScore, maxScore);
        return taskQueue.getItemsByScoreRange(minScore, maxScore);
    }
    
    /**
     * 获取高优先级任务（分数0-3）
     * 
     * @return 高优先级任务集合
     */
    public Set<SortedTask> getHighPriorityTasks() {
        logger.debug("获取高优先级任务(0-3)");
        return getTasksByScoreRange(0, 3);
    }
    
    /**
     * 获取中等优先级任务（分数3.1-7）
     * 
     * @return 中等优先级任务集合
     */
    public Set<SortedTask> getMediumPriorityTasks() {
        logger.debug("获取中等优先级任务(3.1-7)");
        return getTasksByScoreRange(3.1, 7);
    }
    
    /**
     * 获取低优先级任务（分数7.1-10）
     * 
     * @return 低优先级任务集合
     */
    public Set<SortedTask> getLowPriorityTasks() {
        logger.debug("获取低优先级任务(7.1-10)");
        return getTasksByScoreRange(7.1, 10);
    }
    
    /**
     * 更新任务优先级
     * 
     * @param task 要更新的任务
     * @param newPriority 新的优先级
     * @return 更新是否成功
     */
    public boolean updateTaskPriority(SortedTask task, double newPriority) {
        logger.debug("更新任务{}的优先级: {} -> {}", task.getId(), task.getPriority(), newPriority);
        // 更新任务对象的优先级
        task.setPriority(newPriority);
        // 更新队列中的优先级
        return taskQueue.updateScore(task, newPriority);
    }
    
    /**
     * 从队列中移除指定任务
     * 
     * @param task 要移除的任务
     * @return 移除是否成功
     */
    public boolean removeTask(SortedTask task) {
        logger.debug("从队列中移除任务: {}", task.getId());
        return taskQueue.remove(task);
    }
    
    /**
     * 获取队列中的任务数量
     * 
     * @return 任务数量
     */
    public long getTaskCount() {
        long count = taskQueue.size();
        logger.debug("当前队列任务数量: {}", count);
        return count;
    }
    
    /**
     * 清空任务队列
     */
    public void clearTasks() {
        logger.debug("清空任务队列");
        taskQueue.clear();
    }
    
    /**
     * 判断队列是否为空
     * 
     * @return 如果队列为空则返回true，否则返回false
     */
    public boolean isQueueEmpty() {
        boolean isEmpty = taskQueue.isEmpty();
        logger.debug("队列是否为空: {}", isEmpty);
        return isEmpty;
    }
} 