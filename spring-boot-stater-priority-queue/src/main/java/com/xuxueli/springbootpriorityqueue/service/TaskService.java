package com.xuxueli.springbootpriorityqueue.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xuxueli.springbootpriorityqueue.model.Task;
import com.xuxueli.springbootpriorityqueue.queue.RedisPriorityQueue;
import com.xuxueli.springbootpriorityqueue.queue.RedisPriorityQueueFactory;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Set;

/**
 * 任务服务类，演示如何使用Redis优先级队列
 */
@Service
public class TaskService {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private ObjectMapper objectMapper;
    private RedisPriorityQueue<Task> taskQueue;


    @PostConstruct
    public void init() {
        RedisPriorityQueueFactory redisPriorityQueueFactory = new RedisPriorityQueueFactory(redisTemplate, objectMapper);
        // 获取或创建一个Task类型的优先级队列
        this.taskQueue = redisPriorityQueueFactory.getQueue("tasks", Task.class);
    }

    /**
     * 添加任务到队列
     *
     * @param task     任务对象
     * @param priority 优先级(1-10)
     */
    public void addTask(Task task, int priority) {
        taskQueue.enqueue(task, priority);
    }

    /**
     * 获取下一个要处理的任务(优先级最高的)
     *
     * @return 下一个任务，如果队列为空则返回null
     */
    public Task getNextTask() {
        return taskQueue.dequeue();
    }

    /**
     * 查看下一个要处理的任务但不移除
     *
     * @return 下一个任务，如果队列为空则返回null
     */
    public Task peekNextTask() {
        return taskQueue.peek();
    }

    /**
     * 获取高优先级任务(优先级1-3)
     *
     * @return 高优先级任务集合
     */
    public Set<Task> getHighPriorityTasks() {
        return taskQueue.getItemsByPriorityRange(1, 3);
    }

    /**
     * 获取中等优先级任务(优先级4-7)
     *
     * @return 中等优先级任务集合
     */
    public Set<Task> getMediumPriorityTasks() {
        return taskQueue.getItemsByPriorityRange(4, 7);
    }

    /**
     * 获取低优先级任务(优先级8-10)
     *
     * @return 低优先级任务集合
     */
    public Set<Task> getLowPriorityTasks() {
        return taskQueue.getItemsByPriorityRange(8, 10);
    }

    /**
     * 获取队列中的任务数量
     *
     * @return 任务数量
     */
    public long getTaskCount() {
        return taskQueue.size();
    }

    /**
     * 清空任务队列
     */
    public void clearTasks() {
        taskQueue.clear();
    }
} 