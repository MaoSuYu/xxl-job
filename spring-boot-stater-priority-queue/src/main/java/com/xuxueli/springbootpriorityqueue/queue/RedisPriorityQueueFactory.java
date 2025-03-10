package com.xuxueli.springbootpriorityqueue.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Redis优先级队列工厂类
 * 用于创建或获取不同类型和名称的优先级队列实例
 */
@Component
public class RedisPriorityQueueFactory {

    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;
    private final ConcurrentMap<String, RedisPriorityQueue<?>> queueCache = new ConcurrentHashMap<>();
    private final ReentrantLock createQueueLock = new ReentrantLock();

    public RedisPriorityQueueFactory(RedisTemplate<String, Object> redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * 获取指定名称和元素类型的优先级队列实例
     * 如果队列不存在，则创建一个新的队列实例
     * 
     * @param queueName 队列名称
     * @param clazz 队列元素类型
     * @return 优先级队列实例
     */
    @SuppressWarnings("unchecked")
    public <T> RedisPriorityQueue<T> getQueue(String queueName, Class<T> clazz) {
        String cacheKey = queueName + ":" + clazz.getName();
        
        // 首先尝试从缓存中获取已存在的队列实例
        RedisPriorityQueue<?> queue = queueCache.get(cacheKey);
        if (queue != null) {
            return (RedisPriorityQueue<T>) queue;
        }
        
        // 如果缓存中不存在，则创建新实例（使用双重检查锁定模式）
        createQueueLock.lock();
        try {
            // 再次检查，防止其他线程已经创建了队列
            queue = queueCache.get(cacheKey);
            if (queue == null) {
                // 创建新的队列实例
                RedisPriorityQueue<T> newQueue = new RedisPriorityQueue<>(redisTemplate, queueName, clazz);
                queueCache.put(cacheKey, newQueue);
                return newQueue;
            }
        } finally {
            createQueueLock.unlock();
        }
        
        return (RedisPriorityQueue<T>) queue;
    }
} 