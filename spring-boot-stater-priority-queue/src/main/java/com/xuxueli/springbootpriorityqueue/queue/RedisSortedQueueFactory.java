package com.xuxueli.springbootpriorityqueue.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * RedisSortedQueue工厂类，用于创建和管理RedisSortedQueue实例
 */
public class RedisSortedQueueFactory {
    private static final Logger logger = LoggerFactory.getLogger(RedisSortedQueueFactory.class);
    
    private final RedisTemplate<String, Object> redisTemplate;
    private final ConcurrentMap<String, RedisSortedQueue<?>> queueCache = new ConcurrentHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    public RedisSortedQueueFactory(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * 获取指定类型的队列实例
     * @param queueName 队列名称
     * @param clazz 队列元素类型
     * @param <T> 元素泛型
     * @return 队列实例
     */
    @SuppressWarnings("unchecked")
    public <T> RedisSortedQueue<T> getQueue(String queueName, Class<T> clazz) {
        String cacheKey = queueName + ":" + clazz.getName();
        
        // 首先尝试从缓存获取
        RedisSortedQueue<?> queue = queueCache.get(cacheKey);
        if (queue == null) {
            // 使用双重检查锁定模式确保线程安全
            lock.lock();
            try {
                queue = queueCache.get(cacheKey);
                if (queue == null) {
                    logger.info("创建新的Redis有序队列实例: {}", cacheKey);
                    queue = new RedisSortedQueue<>(redisTemplate, queueName, clazz);
                    queueCache.put(cacheKey, queue);
                }
            } finally {
                lock.unlock();
            }
        }
        
        return (RedisSortedQueue<T>) queue;
    }
    
    /**
     * 清除队列缓存
     */
    public void clearCache() {
        lock.lock();
        try {
            queueCache.clear();
            logger.info("已清除所有队列缓存");
        } finally {
            lock.unlock();
        }
    }
} 