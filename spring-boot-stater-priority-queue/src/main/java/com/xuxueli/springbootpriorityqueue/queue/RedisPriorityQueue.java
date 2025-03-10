package com.xuxueli.springbootpriorityqueue.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于Redis的分布式优先级队列实现
 * 支持1-10的优先级，数字越小优先级越高
 */
public class RedisPriorityQueue<T> {
    private static final Logger logger = LoggerFactory.getLogger(RedisPriorityQueue.class);
    private static final int MAX_RETRY = 3;
    private static final long RETRY_DELAY_MS = 50;
    private static final AtomicLong SEQUENCE = new AtomicLong(0);
    
    private final RedisTemplate<String, Object> redisTemplate;
    private final String queueKey;
    private final ObjectMapper objectMapper;
    private final Class<T> clazz;
    
    // Lua脚本，用于原子地执行dequeue操作
    private static final String DEQUEUE_SCRIPT = 
            "local items = redis.call('ZRANGE', KEYS[1], 0, 0) " +
            "if #items > 0 then " +
            "    redis.call('ZREMRANGEBYRANK', KEYS[1], 0, 0) " +
            "    return items[1] " +
            "else " +
            "    return nil " +
            "end";
    
    private final DefaultRedisScript<String> dequeueScript;

    /**
     * 自定义队列异常类
     */
    public static class QueueException extends RuntimeException {
        public QueueException(String message) {
            super(message);
        }
        
        public QueueException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * 构造函数
     * @param redisTemplate Redis操作模板
     * @param queueName 队列名称
     * @param clazz 队列元素类型
     */
    public RedisPriorityQueue(RedisTemplate<String, Object> redisTemplate, String queueName, Class<T> clazz) {
        this.redisTemplate = redisTemplate;
        this.queueKey = "priority_queue:" + queueName;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        this.clazz = clazz;
        
        this.dequeueScript = new DefaultRedisScript<>();
        this.dequeueScript.setScriptText(DEQUEUE_SCRIPT);
        this.dequeueScript.setResultType(String.class);
    }

    /**
     * 将元素添加到队列中
     * @param item 要添加的元素
     * @param priority 优先级(1-10)，数字越小优先级越高
     * @return 添加是否成功
     */
    public boolean enqueue(T item, int priority) {
        if (priority < 1 || priority > 10) {
            throw new IllegalArgumentException("优先级必须在1-10之间");
        }
        
        // 重试机制
        for (int attempt = 0; attempt < MAX_RETRY; attempt++) {
            try {
                // 序列化对象
                String itemJson = objectMapper.writeValueAsString(item);
                
                // 使用原子序列号作为微小增量，确保分数唯一
                // 即使在高并发下也能保证顺序
                long sequence = SEQUENCE.getAndIncrement() % 1000000;
                long timestamp = System.currentTimeMillis() % 1000000;
                double score = priority + (timestamp / 1000000.0) + (sequence / 1000000000.0);
                
                logger.debug("添加任务，优先级: {}, 计算分数: {}", priority, score);
                
                // 使用Redis事务确保操作完整性
                Boolean result = redisTemplate.execute(new SessionCallback<Boolean>() {
                    @SuppressWarnings("unchecked")
                    @Override
                    public Boolean execute(RedisOperations operations) throws DataAccessException {
                        try {
                            operations.multi();
                            operations.opsForZSet().add(queueKey, itemJson, score);
                            return operations.exec() != null;
                        } catch (Exception e) {
                            logger.error("Redis事务执行失败", e);
                            return false;
                        }
                    }
                });
                
                if (result != null && result) {
                    logger.debug("添加任务成功，优先级: {}, 计算分数: {}", priority, score);
                    return true;
                } else {
                    logger.warn("添加任务失败，优先级: {}, 等待{}ms后重试, 当前尝试次数: {}/{}", 
                            priority, RETRY_DELAY_MS, attempt + 1, MAX_RETRY);
                    Thread.sleep(RETRY_DELAY_MS);
                }
            } catch (JsonProcessingException e) {
                logger.error("序列化对象失败", e);
                throw new QueueException("序列化对象失败", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("添加任务被中断", e);
                throw new QueueException("添加任务被中断", e);
            } catch (Exception e) {
                logger.error("添加任务时发生未预期的错误", e);
                if (attempt == MAX_RETRY - 1) {
                    throw new QueueException("添加任务失败，已达到最大重试次数", e);
                }
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new QueueException("添加任务重试被中断", ie);
                }
            }
        }
        
        return false;
    }

    /**
     * 从队列中获取并移除优先级最高的元素
     * @return 优先级最高的元素，如果队列为空则返回null
     */
    @SuppressWarnings("unchecked")
    public T dequeue() {
        for (int attempt = 0; attempt < MAX_RETRY; attempt++) {
            try {
                // 使用Lua脚本原子地执行dequeue操作
                String itemJson = redisTemplate.execute(
                        dequeueScript, 
                        Collections.singletonList(queueKey));
                
                if (itemJson == null) {
                    return null;
                }
                
                return objectMapper.readValue(itemJson, clazz);
            } catch (RedisSystemException e) {
                logger.warn("执行Lua脚本时出错，等待{}ms后重试，当前尝试次数: {}/{}", 
                        RETRY_DELAY_MS, attempt + 1, MAX_RETRY, e);
                if (attempt == MAX_RETRY - 1) {
                    throw new QueueException("获取任务失败，已达到最大重试次数", e);
                }
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new QueueException("获取任务重试被中断", ie);
                }
            } catch (Exception e) {
                logger.error("反序列化对象失败", e);
                throw new QueueException("反序列化对象失败", e);
            }
        }
        
        return null;
    }

    /**
     * 查看优先级最高的元素但不移除
     * @return 优先级最高的元素，如果队列为空则返回null
     */
    @SuppressWarnings("unchecked")
    public T peek() {
        for (int attempt = 0; attempt < MAX_RETRY; attempt++) {
            try {
                // 获取分数最低的元素(优先级最高)
                Set<Object> items = redisTemplate.opsForZSet().range(queueKey, 0, 0);
                if (items == null || items.isEmpty()) {
                    return null;
                }
                
                String itemJson = (String) items.iterator().next();
                return objectMapper.readValue(itemJson, clazz);
            } catch (Exception e) {
                logger.warn("查看任务时出错，等待{}ms后重试，当前尝试次数: {}/{}", 
                        RETRY_DELAY_MS, attempt + 1, MAX_RETRY, e);
                if (attempt == MAX_RETRY - 1) {
                    throw new QueueException("查看任务失败，已达到最大重试次数", e);
                }
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new QueueException("查看任务重试被中断", ie);
                }
            }
        }
        
        return null;
    }

    /**
     * 获取队列长度
     * @return 队列中的元素数量
     */
    public long size() {
        for (int attempt = 0; attempt < MAX_RETRY; attempt++) {
            try {
                Long size = redisTemplate.opsForZSet().size(queueKey);
                return size != null ? size : 0;
            } catch (Exception e) {
                logger.warn("获取队列长度时出错，等待{}ms后重试，当前尝试次数: {}/{}", 
                        RETRY_DELAY_MS, attempt + 1, MAX_RETRY, e);
                if (attempt == MAX_RETRY - 1) {
                    throw new QueueException("获取队列长度失败，已达到最大重试次数", e);
                }
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new QueueException("获取队列长度重试被中断", ie);
                }
            }
        }
        
        return 0;
    }

    /**
     * 判断队列是否为空
     * @return 如果队列为空则返回true，否则返回false
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * 清空队列
     * @return 操作是否成功
     */
    public boolean clear() {
        for (int attempt = 0; attempt < MAX_RETRY; attempt++) {
            try {
                Boolean result = redisTemplate.delete(queueKey);
                return result != null && result;
            } catch (Exception e) {
                logger.warn("清空队列时出错，等待{}ms后重试，当前尝试次数: {}/{}", 
                        RETRY_DELAY_MS, attempt + 1, MAX_RETRY, e);
                if (attempt == MAX_RETRY - 1) {
                    throw new QueueException("清空队列失败，已达到最大重试次数", e);
                }
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new QueueException("清空队列重试被中断", ie);
                }
            }
        }
        
        return false;
    }
    
    /**
     * 获取指定优先级范围的元素
     * @param minPriority 最小优先级(包含)
     * @param maxPriority 最大优先级(包含)
     * @return 指定优先级范围内的元素集合
     */
    @SuppressWarnings("unchecked")
    public Set<T> getItemsByPriorityRange(int minPriority, int maxPriority) {
        // 参数验证
        if (minPriority > maxPriority) {
            throw new IllegalArgumentException("最小优先级不能大于最大优先级");
        }
        if (minPriority < 1 || maxPriority > 10) {
            throw new IllegalArgumentException("优先级必须在1-10范围内");
        }
        
        // 计算分数范围：优先级数值直接作为分数的整数部分
        double minScore = minPriority;
        double maxScore = maxPriority + 0.999999; // 加上0.999999确保包含所有该优先级的元素
        
        logger.debug("查询优先级范围: {}-{}", minPriority, maxPriority);
        logger.debug("转换为分数范围: {}-{}", minScore, maxScore);
        
        for (int attempt = 0; attempt < MAX_RETRY; attempt++) {
            try {
                // 获取指定分数范围内的元素，限制最大获取数量为1000以防内存溢出
                Set<Object> itemJsonSet = redisTemplate.opsForZSet().rangeByScore(queueKey, minScore, maxScore, 0, 1000);
                Set<T> result = new HashSet<>();
                
                if (itemJsonSet != null && !itemJsonSet.isEmpty()) {
                    logger.debug("找到的元素数量: {}", itemJsonSet.size());
                    for (Object itemJsonObj : itemJsonSet) {
                        String itemJson = (String) itemJsonObj;
                        Double score = redisTemplate.opsForZSet().score(queueKey, itemJson);
                        int itemPriority = score != null ? (int) Math.floor(score) : 0;
                        logger.debug("元素分数: {}, 对应优先级: {}", score, itemPriority);
                        
                        try {
                            T item = objectMapper.readValue(itemJson, clazz);
                            result.add(item);
                        } catch (Exception e) {
                            logger.error("反序列化对象失败: {}", itemJson, e);
                            // 继续处理其他项，不因一个项的失败而终止整个操作
                        }
                    }
                } else {
                    logger.debug("未找到符合条件的元素");
                }
                
                return result;
            } catch (Exception e) {
                logger.warn("按优先级范围获取任务时出错，等待{}ms后重试，当前尝试次数: {}/{}", 
                        RETRY_DELAY_MS, attempt + 1, MAX_RETRY, e);
                if (attempt == MAX_RETRY - 1) {
                    throw new QueueException("按优先级范围获取任务失败，已达到最大重试次数", e);
                }
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new QueueException("按优先级范围获取任务重试被中断", ie);
                }
            }
        }
        
        return new HashSet<>();
    }
} 