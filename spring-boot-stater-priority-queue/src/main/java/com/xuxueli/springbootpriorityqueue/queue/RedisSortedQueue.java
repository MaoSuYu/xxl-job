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
 * 基于Redis的分布式有序队列实现
 * 可以直接指定元素的score，分数越小越靠前
 */
public class RedisSortedQueue<T> {
    private static final Logger logger = LoggerFactory.getLogger(RedisSortedQueue.class);
    private static final int MAX_RETRY = 3;
    private static final long RETRY_DELAY_MS = 50;
    private static final AtomicLong SEQUENCE = new AtomicLong(0);
    
    private final RedisTemplate<String, Object> redisTemplate;
    private final String queueKey;
    private final ObjectMapper objectMapper;
    private final Class<T> clazz;
    
    // Lua脚本，用于原子地执行出队操作
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
    public RedisSortedQueue(RedisTemplate<String, Object> redisTemplate, String queueName, Class<T> clazz) {
        this.redisTemplate = redisTemplate;
        this.queueKey = "sorted_queue:" + queueName;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        this.clazz = clazz;
        
        this.dequeueScript = new DefaultRedisScript<>();
        this.dequeueScript.setScriptText(DEQUEUE_SCRIPT);
        this.dequeueScript.setResultType(String.class);
        
        logger.info("创建有序队列: {}", this.queueKey);
    }

    /**
     * 将元素添加到队列中
     * @param item 要添加的元素
     * @param score 分数值，分数越小排名越靠前
     * @return 添加是否成功
     */
    public boolean enqueue(T item, double score) {
        // 重试机制
        for (int attempt = 0; attempt < MAX_RETRY; attempt++) {
            try {
                // 序列化对象
                String itemJson = objectMapper.writeValueAsString(item);
                
                // 使用原子序列号作为微小增量，确保高并发下分数唯一
                long sequence = SEQUENCE.getAndIncrement() % 1000000;
                double uniqueScore = score + (sequence / 1000000000.0);
                
                logger.debug("添加元素到队列，分数: {}, 唯一分数: {}", score, uniqueScore);
                
                // 使用Redis事务确保操作完整性
                Boolean result = redisTemplate.execute(new SessionCallback<Boolean>() {
                    @SuppressWarnings("unchecked")
                    @Override
                    public Boolean execute(RedisOperations operations) throws DataAccessException {
                        try {
                            operations.multi();
                            operations.opsForZSet().add(queueKey, itemJson, uniqueScore);
                            return operations.exec() != null;
                        } catch (Exception e) {
                            logger.error("Redis事务执行失败", e);
                            return false;
                        }
                    }
                });
                
                if (result != null && result) {
                    logger.debug("添加元素成功，分数: {}", uniqueScore);
                    return true;
                } else {
                    logger.warn("添加元素失败，等待{}ms后重试, 当前尝试次数: {}/{}", 
                            RETRY_DELAY_MS, attempt + 1, MAX_RETRY);
                    Thread.sleep(RETRY_DELAY_MS);
                }
            } catch (JsonProcessingException e) {
                logger.error("序列化对象失败", e);
                throw new QueueException("序列化对象失败", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("添加元素被中断", e);
                throw new QueueException("添加元素被中断", e);
            } catch (Exception e) {
                logger.error("添加元素时发生未预期的错误", e);
                if (attempt == MAX_RETRY - 1) {
                    throw new QueueException("添加元素失败，已达到最大重试次数", e);
                }
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new QueueException("添加元素重试被中断", ie);
                }
            }
        }
        
        return false;
    }

    /**
     * 从队列中获取并移除分数最小的元素
     * @return 分数最小的元素，如果队列为空则返回null
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
                    throw new QueueException("获取元素失败，已达到最大重试次数", e);
                }
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new QueueException("获取元素重试被中断", ie);
                }
            } catch (Exception e) {
                logger.error("反序列化对象失败", e);
                throw new QueueException("反序列化对象失败", e);
            }
        }
        
        return null;
    }

    /**
     * 查看分数最小的元素但不移除
     * @return 分数最小的元素，如果队列为空则返回null
     */
    @SuppressWarnings("unchecked")
    public T peek() {
        for (int attempt = 0; attempt < MAX_RETRY; attempt++) {
            try {
                // 获取分数最低的元素
                Set<Object> items = redisTemplate.opsForZSet().range(queueKey, 0, 0);
                if (items == null || items.isEmpty()) {
                    return null;
                }
                
                String itemJson = (String) items.iterator().next();
                return objectMapper.readValue(itemJson, clazz);
            } catch (Exception e) {
                logger.warn("查看元素时出错，等待{}ms后重试，当前尝试次数: {}/{}", 
                        RETRY_DELAY_MS, attempt + 1, MAX_RETRY, e);
                if (attempt == MAX_RETRY - 1) {
                    throw new QueueException("查看元素失败，已达到最大重试次数", e);
                }
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new QueueException("查看元素重试被中断", ie);
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
     * 获取指定分数范围的元素
     * @param minScore 最小分数(包含)
     * @param maxScore 最大分数(包含)
     * @return 指定分数范围内的元素集合
     */
    @SuppressWarnings("unchecked")
    public Set<T> getItemsByScoreRange(double minScore, double maxScore) {
        logger.debug("查询分数范围: {}-{}", minScore, maxScore);
        
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
                        logger.debug("元素分数: {}", score);
                        
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
                logger.warn("按分数范围获取元素时出错，等待{}ms后重试，当前尝试次数: {}/{}", 
                        RETRY_DELAY_MS, attempt + 1, MAX_RETRY, e);
                if (attempt == MAX_RETRY - 1) {
                    throw new QueueException("按分数范围获取元素失败，已达到最大重试次数", e);
                }
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new QueueException("按分数范围获取元素重试被中断", ie);
                }
            }
        }
        
        return new HashSet<>();
    }
    
    /**
     * 获取指定排名范围的元素
     * @param start 起始排名(包含)，0表示第一个元素
     * @param end 结束排名(包含)，-1表示最后一个元素
     * @return 指定排名范围内的元素集合
     */
    @SuppressWarnings("unchecked")
    public Set<T> getItemsByRankRange(long start, long end) {
        logger.debug("查询排名范围: {}-{}", start, end);
        
        for (int attempt = 0; attempt < MAX_RETRY; attempt++) {
            try {
                // 获取指定排名范围内的元素
                Set<Object> itemJsonSet = redisTemplate.opsForZSet().range(queueKey, start, end);
                Set<T> result = new HashSet<>();
                
                if (itemJsonSet != null && !itemJsonSet.isEmpty()) {
                    logger.debug("找到的元素数量: {}", itemJsonSet.size());
                    for (Object itemJsonObj : itemJsonSet) {
                        String itemJson = (String) itemJsonObj;
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
                logger.warn("按排名范围获取元素时出错，等待{}ms后重试，当前尝试次数: {}/{}", 
                        RETRY_DELAY_MS, attempt + 1, MAX_RETRY, e);
                if (attempt == MAX_RETRY - 1) {
                    throw new QueueException("按排名范围获取元素失败，已达到最大重试次数", e);
                }
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new QueueException("按排名范围获取元素重试被中断", ie);
                }
            }
        }
        
        return new HashSet<>();
    }
    
    /**
     * 获取元素的分数
     * @param item 要查询的元素
     * @return 元素的分数，如果元素不存在则返回null
     */
    public Double getScore(T item) {
        for (int attempt = 0; attempt < MAX_RETRY; attempt++) {
            try {
                String itemJson = objectMapper.writeValueAsString(item);
                return redisTemplate.opsForZSet().score(queueKey, itemJson);
            } catch (Exception e) {
                logger.warn("获取元素分数时出错，等待{}ms后重试，当前尝试次数: {}/{}", 
                        RETRY_DELAY_MS, attempt + 1, MAX_RETRY, e);
                if (attempt == MAX_RETRY - 1) {
                    throw new QueueException("获取元素分数失败，已达到最大重试次数", e);
                }
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new QueueException("获取元素分数重试被中断", ie);
                }
            }
        }
        
        return null;
    }
    
    /**
     * 更新元素的分数
     * @param item 要更新的元素
     * @param score 新的分数
     * @return 更新是否成功
     */
    public boolean updateScore(T item, double score) {
        for (int attempt = 0; attempt < MAX_RETRY; attempt++) {
            try {
                // 序列化对象
                String itemJson = objectMapper.writeValueAsString(item);
                
                // 使用原子序列号作为微小增量，确保高并发下分数唯一
                long sequence = SEQUENCE.getAndIncrement() % 1000000;
                double uniqueScore = score + (sequence / 1000000000.0);
                
                logger.debug("尝试更新元素分数为: {}", uniqueScore);
                
                // 检查元素是否存在
                Double oldScore = redisTemplate.opsForZSet().score(queueKey, itemJson);
                
                // 更新元素分数
                Boolean result = redisTemplate.opsForZSet().add(queueKey, itemJson, uniqueScore);
                
                // 如果元素不存在，则result为true表示添加成功
                // 如果元素已存在，则result为false，但分数可能已经更新
                if ((result != null && result) || oldScore != null) {
                    logger.debug("更新元素分数成功，新分数: {}", uniqueScore);
                    return true;
                } else {
                    logger.warn("更新元素分数失败，等待{}ms后重试，当前尝试次数: {}/{}", 
                            RETRY_DELAY_MS, attempt + 1, MAX_RETRY);
                    Thread.sleep(RETRY_DELAY_MS);
                }
            } catch (Exception e) {
                logger.error("更新元素分数时发生错误", e);
                if (attempt == MAX_RETRY - 1) {
                    throw new QueueException("更新元素分数失败，已达到最大重试次数", e);
                }
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new QueueException("更新元素分数重试被中断", ie);
                }
            }
        }
        
        return false;
    }
    
    /**
     * 获取元素的排名
     * @param item 要查询的元素
     * @return 元素的排名(从0开始)，如果元素不存在则返回null
     */
    public Long getRank(T item) {
        for (int attempt = 0; attempt < MAX_RETRY; attempt++) {
            try {
                String itemJson = objectMapper.writeValueAsString(item);
                return redisTemplate.opsForZSet().rank(queueKey, itemJson);
            } catch (Exception e) {
                logger.warn("获取元素排名时出错，等待{}ms后重试，当前尝试次数: {}/{}", 
                        RETRY_DELAY_MS, attempt + 1, MAX_RETRY, e);
                if (attempt == MAX_RETRY - 1) {
                    throw new QueueException("获取元素排名失败，已达到最大重试次数", e);
                }
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new QueueException("获取元素排名重试被中断", ie);
                }
            }
        }
        
        return null;
    }
    
    /**
     * 移除指定的元素
     * @param item 要移除的元素
     * @return 操作是否成功
     */
    public boolean remove(T item) {
        for (int attempt = 0; attempt < MAX_RETRY; attempt++) {
            try {
                String itemJson = objectMapper.writeValueAsString(item);
                Long result = redisTemplate.opsForZSet().remove(queueKey, itemJson);
                return result != null && result > 0;
            } catch (Exception e) {
                logger.warn("移除元素时出错，等待{}ms后重试，当前尝试次数: {}/{}", 
                        RETRY_DELAY_MS, attempt + 1, MAX_RETRY, e);
                if (attempt == MAX_RETRY - 1) {
                    throw new QueueException("移除元素失败，已达到最大重试次数", e);
                }
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new QueueException("移除元素重试被中断", ie);
                }
            }
        }
        
        return false;
    }
} 