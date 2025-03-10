package com.xuxueli.springbootpriorityqueue.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.xuxueli.springbootpriorityqueue.queue.RedisPriorityQueueFactory;
import com.xuxueli.springbootpriorityqueue.queue.RedisSortedQueueFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Redis配置类
 */
@Configuration
public class RedisConfig {

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        // 注册JavaTimeModule以支持Java 8日期/时间类型
        objectMapper.registerModule(new JavaTimeModule());
        return objectMapper;
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory, ObjectMapper objectMapper) {
        RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(connectionFactory);
        
        // 使用Jackson2JsonRedisSerializer作为值的序列化器
        Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<>(objectMapper, Object.class);
        
        // 使用StringRedisSerializer作为键的序列化器
        StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();
        
        // 设置key和value的序列化规则
        redisTemplate.setKeySerializer(stringRedisSerializer);
        redisTemplate.setValueSerializer(jackson2JsonRedisSerializer);
        redisTemplate.setHashKeySerializer(stringRedisSerializer);
        redisTemplate.setHashValueSerializer(jackson2JsonRedisSerializer);
        
        redisTemplate.afterPropertiesSet();
        return redisTemplate;
    }
    
    /**
     * 注册RedisPriorityQueueFactory
     */
    @Bean
    public RedisPriorityQueueFactory redisPriorityQueueFactory(RedisTemplate<String, Object> redisTemplate, ObjectMapper objectMapper) {
        return new RedisPriorityQueueFactory(redisTemplate, objectMapper);
    }
    
    /**
     * 注册RedisSortedQueueFactory（新增功能）
     */
    @Bean
    public RedisSortedQueueFactory redisSortedQueueFactory(RedisTemplate<String, Object> redisTemplate) {
        return new RedisSortedQueueFactory(redisTemplate);
    }
}