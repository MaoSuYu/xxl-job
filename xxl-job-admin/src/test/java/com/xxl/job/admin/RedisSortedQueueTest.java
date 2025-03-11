package com.xxl.job.admin;

import com.xuxueli.springbootpriorityqueue.model.SortedTask;
import com.xuxueli.springbootpriorityqueue.queue.RedisSortedQueue;
import com.xuxueli.springbootpriorityqueue.queue.RedisSortedQueueFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * RedisSortedQueue测试类
 * 
 * 该测试类仅测试RedisSortedQueue中数据的有序性
 */
@SpringBootTest(classes = XxlJobAdminApplication.class)
public class RedisSortedQueueTest {

    @Autowired
    private RedisSortedQueueFactory queueFactory;
    
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    // 创建测试队列
    String queueName = "test_insert_thousand_";
    
    /**
     * 测试向RedisSortedQueue中插入一千条数据
     */
    @Test
    public void testInsertThousandItems() {
        RedisSortedQueue<String> queue = queueFactory.getQueue(queueName, String.class,false);

        // 清空队列，确保测试环境干净
        queue.clear();
        queue.enqueue("3",3);
        queue.enqueue("4",4);
        queue.enqueue("1",1);
        queue.enqueue("2",2);
    }

    @Test
    public void dequeue() {
        RedisSortedQueue<String> queue = queueFactory.getQueue(queueName, String.class);
        String dequeue = queue.dequeue();
        System.err.println(dequeue);
    }
} 