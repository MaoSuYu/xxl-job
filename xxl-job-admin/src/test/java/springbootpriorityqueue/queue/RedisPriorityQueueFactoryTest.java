package springbootpriorityqueue.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.xuxueli.springbootpriorityqueue.model.Task;
import com.xuxueli.springbootpriorityqueue.queue.RedisPriorityQueue;
import com.xuxueli.springbootpriorityqueue.queue.RedisPriorityQueueFactory;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Redis优先级队列工厂的核心功能测试类
 * 
 * 该测试类验证了Redis优先级队列工厂的核心功能，包括：
 * 1. 创建不同类型的队列实例
 * 2. 缓存和复用队列实例
 * 3. 队列名称隔离
 * 
 * 队列工厂的主要作用是管理队列实例，避免重复创建相同类型和名称的队列，
 * 提高系统效率并确保数据的一致性。
 * 
 * 注意：运行此测试需要一个实际的Redis服务器实例
 */
@SpringBootTest
public class RedisPriorityQueueFactoryTest {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    
    @Autowired
    private ObjectMapper objectMapper;

    /**
     * 测试获取队列实例功能
     * 
     * 测试目的：验证工厂能够正确创建、缓存和复用队列实例
     * 期望结果：
     * 1. 工厂能创建不同类型的队列实例
     * 2. 相同名称和类型的队列返回相同实例(引用相同)
     * 3. 不同名称的队列返回不同实例(引用不同)
     */
    @Test
    public void testGetQueue() {
        System.out.println("========== 测试优先级队列工厂 ==========");
        System.out.println("测试工厂创建和缓存队列实例功能");
        
        RedisPriorityQueueFactory factory = new RedisPriorityQueueFactory(redisTemplate, objectMapper);
        
        // 获取队列
        System.out.println("创建Task类型和String类型的队列实例");
        RedisPriorityQueue<Task> taskQueue = factory.getQueue("tasks", Task.class);
        RedisPriorityQueue<String> stringQueue = factory.getQueue("strings", String.class);
        
        // 验证队列不为null
        assertNotNull(taskQueue);
        assertNotNull(stringQueue);
        System.out.println("成功创建不同类型的队列实例：");
        System.out.println("- Task队列: " + taskQueue);
        System.out.println("- String队列: " + stringQueue);
        
        // 验证相同名称和类型的队列是同一个实例
        System.out.println("测试缓存和复用队列实例");
        RedisPriorityQueue<Task> taskQueue2 = factory.getQueue("tasks", Task.class);
        assertSame(taskQueue, taskQueue2);
        System.out.println("相同名称和类型的队列是否为同一实例: " + (taskQueue == taskQueue2));
        
        // 验证不同名称的队列是不同实例
        System.out.println("测试不同名称的队列");
        RedisPriorityQueue<Task> anotherTaskQueue = factory.getQueue("other-tasks", Task.class);
        assertNotSame(taskQueue, anotherTaskQueue);
        System.out.println("不同名称的队列是否为不同实例: " + (taskQueue != anotherTaskQueue));
        
        // 清理
        System.out.println("清理测试数据");
        taskQueue.clear();
        stringQueue.clear();
        anotherTaskQueue.clear();
    }
    
    /**
     * 测试队列操作功能
     * 
     * 测试目的：验证通过工厂创建的队列能够正确执行队列操作
     * 期望结果：
     * 1. 能够添加任务到队列
     * 2. 能够从队列中获取任务
     * 3. 队列操作符合预期
     */
    @Test
    public void testQueueOperations() {
        System.out.println("========== 测试通过工厂创建的队列操作 ==========");
        RedisPriorityQueueFactory factory = new RedisPriorityQueueFactory(redisTemplate, objectMapper);
        
        // 获取队列
        System.out.println("创建测试操作队列");
        RedisPriorityQueue<Task> taskQueue = factory.getQueue("test-operations", Task.class);
        taskQueue.clear(); // 确保队列为空
        System.out.println("已清空队列");
        
        // 添加任务
        Task task = new Task("1", "测试任务", "通过工厂创建的队列测试任务");
        System.out.println("添加任务: " + task);
        taskQueue.enqueue(task, 5);
        
        // 验证队列操作
        long size = taskQueue.size();
        assertEquals(1, size);
        System.out.println("队列长度: " + size);
        
        System.out.println("出队任务");
        Task dequeuedTask = taskQueue.dequeue();
        assertNotNull(dequeuedTask);
        assertEquals("1", dequeuedTask.getId());
        System.out.println("获取到的任务: " + dequeuedTask);
        
        // 队列应该为空
        assertTrue(taskQueue.isEmpty());
        System.out.println("队列现在为空: " + taskQueue.isEmpty());
        
        // 清理
        taskQueue.clear();
        System.out.println("测试完成，已清理队列");
    }
} 