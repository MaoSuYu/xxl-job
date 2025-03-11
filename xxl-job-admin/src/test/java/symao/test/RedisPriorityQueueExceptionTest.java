package symao.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xuxueli.springbootpriorityqueue.model.Task;
import com.xuxueli.springbootpriorityqueue.queue.RedisPriorityQueue;
import com.xuxueli.springbootpriorityqueue.queue.RedisPriorityQueueFactory;
import com.xxl.job.admin.XxlJobAdminApplication;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * RedisPriorityQueue异常处理测试类
 */
@SpringBootTest(classes = XxlJobAdminApplication.class)
public class RedisPriorityQueueExceptionTest {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private RedisPriorityQueue<Task> taskQueue;
    private final String TEST_QUEUE_NAME = "test_exception_queue";

    /**
     * 创建一个无法序列化的对象类
     * 由于存在循环引用，会导致序列化失败
     */
    public static class UnserializableTask extends Task {
        private final Object cyclicReference;
        
        public UnserializableTask(String id, String name, String description) {
            super(id, name, description);
            // 创建循环引用，导致序列化失败
            this.cyclicReference = this;
        }
    }

    @BeforeEach
    public void setUp() {
        // 创建测试用的优先级队列
        RedisPriorityQueueFactory factory = new RedisPriorityQueueFactory(redisTemplate, objectMapper);
        taskQueue = factory.getQueue(TEST_QUEUE_NAME, Task.class);
        
        // 确保测试开始前队列是空的
        taskQueue.clear();
    }

    @AfterEach
    public void tearDown() {
        // 测试结束后清空队列
        try {
            taskQueue.clear();
        } catch (Exception e) {
            // 忽略清理时的异常
        }
    }

    @Test
    @DisplayName("测试优先级范围验证")
    public void testPriorityRangeValidation() {
        Task task = new Task("1", "测试任务", "描述");
        
        // 测试优先级小于1的情况
        Exception exception1 = assertThrows(IllegalArgumentException.class, () -> {
            taskQueue.enqueue(task, 0);
        });
        assertTrue(exception1.getMessage().contains("优先级必须在1-10之间"));
        
        // 测试优先级大于10的情况
        Exception exception2 = assertThrows(IllegalArgumentException.class, () -> {
            taskQueue.enqueue(task, 11);
        });
        assertTrue(exception2.getMessage().contains("优先级必须在1-10之间"));
    }

    @Test
    @DisplayName("测试getItemsByPriorityRange方法的参数验证")
    public void testGetItemsByPriorityRangeValidation() {
        // 测试最小优先级大于最大优先级的情况
        Exception exception1 = assertThrows(IllegalArgumentException.class, () -> {
            taskQueue.getItemsByPriorityRange(5, 3);
        });
        assertTrue(exception1.getMessage().contains("最小优先级不能大于最大优先级"));
        
        // 测试优先级范围超出1-10的情况
        Exception exception2 = assertThrows(IllegalArgumentException.class, () -> {
            taskQueue.getItemsByPriorityRange(0, 5);
        });
        assertTrue(exception2.getMessage().contains("优先级必须在1-10范围内"));
        
        Exception exception3 = assertThrows(IllegalArgumentException.class, () -> {
            taskQueue.getItemsByPriorityRange(5, 11);
        });
        assertTrue(exception3.getMessage().contains("优先级必须在1-10范围内"));
    }

    @Test
    @DisplayName("测试空队列的行为")
    public void testEmptyQueueBehavior() {
        // 验证空队列的初始状态
        assertTrue(taskQueue.isEmpty());
        assertEquals(0, taskQueue.size());
        
        // 测试从空队列中获取元素
        assertNull(taskQueue.peek());
        assertNull(taskQueue.dequeue());
    }

    @Test
    @DisplayName("测试序列化异常处理")
    public void testSerializationException() {
        // 使用无法序列化的对象
        UnserializableTask unserializableTask = new UnserializableTask("1", "无法序列化的任务", "描述");
        
        // 测试序列化异常
        Exception exception = assertThrows(RedisPriorityQueue.QueueException.class, () -> {
            taskQueue.enqueue(unserializableTask, 5);
        });
        assertTrue(exception.getMessage().contains("序列化对象失败"));
    }

    // 注意：以下测试需要使用Mock对象模拟Redis异常，
    // 但由于Spring Boot测试环境的限制，可能无法直接运行
    // 这些测试展示了如何测试异常情况的思路
    
    /*
    @Test
    @DisplayName("测试Redis连接异常")
    public void testRedisConnectionException() {
        // 创建Mock对象
        RedisTemplate<String, Object> mockRedisTemplate = Mockito.mock(RedisTemplate.class);
        ZSetOperations<String, Object> mockZSetOps = Mockito.mock(ZSetOperations.class);
        
        // 配置Mock行为
        when(mockRedisTemplate.opsForZSet()).thenReturn(mockZSetOps);
        when(mockZSetOps.add(anyString(), any(), anyDouble()))
            .thenThrow(new RedisConnectionFailureException("模拟的Redis连接失败"));
        
        // 创建使用Mock的队列
        RedisPriorityQueueFactory factory = new RedisPriorityQueueFactory(mockRedisTemplate, objectMapper);
        RedisPriorityQueue<Task> mockQueue = factory.getQueue("mock_queue", Task.class);
        
        // 测试Redis异常
        Task task = new Task("1", "测试任务", "描述");
        Exception exception = assertThrows(RedisPriorityQueue.QueueException.class, () -> {
            mockQueue.enqueue(task, 5);
        });
        assertTrue(exception.getMessage().contains("添加任务失败"));
    }
    */
} 