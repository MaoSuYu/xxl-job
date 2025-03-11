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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RedisPriorityQueue单元测试类
 */
@SpringBootTest(classes = XxlJobAdminApplication.class)
public class RedisPriorityQueueTest {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private RedisPriorityQueue<Task> taskQueue;
    private final String TEST_QUEUE_NAME = "test_priority_queue";

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
        taskQueue.clear();
    }

    @Test
    @DisplayName("测试入队和出队操作")
    public void testEnqueueAndDequeue() {
        // 创建测试任务
        Task task1 = new Task("1", "任务1", "描述1");
        Task task2 = new Task("2", "任务2", "描述2");
        Task task3 = new Task("3", "任务3", "描述3");

        // 测试入队操作
        assertTrue(taskQueue.enqueue(task1, 5));
        assertTrue(taskQueue.enqueue(task2, 2)); // 高优先级
        assertTrue(taskQueue.enqueue(task3, 8)); // 低优先级

        // 验证队列长度
        assertEquals(3, taskQueue.size());
        assertFalse(taskQueue.isEmpty());

        // 测试出队操作 - 应该按优先级顺序出队
        Task dequeuedTask1 = taskQueue.dequeue();
        assertNotNull(dequeuedTask1);
        assertEquals("2", dequeuedTask1.getId()); // 优先级最高的先出队

        Task dequeuedTask2 = taskQueue.dequeue();
        assertNotNull(dequeuedTask2);
        assertEquals("1", dequeuedTask2.getId()); // 中等优先级的第二个出队

        Task dequeuedTask3 = taskQueue.dequeue();
        assertNotNull(dequeuedTask3);
        assertEquals("3", dequeuedTask3.getId()); // 优先级最低的最后出队

        // 队列应该为空
        assertNull(taskQueue.dequeue());
        assertEquals(0, taskQueue.size());
        assertTrue(taskQueue.isEmpty());
    }

    @Test
    @DisplayName("测试查看队首元素但不移除")
    public void testPeek() {
        // 创建测试任务
        Task task1 = new Task("1", "任务1", "描述1");
        Task task2 = new Task("2", "任务2", "描述2");

        // 入队
        assertTrue(taskQueue.enqueue(task1, 5));
        assertTrue(taskQueue.enqueue(task2, 2)); // 高优先级

        // 测试peek操作
        Task peekedTask = taskQueue.peek();
        assertNotNull(peekedTask);
        assertEquals("2", peekedTask.getId()); // 应该是优先级最高的任务

        // 验证peek不会移除元素
        assertEquals(2, taskQueue.size());
        
        // 再次peek，应该还是同一个任务
        Task peekedTaskAgain = taskQueue.peek();
        assertNotNull(peekedTaskAgain);
        assertEquals("2", peekedTaskAgain.getId());
    }

    @Test
    @DisplayName("测试清空队列")
    public void testClear() {
        // 创建测试任务
        Task task1 = new Task("1", "任务1", "描述1");
        Task task2 = new Task("2", "任务2", "描述2");

        // 入队
        assertTrue(taskQueue.enqueue(task1, 5));
        assertTrue(taskQueue.enqueue(task2, 2));
        
        // 验证队列不为空
        assertEquals(2, taskQueue.size());
        assertFalse(taskQueue.isEmpty());

        // 测试清空队列
        assertTrue(taskQueue.clear());
        
        // 验证队列已清空
        assertEquals(0, taskQueue.size());
        assertTrue(taskQueue.isEmpty());
    }

    @Test
    @DisplayName("测试按优先级范围获取任务")
    public void testGetItemsByPriorityRange() {
        // 创建测试任务
        Task task1 = new Task("1", "高优先级任务", "描述1");
        Task task2 = new Task("2", "高优先级任务", "描述2");
        Task task3 = new Task("3", "中优先级任务", "描述3");
        Task task4 = new Task("4", "中优先级任务", "描述4");
        Task task5 = new Task("5", "低优先级任务", "描述5");
        Task task6 = new Task("6", "低优先级任务", "描述6");

        // 入队不同优先级的任务
        assertTrue(taskQueue.enqueue(task1, 1)); // 高优先级
        assertTrue(taskQueue.enqueue(task2, 3)); // 高优先级
        assertTrue(taskQueue.enqueue(task3, 5)); // 中优先级
        assertTrue(taskQueue.enqueue(task4, 7)); // 中优先级
        assertTrue(taskQueue.enqueue(task5, 8)); // 低优先级
        assertTrue(taskQueue.enqueue(task6, 10)); // 低优先级

        // 测试获取高优先级任务(1-3)
        Set<Task> highPriorityTasks = taskQueue.getItemsByPriorityRange(1, 3);
        assertEquals(2, highPriorityTasks.size());
        assertTrue(highPriorityTasks.contains(task1));
        assertTrue(highPriorityTasks.contains(task2));

        // 测试获取中优先级任务(4-7)
        Set<Task> mediumPriorityTasks = taskQueue.getItemsByPriorityRange(4, 7);
        assertEquals(2, mediumPriorityTasks.size());
        assertTrue(mediumPriorityTasks.contains(task3));
        assertTrue(mediumPriorityTasks.contains(task4));

        // 测试获取低优先级任务(8-10)
        Set<Task> lowPriorityTasks = taskQueue.getItemsByPriorityRange(8, 10);
        assertEquals(2, lowPriorityTasks.size());
        assertTrue(lowPriorityTasks.contains(task5));
        assertTrue(lowPriorityTasks.contains(task6));
    }

    @Test
    @DisplayName("测试优先级边界值")
    public void testPriorityBoundaries() {
        // 创建测试任务
        Task task = new Task("1", "测试任务", "描述");

        // 测试最小优先级
        assertTrue(taskQueue.enqueue(task, 1));
        taskQueue.clear();

        // 测试最大优先级
        assertTrue(taskQueue.enqueue(task, 10));
        taskQueue.clear();

        // 测试无效的优先级值
        Exception exception1 = assertThrows(IllegalArgumentException.class, () -> {
            taskQueue.enqueue(task, 0); // 小于最小优先级
        });
        assertTrue(exception1.getMessage().contains("优先级必须在1-10之间"));

        Exception exception2 = assertThrows(IllegalArgumentException.class, () -> {
            taskQueue.enqueue(task, 11); // 大于最大优先级
        });
        assertTrue(exception2.getMessage().contains("优先级必须在1-10之间"));
    }

    @Test
    @DisplayName("测试相同优先级的任务按FIFO顺序出队")
    public void testFIFOOrderForSamePriority() {
        // 创建测试任务
        Task task1 = new Task("1", "任务1", "描述1");
        Task task2 = new Task("2", "任务2", "描述2");
        Task task3 = new Task("3", "任务3", "描述3");

        // 入队相同优先级的任务
        assertTrue(taskQueue.enqueue(task1, 5));
        assertTrue(taskQueue.enqueue(task2, 5));
        assertTrue(taskQueue.enqueue(task3, 5));

        // 验证按FIFO顺序出队
        Task dequeuedTask1 = taskQueue.dequeue();
        assertNotNull(dequeuedTask1);
        assertEquals("1", dequeuedTask1.getId());

        Task dequeuedTask2 = taskQueue.dequeue();
        assertNotNull(dequeuedTask2);
        assertEquals("2", dequeuedTask2.getId());

        Task dequeuedTask3 = taskQueue.dequeue();
        assertNotNull(dequeuedTask3);
        assertEquals("3", dequeuedTask3.getId());
    }

    @Test
    @DisplayName("测试队列为空时的行为")
    public void testEmptyQueueBehavior() {
        // 验证空队列的初始状态
        assertTrue(taskQueue.isEmpty());
        assertEquals(0, taskQueue.size());
        
        // 测试从空队列中获取元素
        assertNull(taskQueue.peek());
        assertNull(taskQueue.dequeue());
        
        // 测试获取空队列的优先级范围
        Set<Task> tasks = taskQueue.getItemsByPriorityRange(1, 10);
        assertNotNull(tasks);
        assertTrue(tasks.isEmpty());
    }
} 