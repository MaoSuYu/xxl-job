package springbootpriorityqueue.queue;

import com.xuxueli.springbootpriorityqueue.model.Task;
import com.xuxueli.springbootpriorityqueue.queue.RedisPriorityQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Redis优先级队列的核心功能测试类
 * 
 * 该测试类验证了Redis优先级队列的所有核心功能，包括：
 * 1. 入队和出队操作，验证优先级排序是否正确
 * 2. 查看队列头部元素而不移除
 * 3. 按优先级范围获取元素
 * 4. 验证优先级参数范围检查
 * 
 * 注意：运行此测试需要一个实际的Redis服务器实例
 */
@SpringBootTest
public class RedisPriorityQueueTest {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    
    private RedisPriorityQueue<Task> taskQueue;
    
    /**
     * 在每个测试方法执行前设置测试环境
     * 创建一个新的任务队列并清空已有数据
     */
    @BeforeEach
    public void setup() {
        System.out.println("========== 开始测试优先级队列 ==========");
        // 创建队列并确保它是空的
        taskQueue = new RedisPriorityQueue<>(redisTemplate, "test-tasks", Task.class);
        taskQueue.clear();
        System.out.println("已创建并清空测试任务队列");
    }
    
    /**
     * 测试入队和出队操作
     * 
     * 测试目的：验证优先级队列能够正确按照优先级顺序出队，而不是按入队顺序
     * 期望结果：
     * 1. 高优先级任务(priority=1)先出队
     * 2. 中优先级任务(priority=3)其次出队
     * 3. 低优先级任务(priority=5)最后出队
     * 4. 队列清空后返回null
     */
    @Test
    public void testEnqueueAndDequeue() {
        System.out.println("测试 enqueue 和 dequeue 操作");
        
        // 准备测试数据
        Task task1 = new Task("1", "高优先级任务", "这是一个高优先级任务");
        Task task2 = new Task("2", "中优先级任务", "这是一个中优先级任务");
        Task task3 = new Task("3", "低优先级任务", "这是一个低优先级任务");
        
        // 添加任务到队列，注意顺序与优先级无关
        System.out.println("添加任务到队列：先低优先级，再高优先级，最后中优先级");
        taskQueue.enqueue(task3, 5); // 低优先级先入队
        taskQueue.enqueue(task1, 1); // 高优先级后入队
        taskQueue.enqueue(task2, 3); // 中优先级最后入队
        
        // 验证出队顺序（应该按优先级出队，而不是添加顺序）
        Task dequeuedTask1 = taskQueue.dequeue();
        assertNotNull(dequeuedTask1);
        assertEquals("1", dequeuedTask1.getId()); // 应该是高优先级任务
        System.out.println("第一个出队的任务 ID: " + dequeuedTask1.getId() + ", 名称: " + dequeuedTask1.getName());
        
        Task dequeuedTask2 = taskQueue.dequeue();
        assertNotNull(dequeuedTask2);
        assertEquals("2", dequeuedTask2.getId()); // 应该是中优先级任务
        System.out.println("第二个出队的任务 ID: " + dequeuedTask2.getId() + ", 名称: " + dequeuedTask2.getName());
        
        Task dequeuedTask3 = taskQueue.dequeue();
        assertNotNull(dequeuedTask3);
        assertEquals("3", dequeuedTask3.getId()); // 应该是低优先级任务
        System.out.println("第三个出队的任务 ID: " + dequeuedTask3.getId() + ", 名称: " + dequeuedTask3.getName());
        
        // 队列应该为空
        Task nullTask = taskQueue.dequeue();
        assertNull(nullTask);
        System.out.println("队列为空，再次出队返回: " + nullTask);
    }
    
    /**
     * 测试查看队列头部元素功能
     * 
     * 测试目的：验证peek操作可以查看队列中的下一个元素但不移除它
     * 期望结果：
     * 1. peek返回优先级最高的元素
     * 2. 多次peek返回相同的元素
     * 3. peek后再dequeue应返回相同的元素
     */
    @Test
    public void testPeek() {
        System.out.println("测试 peek 操作");
        
        // 准备测试数据
        Task task1 = new Task("1", "高优先级任务", "这是一个高优先级任务");
        Task task2 = new Task("2", "低优先级任务", "这是一个低优先级任务");
        
        // 添加任务到队列
        System.out.println("添加任务到队列：一个低优先级，一个高优先级");
        taskQueue.enqueue(task2, 5);
        taskQueue.enqueue(task1, 1);
        
        // peek应该返回优先级最高的任务但不移除它
        Task peekedTask = taskQueue.peek();
        assertNotNull(peekedTask);
        assertEquals("1", peekedTask.getId());
        System.out.println("第一次peek的任务 ID: " + peekedTask.getId() + ", 名称: " + peekedTask.getName());
        
        // 再次peek应该返回相同的任务
        Task peekedAgain = taskQueue.peek();
        assertNotNull(peekedAgain);
        assertEquals("1", peekedAgain.getId());
        System.out.println("第二次peek的任务 ID: " + peekedAgain.getId() + "，与第一次相同");
        
        // dequeue应该返回相同的任务并移除它
        Task dequeuedTask = taskQueue.dequeue();
        assertNotNull(dequeuedTask);
        assertEquals("1", dequeuedTask.getId());
        System.out.println("dequeue操作返回的任务 ID: " + dequeuedTask.getId() + "，与peek相同");
        
        // 再次peek应该返回下一个任务
        Task nextTask = taskQueue.peek();
        assertNotNull(nextTask);
        assertEquals("2", nextTask.getId());
        System.out.println("再次peek的任务 ID: " + nextTask.getId() + ", 名称: " + nextTask.getName());
    }
    
    /**
     * 测试按优先级范围获取元素
     * 
     * 测试目的：验证getItemsByPriorityRange方法能正确获取指定优先级范围内的所有任务
     * 期望结果：
     * 1. 范围1-3内有2个高优先级任务
     * 2. 范围4-7内有1个中优先级任务
     */
    @Test
    public void testGetItemsByPriorityRange() {
        System.out.println("测试 getItemsByPriorityRange 操作");
        
        // 准备测试数据 - 不同优先级的任务
        Task task1 = new Task("1", "高优先级任务", "优先级1");
        Task task2 = new Task("2", "高优先级任务", "优先级2");
        Task task3 = new Task("3", "中优先级任务", "优先级5");
        
        // 添加任务到队列
        System.out.println("添加3个不同优先级的任务到队列");
        taskQueue.enqueue(task1, 1);
        taskQueue.enqueue(task2, 2);
        taskQueue.enqueue(task3, 5);
        
        // 获取高优先级任务（1-3）
        Set<Task> highPriorityTasks = taskQueue.getItemsByPriorityRange(1, 3);
        assertEquals(2, highPriorityTasks.size());
        assertTrue(highPriorityTasks.stream().anyMatch(task -> "1".equals(task.getId())));
        assertTrue(highPriorityTasks.stream().anyMatch(task -> "2".equals(task.getId())));
        System.out.println("优先级范围1-3内的任务数量: " + highPriorityTasks.size());
        System.out.println("包含的任务ID: " + highPriorityTasks.stream().map(Task::getId).reduce("", (a, b) -> a + ", " + b).substring(2));
        
        // 获取中优先级任务（4-7）
        Set<Task> mediumPriorityTasks = taskQueue.getItemsByPriorityRange(4, 7);
        assertEquals(1, mediumPriorityTasks.size());
        assertTrue(mediumPriorityTasks.stream().anyMatch(task -> "3".equals(task.getId())));
        System.out.println("优先级范围4-7内的任务数量: " + mediumPriorityTasks.size());
        System.out.println("包含的任务ID: " + mediumPriorityTasks.stream().map(Task::getId).reduce("", (a, b) -> a + ", " + b).substring(2));
    }
    
    /**
     * 测试优先级参数范围验证
     * 
     * 测试目的：验证当传入无效的优先级参数时队列会抛出异常
     * 期望结果：
     * 1. 优先级小于1时抛出IllegalArgumentException
     * 2. 优先级大于10时抛出IllegalArgumentException
     */
    @Test
    public void testInvalidPriority() {
        System.out.println("测试无效优先级参数");
        
        Task task = new Task("1", "任务", "任务描述");
        
        // 验证优先级范围检查 - 小于1
        System.out.println("测试优先级为0的情况");
        Exception tooLowException = assertThrows(IllegalArgumentException.class, () -> {
            taskQueue.enqueue(task, 0);
        });
        assertTrue(tooLowException.getMessage().contains("优先级必须在1-10之间"));
        System.out.println("优先级为0时抛出异常: " + tooLowException.getMessage());
        
        // 验证优先级范围检查 - 大于10
        System.out.println("测试优先级为11的情况");
        Exception tooHighException = assertThrows(IllegalArgumentException.class, () -> {
            taskQueue.enqueue(task, 11);
        });
        assertTrue(tooHighException.getMessage().contains("优先级必须在1-10之间"));
        System.out.println("优先级为11时抛出异常: " + tooHighException.getMessage());
    }
} 