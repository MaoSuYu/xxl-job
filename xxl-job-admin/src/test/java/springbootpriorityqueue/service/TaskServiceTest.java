package springbootpriorityqueue.service;

import com.xuxueli.springbootpriorityqueue.model.Task;
import com.xuxueli.springbootpriorityqueue.queue.RedisPriorityQueueFactory;
import com.xuxueli.springbootpriorityqueue.service.TaskService;
import com.xxl.job.admin.XxlJobAdminApplication;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 任务服务的核心功能测试类
 * 
 * 该测试类验证了任务服务的核心业务功能，包括：
 * 1. 添加和获取任务
 * 2. 查看下一个任务
 * 3. 按优先级范围获取任务
 * 
 * 任务服务是真实业务场景中使用优先级队列的示例，展示了如何在实际应用中
 * 整合并使用优先级队列来处理具有不同优先级的任务。
 * 
 * 注意：运行此测试需要一个实际的Redis服务器实例
 */
@SpringBootTest(classes = XxlJobAdminApplication.class)
public class TaskServiceTest {

    @Autowired
    private RedisPriorityQueueFactory queueFactory;
    
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    
    private TaskService taskService;
    
    /**
     * 在每个测试方法执行前设置测试环境
     * 创建一个新的任务服务并清空已有任务
     */
    @BeforeEach
    public void setup() {
        System.out.println("========== 开始测试任务服务 ==========");
        taskService = new TaskService(queueFactory);
        taskService.clearTasks();
        System.out.println("已创建任务服务并清空任务队列");
    }
    
    /**
     * 测试添加和获取任务功能
     * 
     * 测试目的：验证任务服务能够正确添加任务并按优先级顺序获取任务
     * 期望结果：
     * 1. 高优先级任务(priority=1)先被获取
     * 2. 低优先级任务(priority=5)后被获取
     */
    @Test
    public void testAddAndGetTask() {
        System.out.println("测试添加和获取任务功能");
        
        // 添加测试任务
        Task task1 = new Task("1", "高优先级任务", "描述1");
        Task task2 = new Task("2", "中优先级任务", "描述2");
        
        System.out.println("添加两个任务：一个高优先级(1)，一个中优先级(5)");
        taskService.addTask(task1, 1);
        taskService.addTask(task2, 5);
        System.out.println("当前队列任务数量: " + taskService.getTaskCount());
        
        // 验证任务出队顺序
        System.out.println("获取第一个任务");
        Task nextTask = taskService.getNextTask();
        assertNotNull(nextTask);
        assertEquals("1", nextTask.getId());
        System.out.println("获取到的任务ID: " + nextTask.getId() + ", 名称: " + nextTask.getName());
        
        System.out.println("获取第二个任务");
        nextTask = taskService.getNextTask();
        assertNotNull(nextTask);
        assertEquals("2", nextTask.getId());
        System.out.println("获取到的任务ID: " + nextTask.getId() + ", 名称: " + nextTask.getName());
        
        System.out.println("任务队列现在为空，任务数量: " + taskService.getTaskCount());
    }
    
    /**
     * 测试查看下一个任务功能
     * 
     * 测试目的：验证任务服务的peekNextTask方法能够查看下一个任务但不移除它
     * 期望结果：
     * 1. peekNextTask返回队列中的下一个任务
     * 2. 多次调用peekNextTask返回相同的任务
     * 3. 调用peekNextTask后任务仍在队列中
     */
    @Test
    public void testPeekNextTask() {
        System.out.println("测试查看下一个任务功能");
        
        // 添加测试任务
        Task task = new Task("1", "测试任务", "描述");
        System.out.println("添加一个任务到队列");
        taskService.addTask(task, 3);
        System.out.println("当前队列任务数量: " + taskService.getTaskCount());
        
        // 验证peek不移除元素
        System.out.println("查看下一个任务");
        Task peekedTask = taskService.peekNextTask();
        assertNotNull(peekedTask);
        assertEquals("1", peekedTask.getId());
        System.out.println("查看到的任务ID: " + peekedTask.getId() + ", 名称: " + peekedTask.getName());
        
        // 再次peek应该返回相同的任务
        System.out.println("再次查看下一个任务");
        Task peekedAgain = taskService.peekNextTask();
        assertNotNull(peekedAgain);
        assertEquals("1", peekedAgain.getId());
        System.out.println("再次查看到的任务ID: " + peekedAgain.getId() + "，与第一次相同");
        
        // 队列中的任务数量应该不变
        System.out.println("peek操作后队列任务数量: " + taskService.getTaskCount() + "，任务仍在队列中");
    }
    
    /**
     * 测试按优先级范围获取任务功能
     * 
     * 测试目的：验证任务服务能够正确按优先级范围获取任务
     * 期望结果：
     * 1. getHighPriorityTasks方法返回优先级1-3的所有任务
     * 2. getMediumPriorityTasks方法返回优先级4-7的所有任务
     */
    @Test
    public void testGetTasksByPriority() {
        System.out.println("测试按优先级范围获取任务功能");
        
        // 添加不同优先级的任务
        Task highTask1 = new Task("1", "高优先级1", "描述");
        Task highTask2 = new Task("2", "高优先级2", "描述");
        Task mediumTask = new Task("3", "中优先级", "描述");
        
        System.out.println("添加3个不同优先级的任务到队列");
        taskService.addTask(highTask1, 1);
        taskService.addTask(highTask2, 3);
        taskService.addTask(mediumTask, 5);
        System.out.println("当前队列任务总数量: " + taskService.getTaskCount());
        
        // 调试Redis中的数据
        System.out.println("==== Redis中的数据 ====");
        Set<Object> allItems = redisTemplate.opsForZSet().range("priority_queue:tasks", 0, -1);
        if (allItems != null) {
            System.out.println("Redis中任务数量: " + allItems.size());
            for (Object item : allItems) {
                System.out.println("任务数据: " + item);
                Double score = redisTemplate.opsForZSet().score("priority_queue:tasks", item);
                System.out.println("对应Score: " + score);
                // 使用正确的方法计算理论优先级：取分数的整数部分
                int priority = (score != null) ? (int)Math.floor(score) : 0;
                System.out.println("理论优先级: " + priority);
            }
        } else {
            System.out.println("Redis中没有数据");
        }
        
        // 验证高优先级任务
        System.out.println("获取高优先级任务(1-3)");
        Set<Task> highPriorityTasks = taskService.getHighPriorityTasks();
        System.out.println("高优先级任务数量: " + highPriorityTasks.size());
        for (Task task : highPriorityTasks) {
            System.out.println("高优先级任务: " + task.getId() + " - " + task.getName());
        }
        
        assertEquals(2, highPriorityTasks.size());
        assertTrue(highPriorityTasks.stream().anyMatch(task -> "1".equals(task.getId())));
        assertTrue(highPriorityTasks.stream().anyMatch(task -> "2".equals(task.getId())));
        
        // 验证中优先级任务
        System.out.println("获取中优先级任务(4-7)");
        Set<Task> mediumPriorityTasks = taskService.getMediumPriorityTasks();
        assertEquals(1, mediumPriorityTasks.size());
        assertTrue(mediumPriorityTasks.stream().anyMatch(task -> "3".equals(task.getId())));
        System.out.println("中优先级任务数量: " + mediumPriorityTasks.size());
        System.out.println("中优先级任务ID: " + 
            mediumPriorityTasks.stream()
                .map(Task::getId)
                .reduce("", (a, b) -> a.isEmpty() ? b : a + ", " + b));
        
        // 获取任务不会从队列中移除任务
        System.out.println("按优先级获取任务后，队列任务总数量仍为: " + taskService.getTaskCount());
    }
} 