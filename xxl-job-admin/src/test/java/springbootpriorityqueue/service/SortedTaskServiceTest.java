package springbootpriorityqueue.service;

import com.xuxueli.springbootpriorityqueue.service.SortedTaskService;
import com.xuxueli.springbootpriorityqueue.model.SortedTask;
import com.xuxueli.springbootpriorityqueue.queue.RedisSortedQueueFactory;
import com.xxl.job.admin.XxlJobAdminApplication;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 排序任务服务测试类
 * 
 * 该测试类验证了SortedTaskService的所有核心功能，包括：
 * 1. 添加任务到队列
 * 2. 获取并移除优先级最高的任务
 * 3. 查看下一个任务但不移除
 * 4. 按分数范围获取任务
 * 5. 更新任务优先级
 * 6. 从队列中移除指定任务
 * 
 * 注意：运行此测试需要一个实际的Redis服务器实例
 */
@SpringBootTest(classes = XxlJobAdminApplication.class)
public class SortedTaskServiceTest {

    @Autowired
    private RedisSortedQueueFactory queueFactory;
    
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    
    private SortedTaskService taskService;
    

    /**
     * 在每个测试方法执行后清理测试环境
     */
    @AfterEach
    public void cleanup() {
        taskService.clearTasks();
        System.out.println("已清空任务队列");
        System.out.println("========== 测试结束 ==========\n");
    }
    
    /**
     * 测试添加和获取任务功能
     * 
     * 测试目的：验证任务服务能够正确添加任务并按优先级顺序获取任务
     * 期望结果：
     * 1. 高优先级任务(priority=1.5)先被获取
     * 2. 低优先级任务(priority=5.0)后被获取
     */
    @Test
    @DisplayName("测试添加和获取任务")
    public void testAddAndGetTask() {
        System.out.println("测试添加和获取任务功能");
        
        // 添加测试任务
        SortedTask task1 = new SortedTask("1", "高优先级任务", "描述1", 5.0);
        SortedTask task2 = new SortedTask("2", "最高优先级任务", "描述2", 1.5);
        
        System.out.println("添加两个任务：一个优先级5.0，一个优先级1.5");
        assertTrue(taskService.addTask(task1));
        assertTrue(taskService.addTask(task2));
        System.out.println("当前队列任务数量: " + taskService.getTaskCount());
        assertEquals(2, taskService.getTaskCount());
        
        // 验证任务出队顺序
        System.out.println("获取第一个任务（应该是优先级最高的）");
        SortedTask nextTask = taskService.getNextTask();
        assertNotNull(nextTask);
        assertEquals("2", nextTask.getId());
        System.out.println("获取到的任务ID: " + nextTask.getId() + ", 名称: " + nextTask.getName() + ", 优先级: " + nextTask.getPriority());
        
        System.out.println("获取第二个任务");
        nextTask = taskService.getNextTask();
        assertNotNull(nextTask);
        assertEquals("1", nextTask.getId());
        System.out.println("获取到的任务ID: " + nextTask.getId() + ", 名称: " + nextTask.getName() + ", 优先级: " + nextTask.getPriority());
        
        System.out.println("任务队列现在为空，任务数量: " + taskService.getTaskCount());
        assertTrue(taskService.isQueueEmpty());
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
    @DisplayName("测试查看下一个任务")
    public void testPeekNextTask() {
        System.out.println("测试查看下一个任务功能");
        
        // 添加测试任务
        SortedTask task = new SortedTask("1", "测试任务", "描述", 3.0);
        System.out.println("添加一个任务到队列，优先级: " + task.getPriority());
        assertTrue(taskService.addTask(task));
        System.out.println("当前队列任务数量: " + taskService.getTaskCount());
        
        // 验证peek不移除元素
        System.out.println("查看下一个任务");
        SortedTask peekedTask = taskService.peekNextTask();
        assertNotNull(peekedTask);
        assertEquals("1", peekedTask.getId());
        System.out.println("查看到的任务ID: " + peekedTask.getId() + ", 名称: " + peekedTask.getName());
        
        // 再次peek应该返回相同的任务
        System.out.println("再次查看下一个任务");
        SortedTask peekedAgain = taskService.peekNextTask();
        assertNotNull(peekedAgain);
        assertEquals("1", peekedAgain.getId());
        System.out.println("再次查看到的任务ID: " + peekedAgain.getId() + "，与第一次相同");
        
        // 队列中的任务数量应该不变
        assertEquals(1, taskService.getTaskCount());
        System.out.println("peek操作后队列任务数量: " + taskService.getTaskCount() + "，任务仍在队列中");
    }
    
    /**
     * 测试按分数范围获取任务功能
     * 
     * 测试目的：验证任务服务能够正确按分数范围获取任务
     * 期望结果：
     * 1. getHighPriorityTasks方法返回优先级0-3的所有任务
     * 2. getMediumPriorityTasks方法返回优先级3.1-7的所有任务
     * 3. getLowPriorityTasks方法返回优先级7.1-10的所有任务
     */
    @Test
    @DisplayName("测试按优先级范围获取任务")
    public void testGetTasksByPriority() {
        System.out.println("测试按优先级范围获取任务功能");
        
        // 添加不同优先级的任务
        SortedTask highTask1 = new SortedTask("1", "高优先级1", "描述", 1.5);
        SortedTask highTask2 = new SortedTask("2", "高优先级2", "描述", 2.8);
        SortedTask mediumTask1 = new SortedTask("3", "中优先级1", "描述", 4.2);
        SortedTask mediumTask2 = new SortedTask("4", "中优先级2", "描述", 6.5);
        SortedTask lowTask = new SortedTask("5", "低优先级", "描述", 8.0);
        
        System.out.println("添加5个不同优先级的任务到队列");
        taskService.addTask(highTask1);
        taskService.addTask(highTask2);
        taskService.addTask(mediumTask1);
        taskService.addTask(mediumTask2);
        taskService.addTask(lowTask);
        System.out.println("当前队列任务总数量: " + taskService.getTaskCount());
        
        // 验证高优先级任务
        System.out.println("获取高优先级任务(0-3)");
        Set<SortedTask> highPriorityTasks = taskService.getHighPriorityTasks();
        System.out.println("高优先级任务数量: " + highPriorityTasks.size());
        for (SortedTask task : highPriorityTasks) {
            System.out.println("高优先级任务: " + task.getId() + " - " + task.getName() + ", 优先级: " + task.getPriority());
        }
        
        assertEquals(2, highPriorityTasks.size());
        assertTrue(highPriorityTasks.stream().anyMatch(task -> "1".equals(task.getId())));
        assertTrue(highPriorityTasks.stream().anyMatch(task -> "2".equals(task.getId())));
        
        // 验证中优先级任务
        System.out.println("获取中优先级任务(3.1-7)");
        Set<SortedTask> mediumPriorityTasks = taskService.getMediumPriorityTasks();
        assertEquals(2, mediumPriorityTasks.size());
        assertTrue(mediumPriorityTasks.stream().anyMatch(task -> "3".equals(task.getId())));
        assertTrue(mediumPriorityTasks.stream().anyMatch(task -> "4".equals(task.getId())));
        System.out.println("中优先级任务数量: " + mediumPriorityTasks.size());
        for (SortedTask task : mediumPriorityTasks) {
            System.out.println("中优先级任务: " + task.getId() + " - " + task.getName() + ", 优先级: " + task.getPriority());
        }
        
        // 验证低优先级任务
        System.out.println("获取低优先级任务(7.1-10)");
        Set<SortedTask> lowPriorityTasks = taskService.getLowPriorityTasks();
        assertEquals(1, lowPriorityTasks.size());
        assertTrue(lowPriorityTasks.stream().anyMatch(task -> "5".equals(task.getId())));
        System.out.println("低优先级任务数量: " + lowPriorityTasks.size());
        for (SortedTask task : lowPriorityTasks) {
            System.out.println("低优先级任务: " + task.getId() + " - " + task.getName() + ", 优先级: " + task.getPriority());
        }
        
        // 获取任务不会从队列中移除任务
        assertEquals(5, taskService.getTaskCount());
        System.out.println("按优先级获取任务后，队列任务总数量仍为: " + taskService.getTaskCount());
    }
    
    /**
     * 测试更新任务优先级功能
     * 
     * 测试目的：验证任务服务能够正确更新任务的优先级
     * 期望结果：
     * 1. 更新任务优先级后，任务的出队顺序会相应改变
     * 2. 更新后的任务会按照新的优先级排序
     */
    @Test
    @DisplayName("测试更新任务优先级")
    public void testUpdateTaskPriority() {
        System.out.println("测试更新任务优先级功能");
        
        // 添加测试任务
        SortedTask task1 = new SortedTask("1", "任务1", "描述1", 5.0);
        SortedTask task2 = new SortedTask("2", "任务2", "描述2", 3.0);
        
        System.out.println("添加两个任务：优先级5.0和3.0");
        taskService.addTask(task1);
        taskService.addTask(task2);
        
        // 验证初始出队顺序
        System.out.println("初始情况下，任务2(优先级3.0)应该先出队");
        SortedTask nextTask = taskService.peekNextTask();
        assertNotNull(nextTask);
        assertEquals("2", nextTask.getId());
        System.out.println("当前队列中优先级最高的任务ID: " + nextTask.getId() + ", 优先级: " + nextTask.getPriority());
        
        // 更新任务1的优先级，使其比任务2高
        System.out.println("更新任务1的优先级从5.0到1.0");
        assertTrue(taskService.updateTaskPriority(task1, 1.0));
        
        // 验证更新后的出队顺序
        System.out.println("更新后，任务1(优先级1.0)应该先出队");
        nextTask = taskService.peekNextTask();
        assertNotNull(nextTask);
        assertEquals("1", nextTask.getId());
        System.out.println("更新后队列中优先级最高的任务ID: " + nextTask.getId() + ", 优先级: " + nextTask.getPriority());
        
        // 验证出队顺序
        System.out.println("验证出队顺序");
        nextTask = taskService.getNextTask();
        assertNotNull(nextTask);
        assertEquals("1", nextTask.getId());
        System.out.println("第一个出队的任务ID: " + nextTask.getId() + ", 优先级: " + nextTask.getPriority());
        
        nextTask = taskService.getNextTask();
        assertNotNull(nextTask);
        assertEquals("2", nextTask.getId());
        System.out.println("第二个出队的任务ID: " + nextTask.getId() + ", 优先级: " + nextTask.getPriority());
    }
    
    /**
     * 测试移除任务功能
     * 
     * 测试目的：验证任务服务能够正确从队列中移除指定任务
     * 期望结果：
     * 1. 移除任务后，队列中不再包含该任务
     * 2. 移除任务后，队列长度减少
     */
    @Test
    @DisplayName("测试移除任务")
    public void testRemoveTask() {
        System.out.println("测试移除任务功能");
        
        // 添加测试任务
        SortedTask task1 = new SortedTask("1", "任务1", "描述1", 2.0);
        SortedTask task2 = new SortedTask("2", "任务2", "描述2", 4.0);
        SortedTask task3 = new SortedTask("3", "任务3", "描述3", 6.0);
        
        System.out.println("添加三个任务到队列");
        taskService.addTask(task1);
        taskService.addTask(task2);
        taskService.addTask(task3);
        assertEquals(3, taskService.getTaskCount());
        System.out.println("当前队列任务数量: " + taskService.getTaskCount());
        
        // 移除中间优先级的任务
        System.out.println("移除任务2");
        assertTrue(taskService.removeTask(task2));
        assertEquals(2, taskService.getTaskCount());
        System.out.println("移除后队列任务数量: " + taskService.getTaskCount());
        
        // 验证剩余任务的顺序
        System.out.println("验证剩余任务的顺序");
        SortedTask nextTask = taskService.getNextTask();
        assertNotNull(nextTask);
        assertEquals("1", nextTask.getId());
        System.out.println("第一个出队的任务ID: " + nextTask.getId() + ", 优先级: " + nextTask.getPriority());
        
        nextTask = taskService.getNextTask();
        assertNotNull(nextTask);
        assertEquals("3", nextTask.getId());
        System.out.println("第二个出队的任务ID: " + nextTask.getId() + ", 优先级: " + nextTask.getPriority());
        
        // 队列应该为空
        assertTrue(taskService.isQueueEmpty());
        System.out.println("所有任务已出队，队列为空: " + taskService.isQueueEmpty());
    }
    
    /**
     * 测试大量任务的性能
     * 
     * 测试目的：验证任务服务在处理大量任务时的性能
     * 期望结果：
     * 1. 能够成功添加大量任务
     * 2. 能够按照优先级正确获取任务
     */
    @Test
    @DisplayName("测试大量任务性能")
    public void testBulkTasks() {
        System.out.println("测试大量任务性能");
        
        // 添加100个随机优先级的任务
        int taskCount = 100;
        System.out.println("添加" + taskCount + "个随机优先级的任务");
        
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < taskCount; i++) {
            String id = UUID.randomUUID().toString().substring(0, 8);
            double priority = Math.random() * 10; // 0-10之间的随机优先级
            SortedTask task = new SortedTask(id, "批量任务" + i, "批量测试", priority);
            taskService.addTask(task);
        }
        
        long endTime = System.currentTimeMillis();
        System.out.println("添加" + taskCount + "个任务耗时: " + (endTime - startTime) + "ms");
        
        assertEquals(taskCount, taskService.getTaskCount());
        System.out.println("当前队列任务数量: " + taskService.getTaskCount());
        
        // 获取高优先级任务
        Set<SortedTask> highPriorityTasks = taskService.getHighPriorityTasks();
        System.out.println("高优先级任务(0-3)数量: " + highPriorityTasks.size());
        
        // 获取并移除所有任务，验证优先级顺序
        System.out.println("开始按优先级顺序获取所有任务");
        startTime = System.currentTimeMillis();
        
        double lastPriority = -1;
        int count = 0;
        SortedTask task;
        
        while ((task = taskService.getNextTask()) != null) {
            count++;
            // 验证优先级顺序（优先级越小越先出队）
            if (lastPriority >= 0) {
                assertTrue(task.getPriority() >= lastPriority);
            }
            lastPriority = task.getPriority();
            
            // 只打印前5个和最后5个任务，避免输出过多
            if (count <= 5 || count > taskCount - 5) {
                System.out.println("出队任务 #" + count + ": ID=" + task.getId() + ", 优先级=" + task.getPriority());
            } else if (count == 6) {
                System.out.println("...... 省略中间任务 ......");
            }
        }
        
        endTime = System.currentTimeMillis();
        System.out.println("获取" + count + "个任务耗时: " + (endTime - startTime) + "ms");
        
        // 验证所有任务都已出队
        assertEquals(0, taskService.getTaskCount());
        assertTrue(taskService.isQueueEmpty());
        System.out.println("所有任务已出队，队列为空");
    }
} 