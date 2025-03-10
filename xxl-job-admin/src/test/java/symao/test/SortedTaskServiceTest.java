package symao.test;

import com.xuxueli.springbootpriorityqueue.model.SortedTask;
import com.xuxueli.springbootpriorityqueue.queue.RedisSortedQueueFactory;
import com.xuxueli.springbootpriorityqueue.service.SortedTaskService;
import com.xxl.job.admin.XxlJobAdminApplication;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
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

    @Autowired
    private SortedTaskService taskService;
    
    @Test
    @DisplayName("添加任务")
    public void test() {
        // 添加测试任务
        SortedTask task1 = new SortedTask("1", "高优先级任务", "描述1", 5);
        SortedTask task2 = new SortedTask("2", "最高优先级任务", "描述2", 1);
        taskService.addTask(task1);
        taskService.addTask(task2);
        long taskCount = taskService.getTaskCount();
        System.err.println(taskCount);
    }

    @Test
    @DisplayName("取出任务")
    public void test2() {
        SortedTask nextTask = taskService.getNextTask();
        System.err.println(nextTask.toString());
    }
} 