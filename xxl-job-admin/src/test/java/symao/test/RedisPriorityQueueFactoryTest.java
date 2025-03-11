package symao.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xuxueli.springbootpriorityqueue.model.Task;
import com.xuxueli.springbootpriorityqueue.queue.RedisPriorityQueue;
import com.xuxueli.springbootpriorityqueue.queue.RedisPriorityQueueFactory;
import com.xxl.job.admin.XxlJobAdminApplication;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RedisPriorityQueueFactory单元测试类
 */
@SpringBootTest(classes = XxlJobAdminApplication.class)
public class RedisPriorityQueueFactoryTest {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private RedisPriorityQueueFactory factory;

    /**
     * 用于测试的自定义任务类
     * 注意：必须是静态内部类，否则Jackson无法反序列化
     */
    public static class CustomTask {
        private String id;
        private String name;

        public CustomTask() {
        }

        public CustomTask(String id, String name) {
            this.id = id;
            this.name = name;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    @BeforeEach
    public void setUp() {
        factory = new RedisPriorityQueueFactory(redisTemplate, objectMapper);
    }

    @Test
    @DisplayName("测试获取队列实例")
    public void testGetQueue() {
        // 获取队列实例
        RedisPriorityQueue<Task> queue1 = factory.getQueue("test_queue_1", Task.class);
        assertNotNull(queue1);

        // 清空队列，确保测试环境干净
        queue1.clear();

        // 测试队列基本功能
        Task task = new Task("1", "测试任务", "描述");
        assertTrue(queue1.enqueue(task, 5));
        assertEquals(1, queue1.size());

        // 清理
        queue1.clear();
    }

    @Test
    @DisplayName("测试队列缓存机制")
    public void testQueueCaching() {
        // 获取同名队列的两个实例
        RedisPriorityQueue<Task> queue1 = factory.getQueue("test_queue_2", Task.class);
        RedisPriorityQueue<Task> queue2 = factory.getQueue("test_queue_2", Task.class);

        // 清空队列，确保测试环境干净
        queue1.clear();

        // 验证是同一个实例（缓存机制）
        assertSame(queue1, queue2);

        // 通过一个实例添加元素
        Task task = new Task("1", "测试任务", "描述");
        assertTrue(queue1.enqueue(task, 5));

        // 通过另一个实例验证元素存在
        assertEquals(1, queue2.size());
        Task peekedTask = queue2.peek();
        assertNotNull(peekedTask);
        assertEquals("1", peekedTask.getId());

        // 清理
        queue1.clear();
    }

    @Test
    @DisplayName("测试不同名称的队列")
    public void testDifferentQueues() {
        // 获取不同名称的队列
        RedisPriorityQueue<Task> queue1 = factory.getQueue("test_queue_3", Task.class);
        RedisPriorityQueue<Task> queue2 = factory.getQueue("test_queue_4", Task.class);

        // 清空队列，确保测试环境干净
        queue1.clear();
        queue2.clear();

        // 验证是不同的实例
        assertNotSame(queue1, queue2);

        // 在第一个队列中添加元素
        Task task1 = new Task("1", "任务1", "描述1");
        assertTrue(queue1.enqueue(task1, 5));

        // 在第二个队列中添加元素
        Task task2 = new Task("2", "任务2", "描述2");
        assertTrue(queue2.enqueue(task2, 3));

        // 验证队列独立性
        assertEquals(1, queue1.size());
        assertEquals(1, queue2.size());

        Task peekedTask1 = queue1.peek();
        assertNotNull(peekedTask1);
        assertEquals("1", peekedTask1.getId());

        Task peekedTask2 = queue2.peek();
        assertNotNull(peekedTask2);
        assertEquals("2", peekedTask2.getId());

        // 清理
        queue1.clear();
        queue2.clear();
    }

    @Test
    @DisplayName("测试不同类型的队列")
    public void testDifferentTypeQueues() {
        // 获取不同类型的队列
        RedisPriorityQueue<Task> taskQueue = factory.getQueue("test_queue_5", Task.class);
        RedisPriorityQueue<CustomTask> customTaskQueue = factory.getQueue("test_queue_6", CustomTask.class);

        // 清空队列，确保测试环境干净
        taskQueue.clear();
        customTaskQueue.clear();

        // 在任务队列中添加元素
        Task task = new Task("1", "任务", "描述");
        assertTrue(taskQueue.enqueue(task, 5));

        // 在自定义任务队列中添加元素
        CustomTask customTask = new CustomTask("2", "自定义任务");
        assertTrue(customTaskQueue.enqueue(customTask, 3));

        // 验证队列独立性和类型正确性
        Task peekedTask = taskQueue.peek();
        assertNotNull(peekedTask);
        assertEquals("1", peekedTask.getId());

        CustomTask peekedCustomTask = customTaskQueue.peek();
        assertNotNull(peekedCustomTask);
        assertEquals("2", peekedCustomTask.getId());

        // 清理
        taskQueue.clear();
        customTaskQueue.clear();
    }

    @Test
    @DisplayName("测试同名但不同类型的队列")
    public void testSameNameDifferentTypeQueues() {
        // 获取同名但不同类型的队列
        RedisPriorityQueue<Task> taskQueue = factory.getQueue("test_queue_7", Task.class);
        
        // 清空队列，确保测试环境干净
        taskQueue.clear();
        
        // 尝试获取同名但不同类型的队列应该抛出异常
        Exception exception = assertThrows(ClassCastException.class, () -> {
            factory.getQueue("test_queue_7", String.class);
        });
        
        // 清理
        taskQueue.clear();
    }

    @Test
    public void wew() throws InterruptedException {
        RedisPriorityQueue<String> taskQueue = factory.getQueue("test", String.class);
        for (int i = 200; i >= 1 ; i--) {
            Thread.sleep(100);
            taskQueue.enqueue("test"+i,1);
        }

    }


    @Test
    public void deq() {
        RedisPriorityQueue<String> taskQueue = factory.getQueue("symao", String.class);
        System.err.println(taskQueue.dequeue());
    }
} 