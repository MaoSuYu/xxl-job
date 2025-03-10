package springbootpriorityqueue.queue;

import com.xuxueli.springbootpriorityqueue.queue.RedisSortedQueue;
import com.xxl.job.admin.XxlJobAdminApplication;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SpringBootTest(classes = XxlJobAdminApplication.class)
public class SortedTaskServiceTest {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    private RedisSortedQueue<TestTask> queue;
    private static final String QUEUE_NAME = "test_fifo_queue";
    private static final int TOTAL_TASKS = 10000;

    @BeforeEach
    public void setup() {
        // 创建FIFO模式的队列
        queue = new RedisSortedQueue<>(redisTemplate, QUEUE_NAME, TestTask.class, true);
        // 确保队列为空
        queue.clear();
    }

    @AfterEach
    public void cleanup() {
        // 清理测试数据
//        queue.clear();
    }

    /**
     * 测试任务类
     */
    public static class TestTask {
        private int sequence;  // 序号，用于验证顺序
        private String data;   // 测试数据

        public TestTask() {
        }

        public TestTask(int sequence, String data) {
            this.sequence = sequence;
            this.data = data;
        }

        public int getSequence() {
            return sequence;
        }

        public void setSequence(int sequence) {
            this.sequence = sequence;
        }

        public String getData() {
            return data;
        }

        public void setData(String data) {
            this.data = data;
        }

        @Override
        public String toString() {
            return "TestTask{sequence=" + sequence + ", data='" + data + "'}";
        }
    }

    /**
     * 测试单线程下的FIFO特性
     * 按顺序插入10000条数据，验证出队顺序是否与入队顺序一致
     */
    @Test
    public void testFIFOOrderSingleThread() {
        // 1. 按顺序插入10000条数据
        for (int i = 0; i < TOTAL_TASKS; i++) {
            TestTask task = new TestTask(i, "Task-" + i);
            boolean success = queue.enqueue(task, 0); // score在FIFO模式下会被忽略
            assertTrue(success, "入队失败: " + i);
        }

        // 2. 验证队列长度
        assertEquals(TOTAL_TASKS, queue.size(), "队列长度不符");

        // 3. 按顺序出队并验证顺序
        for (int i = 0; i < TOTAL_TASKS; i++) {
            TestTask task = queue.dequeue();
            assertNotNull(task, "出队失败，任务为null: " + i);
            assertEquals(i, task.getSequence(), "任务顺序不符");
        }

        // 4. 验证队列为空
        assertTrue(queue.isEmpty(), "队列应该为空");
    }

    /**
     * 测试多线程并发下的FIFO特性
     * 使用10个线程并发插入数据，每个线程插入1000条数据
     * 验证所有数据是否都被正确处理，且每个线程的数据是否保持顺序
     */
    @Test
    public void testFIFOOrderMultiThread() throws InterruptedException {
        final int THREAD_COUNT = 10;
        final int TASKS_PER_THREAD = TOTAL_TASKS / THREAD_COUNT;
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        // 1. 多线程并发入队
        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            executorService.submit(() -> {
                try {
                    for (int i = 0; i < TASKS_PER_THREAD; i++) {
                        int sequence = threadId * TASKS_PER_THREAD + i;
                        TestTask task = new TestTask(sequence, "Thread-" + threadId + "-Task-" + i);
                        boolean success = queue.enqueue(task, 0);
                        assertTrue(success, "入队失败: " + sequence);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        // 等待所有线程完成入队
        boolean completed = latch.await(30, TimeUnit.SECONDS);
        assertTrue(completed, "入队操作超时");

        // 2. 验证队列长度
        assertEquals(TOTAL_TASKS, queue.size(), "队列长度不符");

        // 3. 收集所有出队的任务
        List<TestTask> dequeuedTasks = new ArrayList<>();
        for (int i = 0; i < TOTAL_TASKS; i++) {
            TestTask task = queue.dequeue();
            assertNotNull(task, "出队失败，任务为null: " + i);
            dequeuedTasks.add(task);
        }

        // 4. 验证每个线程的任务是否按顺序出队
        for (int t = 0; t < THREAD_COUNT; t++) {
            List<TestTask> threadTasks = new ArrayList<>();
            for (TestTask task : dequeuedTasks) {
                if (task.getSequence() >= t * TASKS_PER_THREAD && 
                    task.getSequence() < (t + 1) * TASKS_PER_THREAD) {
                    threadTasks.add(task);
                }
            }

            // 验证每个线程的任务数量
            assertEquals(TASKS_PER_THREAD, threadTasks.size(), "线程" + t + "的任务数量不符");

            // 验证每个线程的任务是否按顺序排列
            for (int i = 0; i < threadTasks.size() - 1; i++) {
                assertTrue(
                    threadTasks.get(i).getSequence() < threadTasks.get(i + 1).getSequence(),
                    "线程" + t + "的任务顺序错误"
                );
            }
        }

        // 5. 验证队列为空
        assertTrue(queue.isEmpty(), "队列应该为空");

        // 关闭线程池
        executorService.shutdown();
    }

    /**
     * 测试高并发下的性能和正确性
     * 使用更多的线程和更短的时间间隔进行测试
     */
    @Test
    public void testHighConcurrencyFIFO() throws InterruptedException {
        final int THREAD_COUNT = 20;  // 使用20个线程
        final int TASKS_PER_THREAD = 500;  // 每个线程500个任务
        final int TOTAL = THREAD_COUNT * TASKS_PER_THREAD;
        
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        long startTime = System.currentTimeMillis();

        // 1. 并发入队
        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            executorService.submit(() -> {
                try {
                    for (int i = 0; i < TASKS_PER_THREAD; i++) {
                        TestTask task = new TestTask(threadId * TASKS_PER_THREAD + i, 
                            "HighConcurrency-" + threadId + "-" + i);
                        queue.enqueue(task, 0);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        // 等待入队完成
        latch.await(30, TimeUnit.SECONDS);
        long enqueueTime = System.currentTimeMillis() - startTime;

        // 2. 验证队列长度
        assertEquals(TOTAL, queue.size());

        // 3. 统计出队时间
        startTime = System.currentTimeMillis();
        List<TestTask> dequeued = new ArrayList<>();
        for (int i = 0; i < TOTAL; i++) {
            TestTask task = queue.dequeue();
            assertNotNull(task);
            dequeued.add(task);
        }
        long dequeueTime = System.currentTimeMillis() - startTime;

        // 4. 验证每个线程的任务顺序
        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadStart = t * TASKS_PER_THREAD;
            final int threadEnd = (t + 1) * TASKS_PER_THREAD;
            
            List<TestTask> threadTasks = new ArrayList<>();
            for (TestTask task : dequeued) {
                if (task.getSequence() >= threadStart && task.getSequence() < threadEnd) {
                    threadTasks.add(task);
                }
            }

            // 验证任务数量和顺序
            assertEquals(TASKS_PER_THREAD, threadTasks.size());
            for (int i = 0; i < threadTasks.size() - 1; i++) {
                assertTrue(
                    threadTasks.get(i).getSequence() < threadTasks.get(i + 1).getSequence()
                );
            }
        }

        // 5. 输出性能统计
        System.out.println("高并发测试结果：");
        System.out.println("总任务数: " + TOTAL);
        System.out.println("入队总时间: " + enqueueTime + "ms");
        System.out.println("入队平均时间: " + (enqueueTime / (double)TOTAL) + "ms/任务");
        System.out.println("出队总时间: " + dequeueTime + "ms");
        System.out.println("出队平均时间: " + (dequeueTime / (double)TOTAL) + "ms/任务");

        // 清理
        executorService.shutdown();
        assertTrue(queue.isEmpty());
    }
} 