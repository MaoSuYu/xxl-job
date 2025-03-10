package springbootpriorityqueue.queue;

import com.xuxueli.springbootpriorityqueue.queue.RedisSortedQueue;
import com.xuxueli.springbootpriorityqueue.queue.RedisSortedQueueFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class RedisSortedQueueTest {

    @Autowired
    private RedisSortedQueueFactory queueFactory;

    private RedisSortedQueue<TestItem> queue;

    @BeforeEach
    void setUp() {
        // 在每个测试方法前创建一个新的队列
        queue = queueFactory.getQueue("test-sorted-queue", TestItem.class);
        queue.clear(); // 确保队列为空
    }

    @AfterEach
    void tearDown() {
        // 在每个测试方法后清空队列
        queue.clear();
    }

    @Test
    void testEnqueueAndDequeue() {
        // 准备测试数据
        TestItem item1 = new TestItem(1L, "Item 1", LocalDateTime.now());
        TestItem item2 = new TestItem(2L, "Item 2", LocalDateTime.now());
        TestItem item3 = new TestItem(3L, "Item 3", LocalDateTime.now());

        // 按照分数从小到大入队
        assertTrue(queue.enqueue(item3, 30.0));
        assertTrue(queue.enqueue(item1, 10.0));
        assertTrue(queue.enqueue(item2, 20.0));

        // 验证队列长度
        assertEquals(3, queue.size());

        // 验证出队顺序（应该按照分数从小到大）
        TestItem dequeued1 = queue.dequeue();
        assertNotNull(dequeued1);
        assertEquals(1L, dequeued1.getId());

        TestItem dequeued2 = queue.dequeue();
        assertNotNull(dequeued2);
        assertEquals(2L, dequeued2.getId());

        TestItem dequeued3 = queue.dequeue();
        assertNotNull(dequeued3);
        assertEquals(3L, dequeued3.getId());

        // 验证队列为空
        assertNull(queue.dequeue());
        assertTrue(queue.isEmpty());
    }

    @Test
    void testPeek() {
        // 准备测试数据
        TestItem item1 = new TestItem(1L, "Item 1", LocalDateTime.now());
        TestItem item2 = new TestItem(2L, "Item 2", LocalDateTime.now());

        // 入队
        assertTrue(queue.enqueue(item2, 20.0));
        assertTrue(queue.enqueue(item1, 10.0));

        // 验证peek不会移除元素
        TestItem peeked = queue.peek();
        assertNotNull(peeked);
        assertEquals(1L, peeked.getId());
        assertEquals(2, queue.size());

        // 验证出队顺序
        TestItem dequeued1 = queue.dequeue();
        assertNotNull(dequeued1);
        assertEquals(1L, dequeued1.getId());

        TestItem dequeued2 = queue.dequeue();
        assertNotNull(dequeued2);
        assertEquals(2L, dequeued2.getId());
    }

    @Test
    void testGetItemsByScoreRange() {
        // 准备测试数据
        TestItem item1 = new TestItem(1L, "Item 1", LocalDateTime.now());
        TestItem item2 = new TestItem(2L, "Item 2", LocalDateTime.now());
        TestItem item3 = new TestItem(3L, "Item 3", LocalDateTime.now());
        TestItem item4 = new TestItem(4L, "Item 4", LocalDateTime.now());

        // 入队
        assertTrue(queue.enqueue(item1, 10.0));
        assertTrue(queue.enqueue(item2, 20.0));
        assertTrue(queue.enqueue(item3, 30.0));
        assertTrue(queue.enqueue(item4, 40.0));

        // 验证按分数范围获取元素
        Set<TestItem> items = queue.getItemsByScoreRange(15.0, 35.0);
        assertEquals(2, items.size());
        assertTrue(items.stream().anyMatch(item -> item.getId() == 2L));
        assertTrue(items.stream().anyMatch(item -> item.getId() == 3L));
    }

    @Test
    void testUpdateScore() {
        // 准备测试数据
        TestItem item1 = new TestItem(1L, "Item 1", LocalDateTime.now());
        TestItem item2 = new TestItem(2L, "Item 2", LocalDateTime.now());

        // 入队
        assertTrue(queue.enqueue(item1, 20.0));
        assertTrue(queue.enqueue(item2, 30.0));

        // 更新分数
        assertTrue(queue.updateScore(item1, 40.0));

        // 验证出队顺序
        TestItem dequeued1 = queue.dequeue();
        assertNotNull(dequeued1);
        assertEquals(2L, dequeued1.getId());

        TestItem dequeued2 = queue.dequeue();
        assertNotNull(dequeued2);
        assertEquals(1L, dequeued2.getId());
    }

    @Test
    void testRemove() {
        // 准备测试数据
        TestItem item1 = new TestItem(1L, "Item 1", LocalDateTime.now());
        TestItem item2 = new TestItem(2L, "Item 2", LocalDateTime.now());

        // 入队
        assertTrue(queue.enqueue(item1, 10.0));
        assertTrue(queue.enqueue(item2, 20.0));

        // 移除特定元素
        assertTrue(queue.remove(item1));
        assertEquals(1, queue.size());

        // 验证剩余元素
        TestItem dequeued = queue.dequeue();
        assertNotNull(dequeued);
        assertEquals(2L, dequeued.getId());
    }

    @Test
    void testConcurrentEnqueue() throws InterruptedException {
        int threadCount = 10;
        int itemsPerThread = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    for (int j = 0; j < itemsPerThread; j++) {
                        int id = threadId * itemsPerThread + j;
                        TestItem item = new TestItem((long) id, "Item " + id, LocalDateTime.now());
                        // 随机分数
                        double score = Math.random() * 1000;
                        queue.enqueue(item, score);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executorService.shutdown();

        assertEquals(threadCount * itemsPerThread, queue.size());
    }

    // 测试用的数据项类
    public static class TestItem {
        private Long id;
        private String name;
        private LocalDateTime createdAt;

        // 无参构造函数，用于反序列化
        public TestItem() {
        }

        public TestItem(Long id, String name, LocalDateTime createdAt) {
            this.id = id;
            this.name = name;
            this.createdAt = createdAt;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public LocalDateTime getCreatedAt() {
            return createdAt;
        }

        public void setCreatedAt(LocalDateTime createdAt) {
            this.createdAt = createdAt;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestItem testItem = (TestItem) o;
            return id.equals(testItem.id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }
    }
} 