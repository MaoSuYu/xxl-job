package symao.test;

import com.xuxueli.springbootpriorityqueue.model.Task;
import com.xuxueli.springbootpriorityqueue.service.TaskService;
import com.xxl.job.admin.XxlJobAdminApplication;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.Time;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SpringBootTest(classes = XxlJobAdminApplication.class)
public class PriorityQueueTest {

    @Autowired
    private TaskService  taskService;

    @Test
    @DisplayName("添加任务")
    public void test() {
        // 添加测试任务
        Task task1 = new Task("1", "symao1", "描述1");
        Task task2 = new Task("2", "symao2", "描述2");
        taskService.addTask(task1,10);
        taskService.addTask(task2,1);
        long taskCount = taskService.getTaskCount();
        System.err.println(taskCount);
    }

    @Test
    @DisplayName("查看任务区间")
    public void test2() {
        Set<Task> lowPriorityTasks = taskService.getLowPriorityTasks();
        for (Task task : lowPriorityTasks) {
            System.err.println(task);
        }
    }

    @Test
    @DisplayName("获取任务")
    public void test3() {
        Task nextTask = taskService.getNextTask();
        System.err.println(nextTask);
    }


    @Test
    @DisplayName("循环获取任务")
    public void test4() throws InterruptedException {
        // 使用 CountDownLatch 控制测试结束
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
        
        Thread thread = new Thread(() -> {
            try {
                // 设置一个循环次数限制，防止测试无限运行
                while (true) {
                    Task nextTask = taskService.getNextTask();
                    if (Objects.nonNull(nextTask)) {
                        System.err.println(nextTask);
                    } else {
                        System.out.println("等待...");
                    }
                    TimeUnit.SECONDS.sleep(2);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
        // 启动线程
        thread.start();
        latch.await();
    }

}
