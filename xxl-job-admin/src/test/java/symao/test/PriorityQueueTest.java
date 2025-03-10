package symao.test;

import com.xuxueli.springbootpriorityqueue.model.Task;
import com.xuxueli.springbootpriorityqueue.service.TaskService;
import com.xxl.job.admin.XxlJobAdminApplication;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Set;

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

}
