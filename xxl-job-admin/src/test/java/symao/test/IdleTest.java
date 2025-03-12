package symao.test;

import com.xxl.job.admin.XxlJobAdminApplication;
import com.xxl.job.admin.core.route.strategy.IdleThreadBasedTaskAllocator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = XxlJobAdminApplication.class)
public class IdleTest {

    @Test
    public void test() throws InterruptedException {
        String s = IdleThreadBasedTaskAllocator.choiceIP("vip-executor");
        System.out.println(s);
    }

}
