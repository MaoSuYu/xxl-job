package com.xxl.job.admin.core.thread;

import cn.hutool.extra.spring.SpringUtil;
import com.xuxueli.springbootpriorityqueue.model.SortedTask;
import com.xuxueli.springbootpriorityqueue.model.Task;
import com.xuxueli.springbootpriorityqueue.service.SortedTaskService;
import com.xuxueli.springbootpriorityqueue.service.TaskService;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * 作者: Mr.Z
 * 时间: 2025-03-11 00:00
 */
public class TaskQueueHelper {
    private static Logger logger = LoggerFactory.getLogger(TaskQueueHelper.class);

    // 单例模式
    private static TaskQueueHelper instance = new TaskQueueHelper();
    public static TaskQueueHelper getInstance() {
        return instance;
    }

    // 执行线程
    private Thread taskThread;
    private Thread sortTaskThread;

    // 停止标志
    private volatile boolean taskThreadToStop = false;
    private volatile boolean sortTaskThreadToStop = false;

    /**
     * 启动任务队列监听器
     */
    public void start() {
        // 启动普通任务执行线程
        taskThread = new Thread(new Runnable() {
            @Override
            public void run() {
                SortedTaskService taskService = (SortedTaskService) SpringUtil.getBean("sortedTaskService");
                while (!taskThreadToStop) {
                    try {
                        // 获取并执行任务
                        SortedTask nextTask = taskService.getNextTask();
                        if (nextTask != null) {
                            try {
                                String id = nextTask.getId();
                                JobTriggerPoolHelper.triggerSharding(Long.parseLong(id), TriggerTypeEnum.MANUAL, -1, null, null, null);
                            } catch (Exception e) {
                                logger.error(">>>>>>>>>>> task execute error: {}", e.getMessage(), e);
                            }
                        } else {
                            // 队列为空时等待
                            TimeUnit.MILLISECONDS.sleep(100);
                        }
                    } catch (InterruptedException e) {
                        if (!taskThreadToStop) {
                            logger.error(">>>>>>>>>>> task thread interrupted: {}", e.getMessage(), e);
                        }
                    }
                }
                logger.info(">>>>>>>>>>> task thread stop");
            }
        });
        taskThread.setDaemon(true);
        taskThread.setName("xxl-job, admin TaskQueueHelper#taskThread");
        taskThread.start();

        // 启动排序任务执行线程
        sortTaskThread = new Thread(new Runnable() {
            @Override
            public void run() {
                TaskService taskService = (TaskService) SpringUtil.getBean("taskService");
                while (!sortTaskThreadToStop) {
                    try {
                        // 获取并执行排序任务
                        Task nextTask = taskService.getNextTask();
                        if (nextTask != null) {
                            try {
                                String id = nextTask.getId();
                                System.out.println("id = " + id);
                                JobTriggerPoolHelper.triggerSharding(Long.parseLong(id), TriggerTypeEnum.MANUAL, -1, null, null, null);
                            } catch (Exception e) {
                                logger.error(">>>>>>>>>>> sort task execute error: {}", e.getMessage(), e);
                            }
                        } else {
                            // 队列为空时等待
                            TimeUnit.MILLISECONDS.sleep(100);
                        }
                    } catch (InterruptedException e) {
                        if (!sortTaskThreadToStop) {
                            logger.error(">>>>>>>>>>> sort task thread interrupted: {}", e.getMessage(), e);
                        }
                    }
                }
                logger.info(">>>>>>>>>>> sort task thread stop");
            }
        });
        sortTaskThread.setDaemon(true);
        sortTaskThread.setName("xxl-job, admin TaskQueueHelper#sortTaskThread");
        sortTaskThread.start();
    }

    /**
     * 停止任务队列监听器
     */
    public void toStop() {
        // 停止普通任务线程
        taskThreadToStop = true;
        if (taskThread != null && taskThread.getState() != Thread.State.TERMINATED) {
            taskThread.interrupt();
            try {
                taskThread.join();
            } catch (InterruptedException e) {
                logger.error(">>>>>>>>>>> task thread stop error: {}", e.getMessage(), e);
            }
        }

        // 停止排序任务线程
        sortTaskThreadToStop = true;
        if (sortTaskThread != null && sortTaskThread.getState() != Thread.State.TERMINATED) {
            sortTaskThread.interrupt();
            try {
                sortTaskThread.join();
            } catch (InterruptedException e) {
                logger.error(">>>>>>>>>>> sort task thread stop error: {}", e.getMessage(), e);
            }
        }

        logger.info(">>>>>>>>>>> TaskQueueHelper stop");
    }
}
