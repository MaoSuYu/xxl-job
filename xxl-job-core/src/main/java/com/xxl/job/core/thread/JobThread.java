package com.xxl.job.core.thread;

import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import com.xxl.job.core.context.JobThreadContext;
import com.xxl.job.core.context.XxlJobContext;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.enums.ThreadConstant;
import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.log.XxlJobFileAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;


/**
 * 任务处理线程
 * 每个JobThread实例负责执行一个具体的任务
 * 每个任务ID对应一个JobThread实例
 * 
 * @author xuxueli 2016-1-16 19:52:47
 */
public class JobThread extends Thread{
	// 日志记录器
	private static Logger logger = LoggerFactory.getLogger(JobThread.class);

	// 任务ID，用于标识当前线程负责的任务
	private int jobId;
	// 任务处理器，用于实际执行任务逻辑
	private IJobHandler handler;
	// 触发器队列，存放待执行的任务触发参数
	private LinkedBlockingQueue<TriggerParam> triggerQueue;
	// 已触发任务的日志ID集合，用于避免重复触发同一任务
	private Set<Long> triggerLogIdSet;		// avoid repeat trigger for the same TRIGGER_LOG_ID

	// 线程停止标志，volatile确保多线程间的可见性
	private volatile boolean toStop = false;
	// 停止原因，记录线程为什么被停止
	private String stopReason;

    // 是否正在执行任务的标志
    private boolean running = false;    // if running job
    // 空闲次数计数，用于检测长时间空闲的线程
	private int idleTimes = 0;			// idle times

    /**
     * 构造方法
     * 初始化JobThread的相关参数
     * 
     * @param jobId   任务ID
     * @param handler 任务处理器
     */
	public JobThread(int jobId, IJobHandler handler) {
		this.jobId = jobId;
		this.handler = handler;
		// 初始化触发器队列
		this.triggerQueue = new LinkedBlockingQueue<TriggerParam>();
		// 使用线程安全的Set存储触发日志ID
		this.triggerLogIdSet = Collections.synchronizedSet(new HashSet<Long>());

		// 设置线程名称，便于调试和监控
		this.setName("xxl-job, JobThread-"+jobId+"-"+System.currentTimeMillis());
		JobThreadContext.setJobThreadContextMap(jobId, this);
	}
	
	/**
     * 获取当前线程使用的任务处理器
     * 
     * @return 任务处理器实例
     */
	public IJobHandler getHandler() {
		return handler;
	}

    /**
     * 将新的触发参数加入到任务队列中
     * 用于接收调度中心发送的任务执行请求
     *
     * @param triggerParam 任务触发参数
     * @return 操作结果
     */
	public ReturnT<String> pushTriggerQueue(TriggerParam triggerParam) {
		// 避免重复触发同一任务
		if (triggerLogIdSet.contains(triggerParam.getLogId())) {
			logger.info(">>>>>>>>>>> repeate trigger job, logId:{}", triggerParam.getLogId());
			return new ReturnT<String>(ReturnT.FAIL_CODE, "repeate trigger job, logId:" + triggerParam.getLogId());
		}

		// 将任务日志ID添加到集合中，标记为已触发
		triggerLogIdSet.add(triggerParam.getLogId());
		// 将触发参数添加到队列中，等待执行
		triggerQueue.add(triggerParam);
        return ReturnT.SUCCESS;
	}

    /**
     * 停止任务线程
     * 通过共享变量方式安全停止线程，而不是直接中断
     *
     * @param stopReason 停止原因
     */
	public void toStop(String stopReason) {
		/**
		 * Thread.interrupt只支持终止线程的阻塞状态(wait、join、sleep)，
		 * 在阻塞出抛出InterruptedException异常,但是并不会终止运行的线程本身；
		 * 所以需要注意，此处彻底销毁本线程，需要通过共享变量方式；
		 */
		this.toStop = true;
		this.stopReason = stopReason;
	}

    /**
     * 检查任务是否正在运行或队列中有等待执行的任务
     * 用于外部判断此线程是否可以安全销毁
     * 
     * @return 如果正在运行或队列不为空，返回true；否则返回false
     */
    public boolean isRunningOrHasQueue() {
        return running || triggerQueue.size()>0;
    }

    /**
     * 从等待队列中移除指定任务
     * 
     * @param jobId 任务ID
     * @return 是否成功移除
     */
    public boolean removeFromQueue(int jobId) {
        boolean removed = false;
        // 遍历队列，移除匹配的任务
        for (TriggerParam param : triggerQueue) {
            if (param.getJobId() == jobId) {
                removed = triggerQueue.remove(param);
                if (removed) {
                    logger.info("任务从等待队列移除成功 [任务ID:{}]", jobId);
                }
                break;
            }
        }
        return removed;
    }

    /**
     * 获取等待执行的触发任务队列大小
     * 
     * @return 触发队列大小
     */
    public int getPendingTriggerQueueSize() {
        return triggerQueue.size();
    }

    /**
     * 线程执行入口
     * 实现了任务的执行、超时控制和回调处理等核心逻辑
     */
    @Override
	public void run() {

    	// 初始化任务处理器
    	try {
			handler.init();
		} catch (Throwable e) {
    		logger.error(e.getMessage(), e);
		}

		// 执行任务循环
		while(!toStop){
			running = false;
			idleTimes++;

            TriggerParam triggerParam = null;
            try {
				// 从队列中获取任务触发参数，设置超时以便检查停止信号
				// 使用poll而不是take，以便定期检查toStop标志
				triggerParam = triggerQueue.poll(1L, TimeUnit.SECONDS);
				if (triggerParam!=null) {
					// 设置执行状态和计数器
					running = true;
					idleTimes = 0;
					// 从已触发集合中移除当前任务日志ID
					triggerLogIdSet.remove(triggerParam.getLogId());

					// 创建日志文件名，格式如 "logPath/yyyy-MM-dd/9999.log"
					String logFileName = XxlJobFileAppender.makeLogFileName(new Date(triggerParam.getLogDateTime()), triggerParam.getLogId());
					// 创建任务上下文，包含任务执行所需的各种参数和环境信息
					XxlJobContext xxlJobContext = new XxlJobContext(
							triggerParam.getJobId(),
							triggerParam.getExecutorParams(),
							logFileName,
							triggerParam.getBroadcastIndex(),
							triggerParam.getBroadcastTotal());

					// 初始化任务上下文，设置线程本地变量
					XxlJobContext.setXxlJobContext(xxlJobContext);

					// 记录任务开始执行的日志
					XxlJobHelper.log("<br>----------- xxl-job job execute start -----------<br>----------- Param:" + xxlJobContext.getJobParam());

					// 判断是否设置了任务超时时间
					if (triggerParam.getExecutorTimeout() > 0) {
						// 使用Future实现超时控制
						Thread futureThread = null;
						try {
							// 创建FutureTask封装任务执行
							FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
								@Override
								public Boolean call() throws Exception {
									// 初始化任务上下文
									XxlJobContext.setXxlJobContext(xxlJobContext);
									// 执行任务
									handler.execute();
									return true;
								}
							});
							// 创建并启动执行线程
							futureThread = new Thread(futureTask);
							futureThread.start();

							// 等待任务执行完成，设置超时时间
							Boolean tempResult = futureTask.get(triggerParam.getExecutorTimeout(), TimeUnit.SECONDS);
						} catch (TimeoutException e) {
							// 记录任务执行超时日志
							XxlJobHelper.log("<br>----------- xxl-job job execute timeout");
							XxlJobHelper.log(e);

							// 处理超时结果
							XxlJobHelper.handleTimeout("job execute timeout ");
						} finally {
							// 中断执行线程
							futureThread.interrupt();
						}
					} else {
						// 无超时限制，直接执行任务
						handler.execute();
					}

					// 验证任务执行结果
					if (XxlJobContext.getXxlJobContext().getHandleCode() <= 0) {
						// 处理丢失执行结果的情况
						XxlJobHelper.handleFail("job handle result lost.");
					} else {
						// 处理消息过长的情况，截断超过50000字符的消息
						String tempHandleMsg = XxlJobContext.getXxlJobContext().getHandleMsg();
						tempHandleMsg = (tempHandleMsg!=null&&tempHandleMsg.length()>50000)
								?tempHandleMsg.substring(0, 50000).concat("...")
								:tempHandleMsg;
						XxlJobContext.getXxlJobContext().setHandleMsg(tempHandleMsg);
					}
					
					// 记录任务执行完成的日志
					XxlJobHelper.log("<br>----------- xxl-job job execute end(finish) -----------<br>----------- Result: handleCode="
							+ XxlJobContext.getXxlJobContext().getHandleCode()
							+ ", handleMsg = "
							+ XxlJobContext.getXxlJobContext().getHandleMsg()
					);

				} else {
					// 这里修改源码，当任务结束且队列为空立马关闭线程
					if(triggerQueue.size() == 0) {	// avoid concurrent trigger causes jobId-lost
						XxlJobExecutor.removeJobThread(jobId, "长时间未获取到任务，终止工作线程！");
					}
					// 任务队列为空，检查空闲时间
//					if (idleTimes > ThreadConstant.ATTEMPTS) {
//						// 空闲超过30次且队列为空，移除任务线程
//						if(triggerQueue.size() == 0) {	// avoid concurrent trigger causes jobId-lost
//							XxlJobExecutor.removeJobThread(jobId, "长时间未获取到任务，终止工作线程！");
//						}
//					}
				}
			} catch (Throwable e) {
				// 检查是否是由于停止信号导致的异常
				if (toStop) {
					XxlJobHelper.log("<br>----------- JobThread toStop, stopReason:" + stopReason);
				}

				// 处理异常结果
				StringWriter stringWriter = new StringWriter();
				e.printStackTrace(new PrintWriter(stringWriter));
				String errorMsg = stringWriter.toString();

				// 标记任务执行失败
				XxlJobHelper.handleFail(errorMsg);

				// 记录异常日志
				XxlJobHelper.log("<br>----------- JobThread Exception:" + errorMsg + "<br>----------- xxl-job job execute end(error) -----------");
			} finally {
                // 任务执行完毕后的回调处理
                if(triggerParam != null) {
                    // 创建回调参数
                    if (!toStop) {
                        // 正常情况下的回调
                        // 创建HandleCallbackParam对象，封装任务执行结果信息
                        TriggerCallbackThread.pushCallBack(new HandleCallbackParam(
                        		triggerParam.getLogId(),      // 日志ID，用于标识任务实例
								triggerParam.getLogDateTime(),// 日志时间
								XxlJobContext.getXxlJobContext().getHandleCode(), // 处理结果代码
								XxlJobContext.getXxlJobContext().getHandleMsg()   // 处理结果消息
						));
                    } else {
                        // 任务被强制停止的情况
                        TriggerCallbackThread.pushCallBack(new HandleCallbackParam(
                        		triggerParam.getLogId(),      // 日志ID
								triggerParam.getLogDateTime(),// 日志时间
								XxlJobContext.HANDLE_CODE_FAIL, // 使用固定的失败状态码
								stopReason + " [job running, killed]" // 停止原因
						));
                    }
                }
            }
        }

		// 处理队列中待执行的任务，将其标记为被终止
		while(triggerQueue !=null && triggerQueue.size()>0){
			TriggerParam triggerParam = triggerQueue.poll();
			if (triggerParam!=null) {
				// 对于未执行的任务，报告被终止状态
				TriggerCallbackThread.pushCallBack(new HandleCallbackParam(
						triggerParam.getLogId(),
						triggerParam.getLogDateTime(),
						XxlJobContext.HANDLE_CODE_FAIL,
						stopReason + " [job not executed, in the job queue, killed.]")
				);
			}
		}

		// 销毁任务处理器
		try {
			handler.destroy();
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
		}

		// 记录线程停止日志
		logger.info("工作线程已停止, thread name:{}", Thread.currentThread());
	}
}
