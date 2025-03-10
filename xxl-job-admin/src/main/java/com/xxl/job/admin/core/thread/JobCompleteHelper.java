package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.complete.XxlJobCompleter;
import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobLog;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

/**
 * 任务完成助手
 * 
 * 该类负责处理任务执行完成的相关操作，主要包括：
 * 1. 接收并处理执行器的任务回调信息
 * 2. 监控并处理丢失的任务（长时间处于运行状态但执行器已下线的任务）
 * 3. 管理回调线程池和监控线程
 *
 * @author xuxueli 2015-9-1 18:05:56
 */
public class JobCompleteHelper {
	/**
	 * 日志记录器
	 */
	private static Logger logger = LoggerFactory.getLogger(JobCompleteHelper.class);
	
	/**
	 * 单例实例
	 */
	private static JobCompleteHelper instance = new JobCompleteHelper();
	
	/**
	 * 获取单例实例
	 * 
	 * @return JobCompleteHelper实例
	 */
	public static JobCompleteHelper getInstance(){
		return instance;
	}

	// ---------------------- monitor ----------------------

	/**
	 * 回调处理线程池
	 * 用于异步处理执行器的任务回调
	 */
	private ThreadPoolExecutor callbackThreadPool = null;
	
	/**
	 * 任务丢失监控线程
	 * 用于定期检查并处理丢失的任务
	 */
	private Thread monitorThread;
	
	/**
	 * 线程停止标志
	 */
	private volatile boolean toStop = false;
	
	/**
	 * 启动任务完成助手
	 * 
	 * 初始化并启动回调线程池和任务丢失监控线程
	 */
	public void start(){

		// 初始化回调线程池
		callbackThreadPool = new ThreadPoolExecutor(
				2,                                  // 核心线程数：2
				20,                                 // 最大线程数：20
				30L,                                // 空闲线程存活时间：30秒
				TimeUnit.SECONDS,                   // 时间单位：秒
				new LinkedBlockingQueue<Runnable>(3000),  // 工作队列：容量为3000的阻塞队列
				new ThreadFactory() {               // 线程工厂：自定义线程名称
					@Override
					public Thread newThread(Runnable r) {
						return new Thread(r, "xxl-job, admin JobLosedMonitorHelper-callbackThreadPool-" + r.hashCode());
					}
				},
				new RejectedExecutionHandler() {    // 拒绝策略：队列满时直接在调用线程中执行任务
					@Override
					public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
						r.run();
						logger.warn(">>>>>>>>>>> xxl-job, callback too fast, match threadpool rejected handler(run now).");
					}
				});


		// 初始化并启动任务丢失监控线程
		monitorThread = new Thread(new Runnable() {

			@Override
			public void run() {

				// 等待JobTriggerPoolHelper初始化完成
				try {
					TimeUnit.MILLISECONDS.sleep(50);
				} catch (Throwable e) {
					if (!toStop) {
						logger.error(e.getMessage(), e);
					}
				}

				// 监控主循环
				while (!toStop) {
					try {
						// 任务结果丢失处理：调度记录停留在 "运行中" 状态超过10min，且对应执行器心跳注册失败不在线，则将本地调度主动标记失败；
						// 计算10分钟前的时间点
						Date losedTime = DateUtil.addMinutes(new Date(), -10);
						// 查询所有丢失的任务ID（运行中状态超过10分钟且执行器不在线的任务）
						List<Long> losedJobIds  = XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().findLostJobIds(losedTime);

						// 处理所有丢失的任务
						if (losedJobIds!=null && losedJobIds.size()>0) {
							for (Long logId: losedJobIds) {
								// 创建任务日志对象
								XxlJobLog jobLog = new XxlJobLog();
								jobLog.setId(logId);

								// 设置处理时间为当前时间
								jobLog.setHandleTime(new Date());
								// 设置处理结果为失败
								jobLog.setHandleCode(ReturnT.FAIL_CODE);
								// 设置处理消息，使用国际化资源获取文本
								jobLog.setHandleMsg( I18nUtil.getString("joblog_lost_fail") );

								// 更新任务处理信息并完成任务
								XxlJobCompleter.updateHandleInfoAndFinish(jobLog);
							}

						}
					} catch (Throwable e) {
						if (!toStop) {
							logger.error(">>>>>>>>>>> xxl-job, job fail monitor thread error:{}", e);
						}
					}

                    try {
                        // 休眠60秒后继续下一轮检查
                        TimeUnit.SECONDS.sleep(60);
                    } catch (Throwable e) {
                        if (!toStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }

                }

				logger.info(">>>>>>>>>>> xxl-job, JobLosedMonitorHelper stop");

			}
		});
		// 设置为守护线程
		monitorThread.setDaemon(true);
		// 设置线程名称
		monitorThread.setName("xxl-job, admin JobLosedMonitorHelper");
		// 启动监控线程
		monitorThread.start();
	}

	/**
	 * 停止任务完成助手
	 * 
	 * 停止回调线程池和任务丢失监控线程
	 */
	public void toStop(){
		// 设置停止标志
		toStop = true;

		// 立即停止回调线程池
		callbackThreadPool.shutdownNow();

		// 停止监控线程（中断并等待）
		monitorThread.interrupt();
		try {
			// 等待监控线程结束
			monitorThread.join();
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
		}
	}


	// ---------------------- helper ----------------------

	/**
	 * 批量处理任务回调
	 * 
	 * 将回调参数列表提交到回调线程池中异步处理
	 * 
	 * @param callbackParamList 回调参数列表
	 * @return 处理结果
	 */
	public ReturnT<String> callback(List<HandleCallbackParam> callbackParamList) {
		// 提交到回调线程池中异步处理
		callbackThreadPool.execute(new Runnable() {
			@Override
			public void run() {
				// 遍历处理每个回调参数
				for (HandleCallbackParam handleCallbackParam: callbackParamList) {
					// 调用单个回调处理方法
					ReturnT<String> callbackResult = callback(handleCallbackParam);
					// 记录处理结果日志
					logger.debug(">>>>>>>>> JobApiController.callback {}, handleCallbackParam={}, callbackResult={}",
							(callbackResult.getCode()== ReturnT.SUCCESS_CODE?"success":"fail"), handleCallbackParam, callbackResult);
				}
			}
		});

		// 立即返回成功，不等待异步处理完成
		return ReturnT.SUCCESS;
	}

	/**
	 * 处理单个任务回调
	 * 
	 * 根据回调参数更新任务日志信息并完成任务
	 * 
	 * @param handleCallbackParam 回调参数
	 * @return 处理结果
	 */
	private ReturnT<String> callback(HandleCallbackParam handleCallbackParam) {
		// 验证任务日志是否存在
		XxlJobLog log = XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().load(handleCallbackParam.getLogId());
		if (log == null) {
			// 任务日志不存在，返回失败
			return new ReturnT<String>(ReturnT.FAIL_CODE, "log item not found.");
		}
		if (log.getHandleCode() > 0) {
			// 任务已经被处理过，避免重复回调，返回失败
			return new ReturnT<String>(ReturnT.FAIL_CODE, "log repeate callback.");     // avoid repeat callback, trigger child job etc
		}

		// 处理回调消息
		StringBuffer handleMsg = new StringBuffer();
		// 如果日志中已有处理消息，则先添加原有消息
		if (log.getHandleMsg()!=null) {
			handleMsg.append(log.getHandleMsg()).append("<br>");
		}
		// 添加回调参数中的处理消息
		if (handleCallbackParam.getHandleMsg() != null) {
			handleMsg.append(handleCallbackParam.getHandleMsg());
		}

		// 更新任务日志信息
		log.setHandleTime(new Date());  // 设置处理时间为当前时间
		log.setHandleCode(handleCallbackParam.getHandleCode());  // 设置处理结果码
		log.setHandleMsg(handleMsg.toString());  // 设置处理消息
		// 调用任务完成处理器更新处理信息并完成任务
		XxlJobCompleter.updateHandleInfoAndFinish(log);

		// 返回处理成功
		return ReturnT.SUCCESS;
	}



}
