package com.xxl.job.core.thread;

import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.context.XxlJobContext;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.enums.RegistryConfig;
import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.log.XxlJobFileAppender;
import com.xxl.job.core.util.FileUtil;
import com.xxl.job.core.util.JdkSerializeTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 任务回调线程
 * 
 * 该类负责将执行器执行完任务后的结果回调给调度中心。
 * 主要包含两个线程：
 * 1. 正常回调线程：从回调队列中获取回调参数并执行回调
 * 2. 重试回调线程：定期重试执行失败的回调
 * 
 * Created by xuxueli on 16/7/22.
 */
public class TriggerCallbackThread {
    /**
     * 日志记录器
     */
    private static Logger logger = LoggerFactory.getLogger(TriggerCallbackThread.class);

    /**
     * 单例实例
     */
    private static TriggerCallbackThread instance = new TriggerCallbackThread();
    
    /**
     * 获取单例实例
     * 
     * @return TriggerCallbackThread实例
     */
    public static TriggerCallbackThread getInstance(){
        return instance;
    }

    /**
     * 回调参数队列
     * 用于存储待回调的任务执行结果
     */
    private LinkedBlockingQueue<HandleCallbackParam> callBackQueue = new LinkedBlockingQueue<HandleCallbackParam>();
    
    /**
     * 将回调参数添加到回调队列中
     * 
     * @param callback 回调参数
     */
    public static void pushCallBack(HandleCallbackParam callback){
        getInstance().callBackQueue.add(callback);
        logger.debug(">>>>>>>>>>> xxl-job, push callback request, logId:{}", callback.getLogId());
    }

    /**
     * 正常回调线程
     */
    private Thread triggerCallbackThread;
    
    /**
     * 重试回调线程
     */
    private Thread triggerRetryCallbackThread;
    
    /**
     * 线程停止标志
     */
    private volatile boolean toStop = false;
    
    /**
     * 启动回调线程
     * 包括正常回调线程和重试回调线程
     */
    public void start() {

        // 验证管理员地址列表是否配置
        if (XxlJobExecutor.getAdminBizList() == null) {
            logger.warn(">>>>>>>>>>> xxl-job, executor callback config fail, adminAddresses is null.");
            return;
        }

        // 初始化并启动正常回调线程
        triggerCallbackThread = new Thread(new Runnable() {

            @Override
            public void run() {

                // 正常回调处理逻辑
                while(!toStop){
                    try {
                        // 从队列中获取回调参数，如果队列为空则阻塞等待
                        HandleCallbackParam callback = getInstance().callBackQueue.take();
                        if (callback != null) {

                            // 批量获取回调参数列表
                            List<HandleCallbackParam> callbackParamList = new ArrayList<HandleCallbackParam>();
                            int drainToNum = getInstance().callBackQueue.drainTo(callbackParamList);
                            callbackParamList.add(callback);

                            // 执行回调，如果出错会进行重试
                            if (callbackParamList!=null && callbackParamList.size()>0) {
                                doCallback(callbackParamList);
                            }
                        }
                    } catch (Throwable e) {
                        if (!toStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }

                // 线程停止前处理剩余的回调请求
                try {
                    List<HandleCallbackParam> callbackParamList = new ArrayList<HandleCallbackParam>();
                    int drainToNum = getInstance().callBackQueue.drainTo(callbackParamList);
                    if (callbackParamList!=null && callbackParamList.size()>0) {
                        doCallback(callbackParamList);
                    }
                } catch (Throwable e) {
                    if (!toStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, executor callback thread destroy.");

            }
        });
        triggerCallbackThread.setDaemon(true);
        triggerCallbackThread.setName("xxl-job, executor TriggerCallbackThread");
        triggerCallbackThread.start();


        // 初始化并启动重试回调线程
        triggerRetryCallbackThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while(!toStop){
                    try {
                        // 重试执行失败的回调
                        retryFailCallbackFile();
                    } catch (Throwable e) {
                        if (!toStop) {
                            logger.error(e.getMessage(), e);
                        }

                    }
                    try {
                        // 按照注册配置的超时时间间隔进行重试
                        TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
                    } catch (Throwable e) {
                        if (!toStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, executor retry callback thread destroy.");
            }
        });
        triggerRetryCallbackThread.setDaemon(true);
        triggerRetryCallbackThread.start();

    }
    
    /**
     * 停止回调线程
     * 包括正常回调线程和重试回调线程
     */
    public void toStop(){
        toStop = true;
        // 停止正常回调线程，中断并等待线程结束
        if (triggerCallbackThread != null) {    // support empty admin address
            triggerCallbackThread.interrupt();
            try {
                triggerCallbackThread.join();
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);
            }
        }

        // 停止重试回调线程，中断并等待线程结束
        if (triggerRetryCallbackThread != null) {
            triggerRetryCallbackThread.interrupt();
            try {
                triggerRetryCallbackThread.join();
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);
            }
        }

    }

    /**
     * 执行回调，如果出错会记录到失败文件中以便后续重试
     * 
     * @param callbackParamList 回调参数列表
     */
    private void doCallback(List<HandleCallbackParam> callbackParamList){
        boolean callbackRet = false;
        // 遍历所有管理员业务实例进行回调，任一成功则停止
        for (AdminBiz adminBiz: XxlJobExecutor.getAdminBizList()) {
            try {
                // 调用管理员业务接口执行回调
                ReturnT<String> callbackResult = adminBiz.callback(callbackParamList);
                if (callbackResult!=null && ReturnT.SUCCESS_CODE == callbackResult.getCode()) {
                    // 回调成功，记录日志
                    callbackLog(callbackParamList, "<br>----------- xxl-job job callback finish.");
                    callbackRet = true;
                    break;
                } else {
                    // 回调失败，记录失败结果
                    callbackLog(callbackParamList, "<br>----------- xxl-job job callback fail, callbackResult:" + callbackResult);
                }
            } catch (Throwable e) {
                // 回调异常，记录异常信息
                callbackLog(callbackParamList, "<br>----------- xxl-job job callback error, errorMsg:" + e.getMessage());
            }
        }
        // 如果所有回调都失败，将回调参数写入失败文件
        if (!callbackRet) {
            appendFailCallbackFile(callbackParamList);
        }
    }

    /**
     * 记录回调日志
     * 
     * @param callbackParamList 回调参数列表
     * @param logContent 日志内容
     */
    private void callbackLog(List<HandleCallbackParam> callbackParamList, String logContent){
        for (HandleCallbackParam callbackParam: callbackParamList) {
            String logFileName = XxlJobFileAppender.makeLogFileName(new Date(callbackParam.getLogDateTim()), callbackParam.getLogId());
            XxlJobContext.setXxlJobContext(new XxlJobContext(
                    -1,
                    null,
                    logFileName,
                    -1,
                    -1));
            XxlJobHelper.log(logContent);
        }
    }


    // ---------------------- 失败回调文件处理 ----------------------

    /**
     * 失败回调文件路径
     */
    private static String failCallbackFilePath = XxlJobFileAppender.getLogPath().concat(File.separator).concat("callbacklog").concat(File.separator);
    
    /**
     * 失败回调文件名模板
     */
    private static String failCallbackFileName = failCallbackFilePath.concat("xxl-job-callback-{x}").concat(".log");

    /**
     * 将失败的回调参数追加到失败文件中
     * 
     * @param callbackParamList 回调参数列表
     */
    private void appendFailCallbackFile(List<HandleCallbackParam> callbackParamList){
        // 验证参数
        if (callbackParamList==null || callbackParamList.size()==0) {
            return;
        }

        // 序列化回调参数列表
        byte[] callbackParamList_bytes = JdkSerializeTool.serialize(callbackParamList);

        // 创建失败回调文件，确保文件名唯一
        File callbackLogFile = new File(failCallbackFileName.replace("{x}", String.valueOf(System.currentTimeMillis())));
        if (callbackLogFile.exists()) {
            for (int i = 0; i < 100; i++) {
                callbackLogFile = new File(failCallbackFileName.replace("{x}", String.valueOf(System.currentTimeMillis()).concat("-").concat(String.valueOf(i)) ));
                if (!callbackLogFile.exists()) {
                    break;
                }
            }
        }
        // 写入文件内容
        FileUtil.writeFileContent(callbackLogFile, callbackParamList_bytes);
    }

    /**
     * 重试执行失败的回调
     * 读取失败回调文件中的回调参数并重新执行回调
     */
    private void retryFailCallbackFile(){

        // 验证失败回调文件路径
        File callbackLogPath = new File(failCallbackFilePath);
        if (!callbackLogPath.exists()) {
            return;
        }
        if (callbackLogPath.isFile()) {
            callbackLogPath.delete();
        }
        if (!(callbackLogPath.isDirectory() && callbackLogPath.list()!=null && callbackLogPath.list().length>0)) {
            return;
        }

        // 加载并清理文件，重试回调
        for (File callbaclLogFile: callbackLogPath.listFiles()) {
            // 读取文件内容
            byte[] callbackParamList_bytes = FileUtil.readFileContent(callbaclLogFile);

            // 避免空文件
            if(callbackParamList_bytes == null || callbackParamList_bytes.length < 1){
                callbaclLogFile.delete();
                continue;
            }

            // 反序列化回调参数列表
            List<HandleCallbackParam> callbackParamList = (List<HandleCallbackParam>) JdkSerializeTool.deserialize(callbackParamList_bytes, List.class);

            // 删除文件并执行回调
            callbaclLogFile.delete();
            doCallback(callbackParamList);
        }

    }

}
