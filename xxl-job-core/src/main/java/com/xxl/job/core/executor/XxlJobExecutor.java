package com.xxl.job.core.executor;

import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.client.AdminBizClient;
import com.xxl.job.core.context.JobThreadContext;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.handler.impl.MethodJobHandler;
import com.xxl.job.core.log.XxlJobFileAppender;
import com.xxl.job.core.server.EmbedServer;
import com.xxl.job.core.thread.JobLogFileCleanThread;
import com.xxl.job.core.thread.JobThread;
import com.xxl.job.core.thread.JobThreadMonitorHelper;
import com.xxl.job.core.thread.TriggerCallbackThread;
import com.xxl.job.core.util.IpUtil;
import com.xxl.job.core.util.NetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by xuxueli on 2016/3/2 21:14.
 */
public class XxlJobExecutor  {
    private static final Logger logger = LoggerFactory.getLogger(XxlJobExecutor.class);

    // 静态实例，用于存储当前执行器的信息
    private static volatile XxlJobExecutor instance;

    /**
     * 获取XxlJobExecutor单例实例
     * 
     * @return XxlJobExecutor实例
     */
    public static XxlJobExecutor getInstance() {
        if (instance == null) {
            throw new RuntimeException("XxlJobExecutor未初始化");
        }
        return instance;
    }

    // ---------------------- param ----------------------
    // 管理员地址列表，用于与调度中心通信
    private String adminAddresses;
    // 访问令牌，用于身份验证
    private String accessToken;
    // 超时时间，单位为秒
    private int timeout;
    // 应用名称
    private String appname;


    // 服务器地址
    private String address;
    // 服务器IP
    private String ip;
    // 服务器端口
    private int port;
    // 日志路径
    private String logPath;
    // 日志保留天数
    private int logRetentionDays;

    // 静态方法，用于获取执行器信息
    public static String getStaticAppname() {
        return instance != null ? instance.appname : "未知执行器";
    }

    public static String getStaticAddress() {
        return instance != null ? instance.address : "";
    }

    public static String getStaticIp() {
        return IpUtil.getIp();
    }

    public static int getStaticPort() {
        return instance != null ? instance.port : 0;
    }

    public static String getStaticAccessToken() {
        return instance != null ? instance.accessToken : "";
    }

    public void setAdminAddresses(String adminAddresses) {
        this.adminAddresses = adminAddresses;
    }
    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
    public void setAppname(String appname) {
        this.appname = appname;
    }
    public void setAddress(String address) {
        this.address = address;
    }
    public void setIp(String ip) {
        this.ip = ip;
    }
    public void setPort(int port) {
        this.port = port;
    }
    public void setLogPath(String logPath) {
        this.logPath = logPath;
    }
    public void setLogRetentionDays(int logRetentionDays) {
        this.logRetentionDays = logRetentionDays;
    }


    // ---------------------- start + stop ----------------------
    /**
     * 启动执行器
     *
     * 该方法初始化日志路径、管理员客户端、日志清理线程、回调线程和嵌入式服务器。
     */
    public void start() throws Exception {
        // 设置静态实例
        instance = this;

        // 初始化日志路径
        XxlJobFileAppender.initLogPath(logPath);

        // 初始化管理员客户端
        initAdminBizList(adminAddresses, accessToken, timeout);

        // 初始化日志清理线程
        JobLogFileCleanThread.getInstance().start(logRetentionDays);

        // 初始化回调线程
        TriggerCallbackThread.getInstance().start();

        // 初始化任务线程监控
        JobThreadMonitorHelper.getInstance().start();

        // 初始化嵌入式服务器
        initEmbedServer(address, ip, port, appname, accessToken);
    }

    /**
     * 销毁执行器
     *
     * 该方法停止嵌入式服务器、清理作业线程和处理器，并停止日志清理和回调线程。
     */
    public void destroy(){
        // 停止嵌入式服务器
        stopEmbedServer();

        // 清理作业线程
        if (jobThreadRepository.size() > 0) {
            for (Map.Entry<Long, JobThread> item: jobThreadRepository.entrySet()) {
                JobThread oldJobThread = removeJobThread(item.getKey(), "web container destroy and kill the job.");
                // 等待作业线程推送结果到回调队列
                if (oldJobThread != null) {
                    try {
                        oldJobThread.join();
                    } catch (InterruptedException e) {
                        logger.error(">>>>>>>>>>> xxl-job, JobThread destroy(join) error, jobId:{}", item.getKey(), e);
                    }
                }
            }
            jobThreadRepository.clear();
        }
        jobHandlerRepository.clear();

        // 清理线程上下文映射
        JobThreadContext.getJobThreadContextMap().clear();
        logger.info(">>>>>>>>>>> xxl-job, JobThreadContext cleared");

        // 停止日志清理线程
        JobLogFileCleanThread.getInstance().toStop();

        // 停止回调线程
        TriggerCallbackThread.getInstance().toStop();

        // 停止任务线程监控
        JobThreadMonitorHelper.getInstance().toStop();
    }

    /**
     * 主动离线执行器
     * 
     * 该方法用于主动将执行器从调度中心注销，
     * 在需要手动下线执行器时调用。
     * 
     * 注意：这不会停止执行器进程，只会停止向调度中心发送心跳。
     * 已经在运行的任务会继续执行直到完成。
     */
    public void offline() {
        logger.info(">>>>>>>>>>> xxl-job, 执行器主动离线开始");
        try {
            // 停止注册线程
            if (embedServer != null) {
                embedServer.stopRegistry();
                logger.info(">>>>>>>>>>> xxl-job, 执行器注册线程已停止");
            }
        } catch (Exception e) {
            logger.error(">>>>>>>>>>> xxl-job, 执行器离线失败", e);
        }
        logger.info(">>>>>>>>>>> xxl-job, 执行器主动离线完成");
    }

    // ---------------------- admin-client (rpc invoker) ----------------------
    private static List<AdminBiz> adminBizList;
    /**
     * 初始化管理员客户端列表
     *
     * @param adminAddresses 管理员地址列表
     * @param accessToken 访问令牌
     * @param timeout 超时时间
     *
     * 该方法根据提供的地址列表创建管理员客户端实例，并添加到列表中。
     */
    private void initAdminBizList(String adminAddresses, String accessToken, int timeout) throws Exception {
        if (adminAddresses!=null && adminAddresses.trim().length()>0) {
            for (String address: adminAddresses.trim().split(",")) {
                if (address!=null && address.trim().length()>0) {

                    AdminBiz adminBiz = new AdminBizClient(address.trim(), accessToken, timeout);

                    if (adminBizList == null) {
                        adminBizList = new ArrayList<AdminBiz>();
                    }
                    adminBizList.add(adminBiz);
                }
            }
        }
    }

    public static List<AdminBiz> getAdminBizList(){
        return adminBizList;
    }

    // ---------------------- executor-server (rpc provider) ----------------------
    private EmbedServer embedServer = null;

    /**
     * 初始化嵌入式服务器
     *
     * @param address 服务器地址
     * @param ip 服务器IP
     * @param port 服务器端口
     * @param appname 应用名称
     * @param accessToken 访问令牌
     *
     * 该方法配置并启动嵌入式服务器，确保其能够接收和处理调度中心的请求。
     */
    private void initEmbedServer(String address, String ip, int port, String appname, String accessToken) throws Exception {

        // 填充IP和端口
        port = port>0?port: NetUtil.findAvailablePort(9999);
        ip = (ip!=null&&ip.trim().length()>0)?ip: IpUtil.getIp();

        // 生成地址
        if (address==null || address.trim().length()==0) {
            String ip_port_address = IpUtil.getIpPort(ip, port);   // 注册地址：默认使用地址进行注册，否则使用ip:port
            address = "http://{ip_port}/".replace("{ip_port}", ip_port_address);
        }

        // 访问令牌
        if (accessToken==null || accessToken.trim().length()==0) {
            logger.warn(">>>>>>>>>>> xxl-job accessToken is empty. To ensure system security, please set the accessToken.");
        }

        // 启动
        embedServer = new EmbedServer();
        embedServer.start(address, port, appname, accessToken);
    }

    /**
     * 停止嵌入式服务器
     *
     * 该方法停止嵌入式服务器的运行。
     */
    private void stopEmbedServer() {
        // 停止提供者工厂
        if (embedServer != null) {
            try {
                embedServer.stop();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }


    // ---------------------- job handler repository ----------------------
    private static ConcurrentMap<String, IJobHandler> jobHandlerRepository = new ConcurrentHashMap<String, IJobHandler>();
    public static IJobHandler loadJobHandler(String name){
        return jobHandlerRepository.get(name);
    }
    /**
     * 注册作业处理器
     *
     * @param name 处理器名称
     * @param jobHandler 作业处理器实例
     *
     * 该方法将作业处理器注册到处理器仓库中。
     */
    public static IJobHandler registJobHandler(String name, IJobHandler jobHandler){
        logger.info(">>>>>>>>>>> xxl-job register jobhandler success, name:{}, jobHandler:{}", name, jobHandler);
        return jobHandlerRepository.put(name, jobHandler);
    }
    /**
     * 注册作业处理器
     *
     * @param xxlJob XXL作业注解
     * @param bean 作业处理器实例
     * @param executeMethod 执行方法
     *
     * 该方法根据注解信息注册作业处理器。
     */
    protected void registJobHandler(XxlJob xxlJob, Object bean, Method executeMethod){
        if (xxlJob == null) {
            return;
        }

        String name = xxlJob.value();
        //make and simplify the variables since they'll be called several times later
        Class<?> clazz = bean.getClass();
        String methodName = executeMethod.getName();
        if (name.trim().length() == 0) {
            throw new RuntimeException("xxl-job method-jobhandler name invalid, for[" + clazz + "#" + methodName + "] .");
        }
        if (loadJobHandler(name) != null) {
            throw new RuntimeException("xxl-job jobhandler[" + name + "] naming conflicts.");
        }

        // execute method
        /*if (!(method.getParameterTypes().length == 1 && method.getParameterTypes()[0].isAssignableFrom(String.class))) {
            throw new RuntimeException("xxl-job method-jobhandler param-classtype invalid, for[" + bean.getClass() + "#" + method.getName() + "] , " +
                    "The correct method format like \" public ReturnT<String> execute(String param) \" .");
        }
        if (!method.getReturnType().isAssignableFrom(ReturnT.class)) {
            throw new RuntimeException("xxl-job method-jobhandler return-classtype invalid, for[" + bean.getClass() + "#" + method.getName() + "] , " +
                    "The correct method format like \" public ReturnT<String> execute(String param) \" .");
        }*/

        executeMethod.setAccessible(true);

        // init and destroy
        Method initMethod = null;
        Method destroyMethod = null;

        if (xxlJob.init().trim().length() > 0) {
            try {
                initMethod = clazz.getDeclaredMethod(xxlJob.init());
                initMethod.setAccessible(true);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("xxl-job method-jobhandler initMethod invalid, for[" + clazz + "#" + methodName + "] .");
            }
        }
        if (xxlJob.destroy().trim().length() > 0) {
            try {
                destroyMethod = clazz.getDeclaredMethod(xxlJob.destroy());
                destroyMethod.setAccessible(true);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("xxl-job method-jobhandler destroyMethod invalid, for[" + clazz + "#" + methodName + "] .");
            }
        }

        // registry jobhandler
        registJobHandler(name, new MethodJobHandler(bean, executeMethod, initMethod, destroyMethod));

    }


    // ---------------------- job thread repository ----------------------
    private static ConcurrentMap<Long, JobThread> jobThreadRepository = new ConcurrentHashMap<Long, JobThread>();
    /**
     * 注册作业线程
     *
     * @param jobId 作业ID
     * @param handler 作业处理器
     * @param removeOldReason 移除旧线程的原因
     *
     * 该方法创建并启动新的作业线程，并将其注册到线程仓库中。
     */
    public static JobThread registJobThread(Long jobId, IJobHandler handler, String removeOldReason){
        JobThread newJobThread = new JobThread(jobId, handler);
        newJobThread.start();
        logger.info(">>>>>>>>>>> xxl-job regist JobThread success, jobId:{}, handler:{}", new Object[]{jobId, handler});
        // 如果还存在旧的任务则直接打断
        JobThread oldJobThread = jobThreadRepository.put(jobId, newJobThread);	// putIfAbsent | oh my god, map's put method return the old value!!!
        if (oldJobThread != null) {
            logger.warn("准备打断旧的任务...");
            oldJobThread.toStop(removeOldReason);
            oldJobThread.interrupt();

            // 从任务线程上下文中移除旧线程
            JobThreadContext.removeJobThread(jobId);
        }

        return newJobThread;
    }

    /**
     * 移除作业线程
     *
     * @param jobId 作业ID
     * @param removeOldReason 移除旧线程的原因
     *
     * 该方法停止并移除指定的作业线程。
     */
    public static JobThread removeJobThread(Long jobId, String removeOldReason){
        JobThread oldJobThread = jobThreadRepository.remove(jobId);
        if (oldJobThread != null) {
            oldJobThread.toStop(removeOldReason);
            oldJobThread.interrupt();

            // 从任务线程上下文中移除
            JobThreadContext.removeJobThread(jobId);

            return oldJobThread;
        }
        return null;
    }

    public static JobThread loadJobThread(Long jobId){
        return jobThreadRepository.get(jobId);
    }

    /**
     * 获取当前正在运行的作业线程数量
     *
     * @return 作业线程数量
     */
    public static int getRunningJobThreadCount(){
        return jobThreadRepository.size();
    }

    /**
     * 获取当前正在执行的任务数量
     * 包括正在运行的任务和队列中等待的任务
     *
     * @return 正在执行的任务数量
     */
    public static int getRunningTaskCount(){
        int count = 0;
        for (JobThread jobThread : jobThreadRepository.values()) {
            if (jobThread.isRunningOrHasQueue()) {
                count++;
            }
        }
        return count;
    }

    /**
     * 获取当前所有任务线程中等待执行的触发任务数量
     *
     * @return 等待执行的触发任务数量
     */
    public static int getPendingTaskCount(){
        int count = 0;
        for (JobThread jobThread : jobThreadRepository.values()) {
            count += jobThread.getPendingTriggerQueueSize();
        }
        return count;
    }
}
