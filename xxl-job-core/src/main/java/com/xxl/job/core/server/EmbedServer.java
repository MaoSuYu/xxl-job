package com.xxl.job.core.server;

import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.impl.ExecutorBizImpl;
import com.xxl.job.core.biz.model.*;
import com.xxl.job.core.thread.ExecutorRegistryThread;
import com.xxl.job.core.util.GsonTool;
import com.xxl.job.core.util.ThrowableUtil;
import com.xxl.job.core.util.XxlJobRemotingUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * 嵌入式服务器类
 *
 * 该类用于启动一个基于 Netty 的 HTTP 服务器，
 * 负责接收调度中心的请求并进行处理。
 *
 * 工作原理：
 * 1. 使用 Netty 启动一个 HTTP 服务器，监听指定端口。
 * 2. 通过线程池处理接收到的请求，确保高并发处理能力。
 * 3. 支持心跳检测、任务执行、任务终止、日志查询等功能。
 * 4. 通过 ExecutorBizImpl 实现具体的业务逻辑。
 *
 * 关键组件：
 * - ServerBootstrap：Netty 服务器启动器，用于配置和启动服务器。
 * - NioEventLoopGroup：Netty 的事件循环组，负责处理连接的接入和数据读写。
 * - ThreadPoolExecutor：线程池，用于处理业务逻辑，避免阻塞 Netty 的 I/O 线程。
 * - EmbedHttpServerHandler：Netty 的通道处理器，负责解析 HTTP 请求并调用业务逻辑。
 *
 * 该类的设计确保了高效的网络通信和灵活的业务处理能力。
 *
 * Copy from : https://github.com/xuxueli/xxl-rpc
 *
 * 作者：xuxueli 2020-04-11 21:25
 */
public class EmbedServer {
    private static final Logger logger = LoggerFactory.getLogger(EmbedServer.class);

    // 执行业务接口，用于处理具体的业务逻辑
    private ExecutorBiz executorBiz;
    // 服务器线程，用于启动和管理服务器的生命周期
    private Thread thread;

    /**
     * 启动嵌入式服务器
     *
     * @param address     服务器地址
     * @param port        服务器端口
     * @param appname     应用名称
     * @param accessToken 访问令牌
     *
     * 该方法初始化并启动一个 Netty HTTP 服务器，
     * 并通过线程池处理业务请求。
     */
    public void start(final String address, final int port, final String appname, final String accessToken) {
        // 实例化业务处理接口
        executorBiz = new ExecutorBizImpl();
        // 创建并启动服务器线程
        thread = new Thread(new Runnable() {
            @Override
            public void run() {
                // 初始化 Netty 的事件循环组
                EventLoopGroup bossGroup = new NioEventLoopGroup();
                EventLoopGroup workerGroup = new NioEventLoopGroup();
                // 创建业务线程池，处理业务逻辑
                ThreadPoolExecutor bizThreadPool = new ThreadPoolExecutor(
                        0,
                        200,
                        60L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<Runnable>(2000),
                        new ThreadFactory() {
                            @Override
                            public Thread newThread(Runnable r) {
                                return new Thread(r, "xxl-job, EmbedServer bizThreadPool-" + r.hashCode());
                            }
                        },
                        new RejectedExecutionHandler() {
                            @Override
                            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                                throw new RuntimeException("xxl-job, EmbedServer bizThreadPool is EXHAUSTED!");
                            }
                        });
                try {
                    // 配置并启动 Netty 服务器
                    ServerBootstrap bootstrap = new ServerBootstrap();
                    bootstrap.group(bossGroup, workerGroup)
                            .channel(NioServerSocketChannel.class)
                            .childHandler(new ChannelInitializer<SocketChannel>() {
                                @Override
                                public void initChannel(SocketChannel channel) throws Exception {
                                    channel.pipeline()
                                            .addLast(new IdleStateHandler(0, 0, 30 * 3, TimeUnit.SECONDS))  // 心跳检测，空闲时关闭连接
                                            .addLast(new HttpServerCodec())
                                            .addLast(new HttpObjectAggregator(5 * 1024 * 1024))  // 聚合 HTTP 请求和响应
                                            .addLast(new EmbedHttpServerHandler(executorBiz, accessToken, bizThreadPool));
                                }
                            })
                            .childOption(ChannelOption.SO_KEEPALIVE, true);

                    // 绑定端口并启动
                    ChannelFuture future = bootstrap.bind(port).sync();

                    logger.info(">>>>>>>>>>> xxl-job remoting server start success, nettype = {}, port = {}", EmbedServer.class, port);

                    // 启动注册线程
                    startRegistry(appname, address);

                    // 等待服务器关闭
                    future.channel().closeFuture().sync();

                } catch (InterruptedException e) {
                    logger.info(">>>>>>>>>>> xxl-job remoting server stop.");
                } catch (Throwable e) {
                    logger.error(">>>>>>>>>>> xxl-job remoting server error.", e);
                } finally {
                    // 关闭事件循环组
                    try {
                        workerGroup.shutdownGracefully();
                        bossGroup.shutdownGracefully();
                    } catch (Throwable e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        });
        thread.setDaemon(true);    // 设置为守护线程，确保 JVM 退出时自动关闭
        thread.start();
    }

    /**
     * 停止嵌入式服务器
     *
     * 该方法用于停止服务器线程并注销注册信息。
     */
    public void stop() throws Exception {
        // 销毁服务器线程
        if (thread != null && thread.isAlive()) {
            thread.interrupt();
        }

        // 停止注册
        stopRegistry();
        logger.info(">>>>>>>>>>> xxl-job remoting server destroy success.");
    }

    // ---------------------- registry ----------------------

    /**
     * 嵌入式 HTTP 服务器处理器
     *
     * 该类负责处理 HTTP 请求，解析请求数据并调用相应的业务逻辑。
     * 支持的请求包括心跳检测、任务执行、任务终止、日志查询等。
     */
    public static class EmbedHttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
        private static final Logger logger = LoggerFactory.getLogger(EmbedHttpServerHandler.class);

        private ExecutorBiz executorBiz;
        private String accessToken;
        private ThreadPoolExecutor bizThreadPool;

        public EmbedHttpServerHandler(ExecutorBiz executorBiz, String accessToken, ThreadPoolExecutor bizThreadPool) {
            this.executorBiz = executorBiz;
            this.accessToken = accessToken;
            this.bizThreadPool = bizThreadPool;
        }

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
            // 解析请求
            String requestData = msg.content().toString(CharsetUtil.UTF_8);
            String uri = msg.uri();
            HttpMethod httpMethod = msg.method();
            boolean keepAlive = HttpUtil.isKeepAlive(msg);
            String accessTokenReq = msg.headers().get(XxlJobRemotingUtil.XXL_JOB_ACCESS_TOKEN);

            // 执行业务逻辑
            bizThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    // 调用相应的业务逻辑
                    Object responseObj = process(httpMethod, uri, requestData, accessTokenReq);

                    // 转换为 JSON
                    String responseJson = GsonTool.toJson(responseObj);

                    // 写入响应
                    writeResponse(ctx, keepAlive, responseJson);
                }
            });
        }

        private Object process(HttpMethod httpMethod, String uri, String requestData, String accessTokenReq) {
            // 验证请求
            if (HttpMethod.POST != httpMethod) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, HttpMethod not support.");
            }
            if (uri == null || uri.trim().length() == 0) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, uri-mapping empty.");
            }
            if (accessToken != null
                    && accessToken.trim().length() > 0
                    && !accessToken.equals(accessTokenReq)) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "The access token is wrong.");
            }

            // 服务映射
            try {
                switch (uri) {
                    case "/beat":
                        return executorBiz.beat();
                    case "/idleBeat":
                        IdleBeatParam idleBeatParam = GsonTool.fromJson(requestData, IdleBeatParam.class);
                        return executorBiz.idleBeat(idleBeatParam);
                    case "/run":
                        TriggerParam triggerParam = GsonTool.fromJson(requestData, TriggerParam.class);
                        return executorBiz.run(triggerParam);
                    case "/kill":
                        KillParam killParam = GsonTool.fromJson(requestData, KillParam.class);
                        return executorBiz.kill(killParam);
                    case "/log":
                        LogParam logParam = GsonTool.fromJson(requestData, LogParam.class);
                        return executorBiz.log(logParam);
                    case "/status":
                        return executorBiz.status();
                    case "/forceKill":
                        Long jobId = GsonTool.fromJson(requestData, Long.class);
                        return executorBiz.forceKill(jobId);
                    case "/offline":
                        String id = GsonTool.fromJson(requestData, String.class);
                        return executorBiz.offline(id);
                    default:
                        return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, uri-mapping(" + uri + ") not found.");
                }
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);
                return new ReturnT<String>(ReturnT.FAIL_CODE, "request error:" + ThrowableUtil.toString(e));
            }
        }

        /**
         * 写入响应
         */
        private void writeResponse(ChannelHandlerContext ctx, boolean keepAlive, String responseJson) {
            // 写入响应
            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.copiedBuffer(responseJson, CharsetUtil.UTF_8));   //  Unpooled.wrappedBuffer(responseJson)
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html;charset=UTF-8");       // HttpHeaderValues.TEXT_PLAIN.toString()
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
            if (keepAlive) {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }
            ctx.writeAndFlush(response);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error(">>>>>>>>>>> xxl-job provider netty_http server caught exception", cause);
            ctx.close();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                ctx.channel().close();      // 心跳检测，空闲时关闭连接
                logger.debug(">>>>>>>>>>> xxl-job provider netty_http server close an idle channel.");
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }
    }

    // ---------------------- registry ----------------------

    /**
     * 启动注册线程
     *
     * @param appname 应用名称
     * @param address 服务器地址
     *
     * 该方法启动一个注册线程，
     * 用于将执行器注册到调度中心。
     */
    public void startRegistry(final String appname, final String address) {
        // 启动注册
        ExecutorRegistryThread.getInstance().start(appname, address);
    }

    /**
     * 停止注册线程
     *
     * 该方法用于停止注册线程，
     * 取消执行器在调度中心的注册。
     */
    public void stopRegistry() {
        // 停止注册
        ExecutorRegistryThread.getInstance().toStop();
    }
}
