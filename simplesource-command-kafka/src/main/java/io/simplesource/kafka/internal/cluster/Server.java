package io.simplesource.kafka.internal.cluster;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.util.concurrent.FutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Server {

    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private final ClusterConfig config;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final Class channelClass;
    private final PipelineInitializer pipelineInitializer;
    private ServerBootstrap bootstrap;
    private boolean started = false;

    Server(ClusterConfig config, EventLoopGroup bossGroup, EventLoopGroup workerGroup, Class channelClass, PipelineInitializer pipelineInitializer) {
        this.config = config;
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.channelClass = channelClass;
        this.pipelineInitializer = pipelineInitializer;
    }

    public synchronized ChannelFuture start() {
        if (started) {
            throw new IllegalStateException("Attempting to start Simple Sourcing RPC server that has already started");
        }

        started = true;

        bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(channelClass)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.SO_REUSEADDR, config.reuseAddress())
                .option(ChannelOption.SO_BACKLOG, config.acceptBackLog())
                .childOption(ChannelOption.TCP_NODELAY, config.tcpNoDelay())
                .childOption(ChannelOption.SO_SNDBUF, config.tcpSendBufferSize())
                .childOption(ChannelOption.SO_RCVBUF, config.tcpReceiveBufferSize())
                .childOption(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(config.tcpReceiveBufferSize()))
                .childOption(ChannelOption.SO_KEEPALIVE, config.tcpKeepAlive())
                .childOption(ChannelOption.SO_LINGER, config.soLinger())
                .localAddress(config.iface(), config.port())
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch)  {
                        pipelineInitializer.init(ch.pipeline());
                    }
                });

        return bootstrap.bind().addListener((FutureListener<Void>) future -> {
            if (future.isSuccess()) {
                logger.info("Simple Sourcing RPC server started and bound to interface: {} on port: {}",
                        config.iface(), config.port());
            } else {
                logger.error("Simple Sourcing RPC server failed to bind to interface: {} on port: {}",
                        config.iface(), config.port());
            }
        });
    }

    public synchronized void stop() {
        if (!started) {
            throw new IllegalStateException("Attempting to stop Simple Sourcing RPC server that has not been started");
        }

        bossGroup.shutdownGracefully().syncUninterruptibly();
        workerGroup.shutdownGracefully().syncUninterruptibly();
        logger.info("Simple Sourcing RPC server stopped");
    }

}