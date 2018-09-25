package io.simplesource.kafka.internal.cluster;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.*;
import io.netty.util.concurrent.FutureListener;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

class ClientImpl implements Client {
    private static final Logger logger = LoggerFactory.getLogger(ClientImpl.class);

    private final ClusterConfig clusterConfig;
    private final EventLoopGroup workGroup;
    private final Class channelClass;
    private final PipelineInitializer pipelineInitializer;
    private final AbstractChannelPoolMap<HostInfo, ChannelPool> poolMap;

    ClientImpl(ClusterConfig clusterConfig, EventLoopGroup workerGroup, Class channelClass, PipelineInitializer messageHandle) {
        this.clusterConfig = clusterConfig;
        this.workGroup = workerGroup;
        this.channelClass = channelClass;
        this.pipelineInitializer = messageHandle;
        this.poolMap = createPoolMap();
    }

    private AbstractChannelPoolMap<HostInfo, ChannelPool> createPoolMap() {
        return new AbstractChannelPoolMap<HostInfo, ChannelPool>() {
            @Override
            protected ChannelPool newPool(HostInfo hostInfo) {
                Bootstrap bootstrap = new Bootstrap();
                bootstrap.group(workGroup);
                bootstrap.channel(channelClass);
                bootstrap.remoteAddress(hostInfo.host(), hostInfo.port());

                ChannelPool pool = new FixedChannelPool(
                        bootstrap,
                        new AbstractChannelPoolHandler() {
                            @Override
                            public void channelCreated(Channel ch) {
                                pipelineInitializer.init(ch.pipeline());
                            }
                        },
                        ChannelHealthChecker.ACTIVE,
                        FixedChannelPool.AcquireTimeoutAction.FAIL,
                        clusterConfig.acquireTimeoutMillis(),
                        clusterConfig.maxConnectionsPerHost(),
                        clusterConfig.maxPendingAcquires(),
                        true);
                return pool;
            }
        };
    }

    public void send(HostInfo targetHost, Message message) {
        logger.trace("Sending message:{} to {}", message, targetHost);
        final ChannelPool pool = poolMap.get(targetHost);
        pool.acquire().addListener((FutureListener<Channel>) channelFuture -> {
            if (channelFuture.isSuccess()) {
                Channel ch = channelFuture.getNow();
                ch.writeAndFlush(message);
                pool.release(ch);
            } else {
                logger.error("Failed to connect to remote system:" + targetHost, channelFuture.cause());
            }
        });
    }

    public void stop() {
        poolMap.close();
    }

}
