package io.simplesource.kafka.internal.cluster;

import io.simplesource.api.*;
import io.simplesource.data.Sequence;
import io.simplesource.data.FutureResult;
import io.simplesource.data.NonEmptyList;
import io.simplesource.kafka.api.RemoteCommandResponseStore;
import io.simplesource.kafka.internal.util.NamedThreadFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.kafka.streams.state.HostInfo;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

public final class ClusterSubsystem implements RemoteCommandResponseStore {

    private final ClusterConfig config;
    private final Server server;
    private final ClientImpl client;
    private final RequestResponseMapper requestResponseMapper;

    public ClusterSubsystem(CommandAPILoader aggregates, ClusterConfig config, ScheduledExecutorService scheduledExecutorService) {
        this.config = config;
        final EventLoopGroup bossGroup = getEventLoopGroup(config.bossThreadCount(), config.isNative(), new NamedThreadFactory("SimpleSourcing-Cluster-BossThread"));
        final EventLoopGroup workerGroup = getEventLoopGroup(config.bossThreadCount(), config.isNative(), new NamedThreadFactory("SimpleSourcing-Cluster-WorkerThread"));
        this.requestResponseMapper = new RequestResponseMapper(new HostInfo(config.iface(), config.port()), scheduledExecutorService);

        final Client[] clientRef = new Client[1];
        final MessageHandler messageHandler = new MessageHandler((targetHost, message) -> clientRef[0].send(targetHost, message),
                aggregates, requestResponseMapper);

        final PipelineInitializer pipelineInitializer = new PipelineInitializer(messageHandler);

        this.server = new Server(config, bossGroup, workerGroup, getServerSocketChannel(), pipelineInitializer);
        this.client = new ClientImpl(config, workerGroup, getClientSocketChannel(), pipelineInitializer);
        clientRef[0] = client;
    }

    @Override
    public FutureResult<CommandAPI.CommandError, NonEmptyList<Sequence>> get(HostInfo targetHost, String aggregateName, UUID commandId, Duration timeout) {
        final MappedCommandRequest request = requestResponseMapper.newCommandRequest(targetHost, aggregateName, commandId, timeout);
        client.send(targetHost, request.commandRequest());
        return FutureResult.ofCompletableFuture(request.completableFuture());
    }

    private EventLoopGroup getEventLoopGroup(int threadCount, boolean isNative, ThreadFactory threadFactory) {
        return isNative ? new EpollEventLoopGroup(threadCount, threadFactory) :
                new NioEventLoopGroup(threadCount, threadFactory);
    }

    private Class getServerSocketChannel() {
        return config.isNative() ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
    }

    private Class getClientSocketChannel() {
        return config.isNative() ? EpollSocketChannel.class : NioSocketChannel.class;
    }

    public void start() {
        server.start();
    }

    public void stop() {
        try {
            client.stop();
        } finally {
            server.stop();
        }
    }

}
