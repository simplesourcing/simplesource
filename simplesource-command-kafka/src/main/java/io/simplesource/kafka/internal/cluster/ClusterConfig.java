package io.simplesource.kafka.internal.cluster;

import lombok.Data;

import java.util.concurrent.TimeUnit;

@Data
public final class ClusterConfig {
    //TODO add support for external interface to be different to the local host:port binding
    private String iface = "127.0.0.1";
    private int port = 1125;
    private boolean isNative = false; //TODO guess this somehow
    private int bossThreadCount = 2;
    private int workerThreadCount = 4; //TODO number of cores
    private boolean tcpNoDelay = true;
    private int tcpSendBufferSize = 2048;
    private int tcpReceiveBufferSize = 2048;
    private boolean tcpKeepAlive = false;
    private int soLinger = -1;
    private boolean reuseAddress = false;
    private int acceptBackLog = 2048;
    private int maxConnectionsPerHost = 3;
    private int maxPendingAcquires = Integer.MAX_VALUE; //TODO sensible default
    private long acquireTimeoutMillis = TimeUnit.MINUTES.toMillis(5);
}
