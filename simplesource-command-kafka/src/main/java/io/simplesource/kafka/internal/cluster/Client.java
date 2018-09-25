package io.simplesource.kafka.internal.cluster;

import org.apache.kafka.streams.state.HostInfo;

@FunctionalInterface
public interface Client {

    void send(HostInfo targetHost, Message message);

}
