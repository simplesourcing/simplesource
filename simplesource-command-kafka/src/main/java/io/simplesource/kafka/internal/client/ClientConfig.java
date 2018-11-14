package io.simplesource.kafka.internal.client;

import lombok.Data;

import java.util.concurrent.TimeUnit;

@Data
public final class ClientConfig {
    //TODO add support for external interface to be different to the local host:port binding
    private String iface = "127.0.0.1";
    private int port = 1125;
}
