package io.simplesource.kafka.spec;

import lombok.Value;

import java.util.Map;

@Value
public final class TopicSpec {
    private final int partitionCount;
    private final short replicaCount;
    private final Map<String, String> config;
}
