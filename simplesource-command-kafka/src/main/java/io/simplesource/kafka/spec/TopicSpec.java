package io.simplesource.kafka.spec;

import lombok.Value;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

@Value
public final class TopicSpec {
    private final int partitionCount;
    private final short replicaCount;
    private final Map<String, String> config;

    public static TopicSpec of(final int partitionCount, final int replicaCount) {
        return new TopicSpec(partitionCount, (short)replicaCount, Collections.emptyMap());
    }

    public static TopicSpec of(final int partitionCount, final int replicaCount, Map<String, String> config) {
        return new TopicSpec(partitionCount, (short)replicaCount, config);
    }
}
