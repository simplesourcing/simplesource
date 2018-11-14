package io.simplesource.kafka.spec;

import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.api.ResourceNamingStrategy;
import io.simplesource.kafka.internal.util.RetryDelay;
import lombok.Value;

@Value
public final class CommandSpec<K, C> {
    private final String aggregateName;
    private final ResourceNamingStrategy resourceNamingStrategy;
    private final CommandSerdes<K, C> serdes;
    private final RetryDelay retryDelay;
    private final TopicSpec outputTopicConfig;
}
