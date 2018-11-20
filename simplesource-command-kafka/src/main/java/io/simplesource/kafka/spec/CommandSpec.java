package io.simplesource.kafka.spec;

import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.api.ResourceNamingStrategy;
import lombok.Value;

@Value
public final class CommandSpec<K, C> {
    private final String aggregateName;
    private final String clientId;
    private final ResourceNamingStrategy resourceNamingStrategy;
    private final CommandSerdes<K, C> serdes;
    private final WindowSpec commandResponseWindowSpec;
    private final TopicSpec outputTopicConfig;
}
