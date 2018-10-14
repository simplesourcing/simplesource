package io.simplesource.kafka.spec;

import io.simplesource.api.*;
import io.simplesource.kafka.api.*;
import io.simplesource.kafka.internal.util.RetryDelay;
import lombok.Value;

import java.util.Map;

@Value
public final class AggregateSpec<K, C, E, A>  {
    private final String aggregateName;
    private final Serialization<K, C, E, A> serialization;
    private final Generation<K, C, E, A> generation;

    @Value
    public static class Serialization<K, C, E, A> {
        private final ResourceNamingStrategy resourceNamingStrategy;
        private final AggregateSerdes<K, C, E, A> serdes;
    }

    @Value
    public static class Generation<K, C, E, A> {
        private final Map<AggregateResources.TopicEntity, TopicSpec> topicConfig;
        private final WindowedStateStoreSpec stateStoreSpec;
        private final RetryDelay retryDelay;
        private final CommandAggregateKey<K, C> commandAggregateKey;
        private final CommandHandler<K, C, E, A> commandHandler;
        private final InvalidSequenceHandler<K, C, A> invalidSequenceHandler;
        private final Aggregator<E, A> aggregator;
        private final InitialValue<K, A> initialValue;
    }
}
