package io.simplesource.kafka.spec;

import io.simplesource.api.Aggregator;
import io.simplesource.api.CommandHandler;
import io.simplesource.api.SequenceHandler;
import io.simplesource.api.InitialValue;
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
        private final CommandHandler<K, C, E, A> commandHandler;
        private final SequenceHandler<K, C, A> sequenceHandler;
        private final Aggregator<E, A> aggregator;
        private final InitialValue<K, A> initialValue;
    }
}
