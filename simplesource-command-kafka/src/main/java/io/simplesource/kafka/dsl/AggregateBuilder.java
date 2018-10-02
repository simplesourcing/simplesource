package io.simplesource.kafka.dsl;

import io.simplesource.api.CommandHandler;
import io.simplesource.kafka.api.AggregateResources.TopicEntity;
import io.simplesource.api.Aggregator;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.api.InitialValue;
import io.simplesource.kafka.api.ResourceNamingStrategy;
import io.simplesource.kafka.internal.util.RetryDelay;
import io.simplesource.kafka.spec.AggregateSpec;
import io.simplesource.kafka.spec.TopicSpec;
import io.simplesource.kafka.spec.WindowedStateStoreSpec;
import org.apache.kafka.common.config.TopicConfig;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public final class AggregateBuilder<K, C, E, A> {
    private String name;
    private ResourceNamingStrategy resourceNamingStrategy;
    private AggregateSerdes<K, C, E, A> aggregateSerdes;
    private Map<TopicEntity, TopicSpec> topicConfig;
    private WindowedStateStoreSpec commandResponseStoreSpec;
    private InitialValue<K, A> initialValue;
    private RetryDelay retryDelay = (startTime, timeoutMillis, spinCount) -> 15L;
    private CommandHandler<K, C, E, A> commandHandler;
    private Aggregator<E, A> aggregator;

    public static <K, C, E, A> AggregateBuilder<K, C, E, A> newBuilder() {
        return new AggregateBuilder<>();
    }

    private AggregateBuilder() {
        topicConfig = defaultTopicConfig();
        commandResponseStoreSpec = new WindowedStateStoreSpec(3600);
    }

    public AggregateBuilder<K, C, E, A> withName(final String name) {
        this.name = name;
        return this;
    }

    public AggregateBuilder<K, C, E, A> withResourceNamingStrategy(final ResourceNamingStrategy resourceNamingStrategy) {
        this.resourceNamingStrategy = resourceNamingStrategy;
        return this;
    }

    public AggregateBuilder<K, C, E, A> withSerdes(final AggregateSerdes<K, C, E, A> aggregateSerdes) {
        this.aggregateSerdes = aggregateSerdes;
        return this;
    }

    public AggregateBuilder<K, C, E, A> withTopicSpec(final TopicEntity topicEntity, final TopicSpec topicSpec) {
        topicConfig.put(topicEntity, topicSpec);
        return this;
    }

    public AggregateBuilder<K, C, E, A> withCommandResponseRetention(final long retentionInSeconds) {
        commandResponseStoreSpec = new WindowedStateStoreSpec(retentionInSeconds);
        return this;
    }

    public AggregateBuilder<K, C, E, A> withRetryDelay(final RetryDelay retryDelay) {
        this.retryDelay = retryDelay;
        return this;
    }

    public AggregateBuilder<K, C, E, A> withInitialValue(final InitialValue initialValue) {
        this.initialValue = initialValue;
        return this;
    }

    public AggregateBuilder<K, C, E, A> withAggregator(final Aggregator<E, A> aggregator) {
        this.aggregator = aggregator;
        return this;
    }

    public AggregateBuilder<K, C, E, A> withCommandHandler(final CommandHandler<K, C, E, A> commandHandler) {
        this.commandHandler = commandHandler;
        return this;
    }

    public <SC extends C> AggregateSpec<K, C, E, A> build() {
        requireNonNull(name, "No name for aggregate has been defined");
        requireNonNull(resourceNamingStrategy, "No resource naming strategy for aggregate has been defined");
        requireNonNull(aggregateSerdes, "No domain serializer for aggregate has been defined");
        requireNonNull(topicConfig, "No topic config for aggregate has been defined");
        requireNonNull(initialValue, "No initial value for aggregate has been defined");
        requireNonNull(commandHandler, "No CommandHandler for aggregate has been defined");
        requireNonNull(aggregator, "No Aggregator for aggregate has been defined");

        // defensive copy

        final AggregateSpec.Serialization<K, C, E, A> serialization =
            new AggregateSpec.Serialization<>(resourceNamingStrategy, aggregateSerdes);
        final AggregateSpec.Generation<K, C, E, A> generation =
            new AggregateSpec.Generation<>(topicConfig, commandResponseStoreSpec, retryDelay, commandHandler, aggregator, initialValue);

        return  new AggregateSpec<>(name, serialization, generation);
    }

    private Map<TopicEntity, TopicSpec> defaultTopicConfig() {
        final Map<TopicEntity, TopicSpec> config = new HashMap<>();

        final Map<String, String> commandRequestTopic = new HashMap<>();
        commandRequestTopic.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(TimeUnit.HOURS.toMillis(1)));
        config.put(
            TopicEntity.command_request,
            new TopicSpec(1, (short)1, commandRequestTopic));

        final Map<String, String> aggregateTopic = new HashMap<>();
        aggregateTopic.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(TimeUnit.DAYS.toMillis(1)));
        config.put(
            TopicEntity.aggregate,
            new TopicSpec(1, (short)1, aggregateTopic));

        final Map<String, String> eventTopic = new HashMap<>();
        eventTopic.put(TopicConfig.RETENTION_MS_CONFIG, "-1"); // never delete old log segments
        config.put(
            TopicEntity.event,
            new TopicSpec(8, (short)1, eventTopic));

        return config;
    }
}
