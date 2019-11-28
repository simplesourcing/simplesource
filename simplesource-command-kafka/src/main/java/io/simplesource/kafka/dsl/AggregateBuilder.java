package io.simplesource.kafka.dsl;

import io.simplesource.api.CommandHandler;
import io.simplesource.api.InvalidSequenceHandler;
import io.simplesource.kafka.api.AggregateResources.TopicEntity;
import io.simplesource.api.Aggregator;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.api.InitialValue;
import io.simplesource.kafka.api.ResourceNamingStrategy;
import io.simplesource.kafka.internal.streams.InvalidSequenceHandlerProvider;
import io.simplesource.kafka.spec.AggregateSpec;
import io.simplesource.kafka.spec.TopicSpec;
import io.simplesource.kafka.spec.WindowSpec;
import org.apache.kafka.common.config.TopicConfig;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public final class AggregateBuilder<K, C, E, A> {
    private String name;
    private ResourceNamingStrategy resourceNamingStrategy;
    private AggregateSerdes<K, C, E, A> aggregateSerdes;
    private final Map<TopicEntity, TopicSpec> topicConfig = new HashMap<>();
    private WindowSpec commandResponseStoreSpec;
    private InitialValue<K, A> initialValue;
    private CommandHandler<K, C, E, A> commandHandler;
    private Aggregator<E, A> aggregator;
    private InvalidSequenceHandler<K, C, A> invalidSequenceHandler;

    public static <K, C, E, A> AggregateBuilder<K, C, E, A> newBuilder() {
        return new AggregateBuilder<>();
    }

    private AggregateBuilder() {
        commandResponseStoreSpec = new WindowSpec(TimeUnit.DAYS.toSeconds(1L));
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

    public AggregateBuilder<K, C, E, A> withDefaultTopicSpec(final int partitions, final int replication, final int retentionDays) {
        Map<TopicEntity, TopicSpec> defaultTopicConf = defaultTopicConfig(partitions, replication, retentionDays);
        defaultTopicConf.keySet().forEach(topicKey -> topicConfig.putIfAbsent(topicKey, defaultTopicConf.get(topicKey)));
        return this;
    }

    public AggregateBuilder<K, C, E, A> withTopicSpec(final TopicEntity topicEntity, final TopicSpec topicSpec) {
        topicConfig.put(topicEntity, topicSpec);
        return this;
    }

    public AggregateBuilder<K, C, E, A> withCommandResponseRetention(final long retentionInSeconds) {
        commandResponseStoreSpec = new WindowSpec(retentionInSeconds);
        return this;
    }

    public AggregateBuilder<K, C, E, A> withInitialValue(final InitialValue<K, A> initialValue) {
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

    public AggregateBuilder<K, C, E, A> withInvalidSequenceStrategy(final InvalidSequenceStrategy invalidSequenceStrategy) {
        this.invalidSequenceHandler = InvalidSequenceHandlerProvider.getForStrategy(invalidSequenceStrategy);
        return this;
    }

    public <SC extends C> AggregateSpec<K, C, E, A> build() {
        requireNonNull(name, "No name for aggregate has been defined");
        requireNonNull(resourceNamingStrategy, "No resource naming strategy for aggregate has been defined");
        requireNonNull(aggregateSerdes, "No domain serializer for aggregate has been defined");
        requireNonNull(initialValue, "No initial value for aggregate has been defined");
        requireNonNull(commandHandler, "No CommandHandler for aggregate has been defined");
        requireNonNull(aggregator, "No Aggregator for aggregate has been defined");

        if (topicConfig.isEmpty()) {
            throw new RuntimeException("No topic config for aggregate has been defined");
        }

        // by default strict
        if (invalidSequenceHandler == null)
            invalidSequenceHandler = InvalidSequenceHandlerProvider.getForStrategy(InvalidSequenceStrategy.Strict);

        // set missing topic configuration to default
        withDefaultTopicSpec(1, 1, 1);

        final AggregateSpec.Serialization<K, C, E, A> serialization =
            new AggregateSpec.Serialization<>(resourceNamingStrategy, aggregateSerdes);
        final AggregateSpec.Generation<K, C, E, A> generation =
            new AggregateSpec.Generation<>(topicConfig, commandResponseStoreSpec, commandHandler, invalidSequenceHandler, aggregator, initialValue);

        return new AggregateSpec<>(name, serialization, generation);
    }

    private Map<TopicEntity, TopicSpec> defaultTopicConfig(final int partitions, final int replication, final int retentionDays) {
        short replicationShort = (short)replication;
        final Map<TopicEntity, TopicSpec> config = new HashMap<>();
        String retentionMillis = String.valueOf(TimeUnit.DAYS.toMillis(retentionDays));

        final Map<String, String> commandRequestTopic = new HashMap<>();
        commandRequestTopic.put(TopicConfig.RETENTION_MS_CONFIG, retentionMillis);
        config.put(
            TopicEntity.COMMAND_REQUEST,
            new TopicSpec(partitions, replicationShort, commandRequestTopic));

        // turn on log compaction of aggregates by default
        final Map<String, String> aggregateTopic = new HashMap<>();
        aggregateTopic.put(TopicConfig.CLEANUP_POLICY_CONFIG,TopicConfig.CLEANUP_POLICY_COMPACT);
        aggregateTopic.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, retentionMillis);
        aggregateTopic.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, retentionMillis);
        config.put(
            TopicEntity.AGGREGATE,
            new TopicSpec(partitions, replicationShort, aggregateTopic));

        // never delete old log segments for events
        final Map<String, String> eventTopic = new HashMap<>();
        eventTopic.put(TopicConfig.RETENTION_MS_CONFIG, "-1");
        config.put(
                TopicEntity.EVENT,
                new TopicSpec(partitions, replicationShort, eventTopic));

        final Map<String, String> commandResponseTopic = new HashMap<>();
        aggregateTopic.put(TopicConfig.RETENTION_MS_CONFIG, retentionMillis);
        config.put(
                TopicEntity.COMMAND_RESPONSE,
                new TopicSpec(partitions, replicationShort, commandResponseTopic));

        final Map<String, String> commandResponseTopicMapTopic = new HashMap<>();
        aggregateTopic.put(TopicConfig.RETENTION_MS_CONFIG, retentionMillis);
        config.put(
                TopicEntity.COMMAND_RESPONSE_TOPIC_MAP,
                new TopicSpec(partitions, replicationShort, commandResponseTopicMapTopic));

        return config;
    }
}
