package io.simplesource.kafka.dsl;

import io.simplesource.api.CommandAPISet;
import io.simplesource.kafka.internal.streams.EventSourcedStreamsApp;
import io.simplesource.kafka.spec.AggregateSetSpec;
import io.simplesource.kafka.spec.AggregateSpec;
import io.simplesource.kafka.spec.CommandSpec;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public final class EventSourcedApp {
    private KafkaConfig kafkaConfig;
    private Map<String, AggregateSpec<?, ?, ?, ?>> aggregateConfigMap = new HashMap<>();
    private AggregateSetSpec aggregateSetSpec;

    public EventSourcedApp withKafkaConfig(
            final Function<KafkaConfig.Builder, KafkaConfig> builder) {
        kafkaConfig = builder.apply(new KafkaConfig.Builder());
        return this;
    }

    public EventSourcedApp withKafkaConfig(final KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        return this;
    }

    public <K, C, E, A> EventSourcedApp addAggregate(
            final Function<AggregateBuilder<K, C, E, A>, AggregateSpec<K, C, E, A>> builder) {
        final AggregateSpec<K, C, E, A> spec = builder.apply(AggregateBuilder.newBuilder());
        aggregateConfigMap.put(spec.aggregateName(), spec);
        return this;
    }

    public <K, C, E, A> EventSourcedApp addAggregate(final AggregateSpec<K, C, E, A> spec) {
        aggregateConfigMap.put(spec.aggregateName(), spec);
        return this;
    }

    public EventSourcedApp start() {
        final AggregateSetSpec aggregateSetSpec = new AggregateSetSpec(
                kafkaConfig,
                aggregateConfigMap);

        final EventSourcedStreamsApp app =
                new EventSourcedStreamsApp(aggregateSetSpec);

        app.start();
        this.aggregateSetSpec = aggregateSetSpec;
        return this;
    }

    /**
     * Creates a CommandAPISet instance
     *
     * Used for directly exposing a CommandAPISet from within a Simple Sourcing application
     * If creating a CommandAPISet from an external application, rather use the EventSourcedClient DSL
     *
     * @return a CommandAPISet
     */
    public CommandAPISet getCommandAPISet(String clientId) {
        requireNonNull(aggregateSetSpec, "App has not been started. start() must be called before getCommandAPISet");
        Stream<CommandSpec<?, ?>> commandSpecs = aggregateSetSpec
                .aggregateConfigMap()
                .values()
                .stream()
                .map(aSpec -> aSpec.getCommandSpec(clientId));

        return EventSourcedClient.getCommandAPISet(commandSpecs, aggregateSetSpec.kafkaConfig());
    }
}
