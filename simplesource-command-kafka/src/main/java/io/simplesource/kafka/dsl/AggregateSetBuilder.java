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

public final class AggregateSetBuilder {
    private KafkaConfig kafkaConfig;
    private Map<String, AggregateSpec<?, ?, ?, ?>> aggregateConfigMap = new HashMap<>();

    public AggregateSetBuilder withKafkaConfig(
            final Function<KafkaConfig.Builder, KafkaConfig> builder) {
        kafkaConfig = builder.apply(new KafkaConfig.Builder());
        return this;
    }

    public AggregateSetBuilder withKafkaConfig(final KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        return this;
    }

    public <K, C, E, A> AggregateSetBuilder addAggregate(
            final Function<AggregateBuilder<K, C, E, A>, AggregateSpec<K, C, E, A>> builder) {
        final AggregateSpec<K, C, E, A> spec = builder.apply(AggregateBuilder.newBuilder());
        aggregateConfigMap.put(spec.aggregateName(), spec);
        return this;
    }

    public <K, C, E, A> AggregateSetBuilder addAggregate(final AggregateSpec<K, C, E, A> spec) {
        aggregateConfigMap.put(spec.aggregateName(), spec);
        return this;
    }

    public AggregateSetSpec build() {
        final AggregateSetSpec aggregateSetSpec = new AggregateSetSpec(
                kafkaConfig,
                aggregateConfigMap);

        final EventSourcedStreamsApp app =
                new EventSourcedStreamsApp(aggregateSetSpec);

        app.start();
        return aggregateSetSpec;
    }

    /**
     * Creates a CommandAPISet instance from a AggregateSetSpec
     *
     * Used for directly exposing a CommandAPISet from within a Simple Sourcing application
     * If creating a CommandAPISet from an external application, rather use the CommandAPISetBuilder DSL
     *
     * @param aggregateSetSpec
     * @return a CommandAPISet
     */
    static CommandAPISet getCommandAPISet(AggregateSetSpec aggregateSetSpec) {
        Stream<CommandSpec<?, ?>> commandSpecs = aggregateSetSpec
                .aggregateConfigMap()
                .values()
                .stream()
                .map(aSpec -> aSpec.getCommandSpec());

        return CommandApiSetBuilder.getCommandAPISet(commandSpecs, aggregateSetSpec.kafkaConfig());
    }
}


