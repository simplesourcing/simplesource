package io.simplesource.kafka.dsl;

import io.simplesource.api.CommandAPISet;
import io.simplesource.kafka.internal.streams.EventSourcedStreamsApp;
import io.simplesource.kafka.internal.util.NamedThreadFactory;
import io.simplesource.kafka.spec.AggregateSetSpec;
import io.simplesource.kafka.spec.AggregateSpec;
import io.simplesource.kafka.spec.CommandSpec;
import io.simplesource.kafka.util.SpecUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public final class EventSourcedApp {
    private KafkaConfig kafkaConfig;
    private Map<String, AggregateSpec<?, ?, ?, ?>> aggregateConfigMap = new HashMap<>();
    private AggregateSetSpec aggregateSetSpec;
    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("EventSourcedApp-scheduler"));;

    public EventSourcedApp withKafkaConfig(
            final Function<KafkaConfig.Builder, KafkaConfig> builder) {
        kafkaConfig = builder.apply(new KafkaConfig.Builder());
        return this;
    }

    public EventSourcedApp withKafkaConfig(final KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        return this;
    }

    public EventSourcedApp withScheduler(final ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
        return this;
    }

    public <K, C, E, A> EventSourcedApp addAggregate(
            final Consumer<AggregateBuilder<K, C, E, A>> buildSteps) {
        AggregateBuilder<K, C, E, A> builder = AggregateBuilder.newBuilder();
        buildSteps.accept(builder);
        final AggregateSpec<K, C, E, A> spec = builder.build();
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
     * If creating a CommandAPISet from an external application, rather use the CommandAPISetBuilder DSL
     *
     * @return a CommandAPISet
     */
    public CommandAPISet getCommandAPISet(String clientId) {
        requireNonNull(aggregateSetSpec, "App has not been started. start() must be called before getCommandAPISet");
        requireNonNull(scheduler, "Scheduler has not been defined. Please define with with 'withScheduler' method.");
        Stream<CommandSpec<?, ?>> commandSpecs = aggregateSetSpec
                .aggregateConfigMap()
                .values()
                .stream()
                .map(aggregateSpec -> SpecUtils.getCommandSpec(aggregateSpec, clientId));

        return EventSourcedClient.getCommandAPISet(commandSpecs, aggregateSetSpec.kafkaConfig(), scheduler);
    }}
