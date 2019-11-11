package io.simplesource.kafka.dsl;


import io.simplesource.api.CommandAPI;
import io.simplesource.kafka.internal.client.KafkaCommandAPI;
import io.simplesource.kafka.internal.util.NamedThreadFactory;
import io.simplesource.kafka.spec.CommandSpec;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public final class EventSourcedClient {
    private KafkaConfig kafkaConfig;
    private Map<String, CommandSpec<?, ?>> commandConfigMap = new HashMap<>();
    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("EventSourcedClient-scheduler"));

    public EventSourcedClient withKafkaConfig(
            final Function<? super KafkaConfig.Builder, KafkaConfig> builderFn) {
        KafkaConfig.Builder builder = new KafkaConfig.Builder() {
            @Override
            public KafkaConfig build() {
                return super.build(true);
            }
        };
        kafkaConfig = builderFn.apply(builder);
        return this;
    }

    public EventSourcedClient withScheduler(final ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
        return this;
    }

    CommandAPI<?, ?> createCommandApi(final Consumer<CommandAPIBuilder<?, ?>> buildSteps) {
        CommandAPIBuilder<?, ?> builder = CommandAPIBuilder.newBuilder();
        buildSteps.accept(builder);
        final CommandSpec<?, ?> commandSpec = builder.build();
        return createCommandApi(commandSpec);
    }

    CommandAPI<?, ?> createCommandApi(final CommandSpec<?, ?> commandSpec) {
        requireNonNull(scheduler, "Scheduler has not been defined. Please define with with 'withScheduler' method.");

        return new KafkaCommandAPI(
                        commandSpec,
                        kafkaConfig,
                        scheduler);
    }
}
