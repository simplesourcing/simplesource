package io.simplesource.kafka.dsl;


import io.simplesource.api.CommandAPI;
import io.simplesource.api.CommandAPISet;
import io.simplesource.kafka.internal.client.KafkaCommandAPI;
import io.simplesource.kafka.internal.util.NamedThreadFactory;
import io.simplesource.kafka.spec.CommandSetSpec;
import io.simplesource.kafka.spec.CommandSpec;
import org.apache.kafka.streams.KeyValue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public final class EventSourcedClient {
    private KafkaConfig kafkaConfig;
    private Map<String, CommandSpec<?, ?>> commandConfigMap = new HashMap<>();
    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("EventSourcedClient-scheduler"));;

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

    public <K, C> EventSourcedClient addCommands(
            final Consumer<CommandApiBuilder<K, C>> buildSteps) {
        CommandApiBuilder<K, C> builder = CommandApiBuilder.newBuilder();
        buildSteps.accept(builder);
        final CommandSpec<K, C> spec = builder.build();
        commandConfigMap.put(spec.aggregateName(), spec);
        return this;
    }

    public <K, C> EventSourcedClient addCommands(final CommandSpec<K, C> spec) {
        commandConfigMap.put(spec.aggregateName(), spec);
        return this;
    }

    public EventSourcedClient withScheduler(final ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
        return this;
    }

    public CommandAPISet build() {
        requireNonNull(scheduler, "Scheduler has not been defined. Please define with with 'withScheduler' method.");
        final CommandSetSpec commandSetSpec = new CommandSetSpec(
                kafkaConfig,
                commandConfigMap);

        Stream<CommandSpec<?, ?>> commandSpecs = commandSetSpec
                .commandConfigMap()
                .values()
                .stream();

        return getCommandAPISet(commandSpecs, commandSetSpec.kafkaConfig(), scheduler);
    }

    static CommandAPISet getCommandAPISet(
            Stream<CommandSpec<?, ?>> commandSpecStream,
            KafkaConfig kafkaConfig,
            final ScheduledExecutorService scheduler) {
        final Map<String, CommandAPI<?, ?>> commandApis = commandSpecStream
                .map(createCommandApi(kafkaConfig, scheduler))
                .collect(Collectors.toMap(kv -> kv.key, kv -> kv.value));

        return new CommandAPISet() {
            @Override
            public <K, C> CommandAPI<K, C> getCommandAPI(final String aggregateName) {
                return (CommandAPI<K, C>) commandApis.get(aggregateName);
            }
        };
    }

    static Function<CommandSpec<?, ?>, KeyValue<String, CommandAPI<?, ?>>> createCommandApi(
            final KafkaConfig kafkaConfig,
            final ScheduledExecutorService scheduler
    ) {
        return commandSpec -> {
            final CommandAPI commandAPI =
                    new KafkaCommandAPI(
                            commandSpec,
                            kafkaConfig,
                            scheduler);

            return KeyValue.pair(commandSpec.aggregateName(), commandAPI);
        };
    }
}


