package io.simplesource.kafka.dsl;


import io.simplesource.api.CommandAPI;
import io.simplesource.api.CommandAPISet;
import io.simplesource.kafka.internal.client.KafkaCommandAPI;
import io.simplesource.kafka.internal.streams.EventSourcedStreamsApp;
import io.simplesource.kafka.spec.CommandSetSpec;
import io.simplesource.kafka.spec.CommandSpec;
import org.apache.kafka.streams.KeyValue;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class CommandApiSetBuilder {
    private KafkaConfig kafkaConfig;
    private Map<String, CommandSpec<?, ?>> commandConfigMap = new HashMap<>();

    public CommandApiSetBuilder withKafkaConfig(
            final Function<KafkaConfig.Builder, KafkaConfig> builder) {
        kafkaConfig = builder.apply(new KafkaConfig.Builder());
        return this;
    }

    public CommandApiSetBuilder withKafkaConfig(final KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        return this;
    }

    public <K, C> CommandApiSetBuilder addCommands(
            final Function<CommandApiBuilder<K, C>, CommandSpec<K, C>> builder) {
        final CommandSpec<K, C> spec = builder.apply(CommandApiBuilder.newBuilder());
        commandConfigMap.put(spec.aggregateName(), spec);
        return this;
    }

    public <K, C> CommandApiSetBuilder addCommands(final CommandSpec<K, C> spec) {
        commandConfigMap.put(spec.aggregateName(), spec);
        return this;
    }

    public CommandAPISet build() {
        final CommandSetSpec commandSetSpec = new CommandSetSpec(
                kafkaConfig,
                commandConfigMap);

        Stream<CommandSpec<?, ?>> commandSpecs = commandSetSpec
                .commandConfigMap()
                .values()
                .stream();

        return getCommandAPISet(commandSpecs, commandSetSpec.kafkaConfig());
    }

    static CommandAPISet getCommandAPISet(Stream<CommandSpec<?, ?>> commandSpecStream, KafkaConfig kafkaConfig) {
        final Map<String, CommandAPI<?, ?>> commandApis = commandSpecStream
                .map(createCommandApi(kafkaConfig))
                .collect(Collectors.toMap(kv -> kv.key, kv -> kv.value));

        return new CommandAPISet() {
            @Override
            public <K, C> CommandAPI<K, C> getCommandAPI(final String aggregateName) {
                return (CommandAPI<K, C>) commandApis.get(aggregateName);
            }
        };
    }

    static Function<CommandSpec<?, ?>, KeyValue<String, CommandAPI<?, ?>>> createCommandApi(
            final KafkaConfig kafkaConfig
    ) {
        return commandSpec -> {
            final CommandAPI commandAPI =
                    new KafkaCommandAPI(
                            commandSpec,
                            kafkaConfig);

            return KeyValue.pair(commandSpec.aggregateName(), commandAPI);
        };
    }
}


