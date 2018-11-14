package io.simplesource.kafka.dsl;

import io.simplesource.api.CommandAPISet;
import io.simplesource.api.CommandAPI;
import io.simplesource.kafka.internal.client.KafkaCommandAPI;
import io.simplesource.kafka.internal.streams.EventSourcedStreamsApp;
import io.simplesource.kafka.spec.AggregateSetSpec;
import io.simplesource.kafka.spec.AggregateSpec;
import io.simplesource.kafka.spec.CommandSpec;
import org.apache.kafka.streams.KeyValue;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    public CommandAPISet build() {
        final AggregateSetSpec aggregateSetSpec = new AggregateSetSpec(
                kafkaConfig,
                aggregateConfigMap);

        final EventSourcedStreamsApp app =
                new EventSourcedStreamsApp(aggregateSetSpec);

        app.start();
        return getCommandAPISet(aggregateSetSpec);

    }

    private static CommandAPISet getCommandAPISet(AggregateSetSpec aggregateSetSpec) {
        //Hack to get round circular ref
        final CommandAPISet[] aggregatesRef = new CommandAPISet[1];

        final Map<String, CommandAPI<?, ?>> commandApis = aggregateSetSpec.aggregateConfigMap().values()
                .stream()
                .map(AggregateSpec::getCommandSpec)
                .map(createCommandApi(aggregateSetSpec.kafkaConfig()))
                .collect(Collectors.toMap(kv -> kv.key, kv -> kv.value));

        CommandAPISet commandAPISet = new CommandAPISet() {
            @Override
            public <K, C> CommandAPI<K, C> getCommandAPI(final String aggregateName) {
                return (CommandAPI<K, C>) commandApis.get(aggregateName);
            }
        };

        aggregatesRef[0] = commandAPISet;
        return commandAPISet;
    }

    private static Function<CommandSpec<?, ?>, KeyValue<String, CommandAPI<?, ?>>> createCommandApi(
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
