package io.simplesource.kafka.dsl;

import io.simplesource.api.CommandAPISet;
import io.simplesource.api.CommandAPI;
import io.simplesource.kafka.api.RemoteCommandResponseStore;
import io.simplesource.kafka.internal.KafkaCommandAPI;
import io.simplesource.kafka.internal.cluster.ClusterSubsystem;
import io.simplesource.kafka.internal.streams.EventSourcedStreamsApp;
import io.simplesource.kafka.internal.streams.statestore.CommandResponseStoreBridge;
import io.simplesource.kafka.internal.streams.statestore.KafkaStreamCommandResponseStoreBridge;
import io.simplesource.kafka.internal.streams.statestore.KafkaStreamAggregateStoreBridge;
import io.simplesource.kafka.internal.streams.statestore.AggregateStoreBridge;
import io.simplesource.kafka.internal.util.NamedThreadFactory;
import io.simplesource.kafka.spec.AggregateSetSpec;
import io.simplesource.kafka.spec.AggregateSpec;
import io.simplesource.kafka.spec.KafkaExecutionSpec;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class AggregateSetBuilder {
    private ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("SimpleSourcingKafkaBuilder-scheduler"));
    private KafkaConfig kafkaConfig;
    private Map<String, AggregateSpec<?, ?, ?, ?>> aggregateConfigMap = new HashMap<>();

    private static Function<AggregateSpec<?, ?, ?, ?>, KeyValue<String, CommandAPI<?>>> createAggregate(
            final KafkaConfig kafkaConfig,
            final KafkaStreams kafkaStreams,
            final RemoteCommandResponseStore remoteCommandResponseStore,
            final ScheduledExecutorService scheduledExecutor
    ) {
        return aggregateSpec -> {
            final CommandResponseStoreBridge<?> commandResponseStoreBridge = new KafkaStreamCommandResponseStoreBridge<>(
                    aggregateSpec,
                    kafkaStreams);
            final CommandAPI commandAPI = new KafkaCommandAPI(
                    aggregateSpec,
                    kafkaConfig,
                    commandResponseStoreBridge,
                    remoteCommandResponseStore,
                    scheduledExecutor,
                    aggregateSpec.generation().retryDelay());

            final AggregateStoreBridge<?, ?> aggregateStoreBridge = new KafkaStreamAggregateStoreBridge<>(
                    aggregateSpec, kafkaStreams);

            return KeyValue.pair(aggregateSpec.aggregateName(), commandAPI);
        };
    }

    public AggregateSetBuilder withScheduledExecutorService(final ScheduledExecutorService scheduledExecutor) {
        this.scheduledExecutor = scheduledExecutor;
        return this;
    }

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
                new KafkaExecutionSpec(scheduledExecutor,
                kafkaConfig),
                aggregateConfigMap);

        final EventSourcedStreamsApp app =
                new EventSourcedStreamsApp(aggregateSetSpec);


        app.start();
        final KafkaStreams kafkaStreams = app.getStreams();


        //Hack to get round circular ref
        final CommandAPISet[] aggregatesRef = new CommandAPISet[1];

        final ClusterSubsystem clusterSubsystem =
                new ClusterSubsystem((aggName) -> aggregatesRef[0].getCommandAPI(aggName), kafkaConfig.clusterConfig(), scheduledExecutor);

        final Map<String, CommandAPI<?>> aggregates = aggregateSetSpec.aggregateConfigMap().values()
                .stream()
                .map(createAggregate(
                        aggregateSetSpec.executionSpec().kafkaConfig(),
                        kafkaStreams,
                        clusterSubsystem,
                        aggregateSetSpec.executionSpec().scheduledExecutor()
                ))
                .collect(Collectors.toMap(kv -> kv.key, kv -> kv.value));

        CommandAPISet commandAPISet = new CommandAPISet() {
            @Override
            public <C> CommandAPI<C> getCommandAPI(final String aggregateName) {
                return (CommandAPI<C>) aggregates.get(aggregateName);
            }
        };

        aggregatesRef[0] = commandAPISet;
        clusterSubsystem.start();

        return commandAPISet;
    }

}
