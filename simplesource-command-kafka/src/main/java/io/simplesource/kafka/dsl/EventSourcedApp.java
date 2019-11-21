package io.simplesource.kafka.dsl;

import io.simplesource.api.CommandAPI;
import io.simplesource.kafka.internal.client.KafkaCommandAPI;
import io.simplesource.kafka.internal.streams.EventSourcedStreamsApp;
import io.simplesource.kafka.internal.util.NamedThreadFactory;
import io.simplesource.kafka.spec.AggregateSetSpec;
import io.simplesource.kafka.spec.AggregateSpec;
import io.simplesource.kafka.spec.CommandSpec;
import io.simplesource.kafka.util.SpecUtils;
import org.apache.kafka.streams.errors.StreamsException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public final class EventSourcedApp {
    private KafkaConfig kafkaConfig;
    private Map<String, AggregateSpec<?, ?, ?, ?>> aggregateConfigMap = new HashMap<>();
    private boolean isStarted = false;

    public EventSourcedApp withKafkaConfig(
            final Function<KafkaConfig.Builder, KafkaConfig> builder) {
        kafkaConfig = builder.apply(new KafkaConfig.Builder());
        return this;
    }

    public EventSourcedApp withKafkaConfig(final KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        return this;
    }

    public <K, C, E, A> EventSourcedApp withAggregate(
            final Function<AggregateBuilder<K, C, E, A>, AggregateSpec<K, C, E, A>> buildSteps) {
        AggregateBuilder<K, C, E, A> builder = AggregateBuilder.newBuilder();
        final AggregateSpec<K, C, E, A> spec = buildSteps.apply(builder);
        aggregateConfigMap.put(spec.aggregateName(), spec);
        return this;
    }

    public <K, C, E, A> EventSourcedApp withAggregate(final AggregateSpec<K, C, E, A> spec) {
        aggregateConfigMap.put(spec.aggregateName(), spec);
        return this;
    }

    public <K, E> EventSourcedApp withEvent(
            final Consumer<EventAggregateBuilder<K, E>> buildSteps) {
        EventAggregateBuilder<K, E> builder = EventAggregateBuilder.newBuilder();
        buildSteps.accept(builder);
        final AggregateSpec<K, E, E, ?> spec = builder.build();
        aggregateConfigMap.put(spec.aggregateName(), spec);
        return this;
    }

    public <K, E> EventSourcedApp withEvent(final AggregateSpec<K, E, E, ?> spec) {
        aggregateConfigMap.put(spec.aggregateName(), spec);
        return this;
    }

    public void start() {
        if (isStarted) return;
        requireNonNull(kafkaConfig, "KafkaConfig has not been defined. Please define it with 'withKafkaConfig' method.");

        final AggregateSetSpec aggregateSetSpec = new AggregateSetSpec(
                kafkaConfig,
                aggregateConfigMap);

        EventSourcedStreamsApp app = new EventSourcedStreamsApp(aggregateSetSpec);
        app.start();
        isStarted = true;
    }
}
