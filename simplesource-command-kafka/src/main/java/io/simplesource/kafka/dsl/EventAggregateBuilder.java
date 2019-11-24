package io.simplesource.kafka.dsl;

import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.kafka.api.AggregateResources.TopicEntity;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.api.EventSerdes;
import io.simplesource.kafka.api.ResourceNamingStrategy;
import io.simplesource.kafka.spec.AggregateSpec;
import io.simplesource.kafka.spec.TopicSpec;

import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public final class EventAggregateBuilder<K, E> {
    private AggregateBuilder<K, E, E, Boolean> builder;

    public static <K, E> EventAggregateBuilder<K, E> newBuilder() {
        return new EventAggregateBuilder<>();
    }

    private EventAggregateBuilder() {
        builder = AggregateBuilder.newBuilder();
        builder.withInitialValue(k -> true);
        builder.withCommandHandler((k, a, c) -> Result.success(NonEmptyList.of(c)));
        builder.withAggregator((x, e) -> true);
    }

    private EventAggregateBuilder<K, E> apply(Consumer<AggregateBuilder<K, E, E, Boolean>> applyStep) {
        applyStep.accept(builder);
        return this;
    }

    public EventAggregateBuilder<K, E> withName(final String name) {
        return apply(b -> b.withName(name));
    }

    public EventAggregateBuilder<K, E> withResourceNamingStrategy(final ResourceNamingStrategy resourceNamingStrategy) {
        return apply(b -> b.withResourceNamingStrategy(resourceNamingStrategy));
    }

    public EventAggregateBuilder<K, E> withSerdes(final EventSerdes<K, E> eventSerdes) {
        return apply(b -> b.withSerdes(eventSerdes));
    }

    public EventAggregateBuilder<K, E> withDefaultTopicSpec(final int partitions, final int replication, final int retentionDays) {
        return apply(b -> b.withDefaultTopicSpec(partitions, replication, retentionDays));
    }

    public EventAggregateBuilder<K, E> withTopicSpec(final TopicEntity topicEntity, final TopicSpec topicSpec) {
        return apply(b -> b.withTopicSpec(topicEntity, topicSpec));
    }

    public EventAggregateBuilder<K, E> withCommandResponseRetention(final long retentionInSeconds) {
        return apply(b -> b.withCommandResponseRetention(retentionInSeconds));
    }

    public EventAggregateBuilder<K, E> withInvalidSequenceStrategy(final InvalidSequenceStrategy invalidSequenceStrategy) {
        return apply(b -> b.withInvalidSequenceStrategy(invalidSequenceStrategy));
    }

    public AggregateSpec<K, E, E, Boolean> build() {
        return builder.build();
    }
}
