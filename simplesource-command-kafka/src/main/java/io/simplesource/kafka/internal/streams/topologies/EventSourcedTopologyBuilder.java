package io.simplesource.kafka.internal.streams.topologies;

import io.simplesource.data.NonEmptyList;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.spec.AggregateSpec;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public final class EventSourcedTopologyBuilder<K, C, E, A> {
    private static final long HOPPING_WINDOW_FACTOR = 3L;

    private final AggregateSpec<K, C, E, A> aggregateSpec;

    public EventSourcedTopologyBuilder(AggregateSpec<K, C, E, A> aggregateSpec) {
        this.aggregateSpec = aggregateSpec;
    }

    public EventSourcedTopology<K, C, E, A> build() {
        requireNonNull(aggregateSpec, "Aggregate spec is required");

        AggregateSerdes<K, C, E, A> serdes = aggregateSpec.serialization().serdes();
        final AggregateStreamResourceNames aggregateResourceNames = aggregateStreamResourceNames(aggregateSpec);

        final CommandProcessingSubTopology<K, C, E, A> commandProcessingSubTopology = commandProcessingTopology(aggregateResourceNames, serdes);
        final EventProcessingSubTopology<K, E, A> eventProcessingSubTopology = eventProcessingTopology(aggregateResourceNames, serdes);

        return new EventSourcedTopology<>(serdes, aggregateResourceNames, commandProcessingSubTopology, eventProcessingSubTopology,
                aggregateUpdateResultStreamConsumers(aggregateResourceNames, serdes));
    }

    private CommandProcessingSubTopology<K, C, E, A> commandProcessingTopology(final AggregateStreamResourceNames aggregateResourceNames,
                                                                               final AggregateSerdes<K, C, E, A> serdes) {
        CommandRequestTransformer<K, C, E, A> commandRequestTransformer = new CommandRequestTransformer<>(aggregateSpec.generation().initialValue(),
                aggregateSpec.generation().invalidSequenceHandler(), aggregateSpec.generation().commandHandler(), aggregateResourceNames);

        return new CommandProcessingSubTopology<>(aggregateResourceNames, commandRequestTransformer,
                serdes.aggregateKey(), serdes.valueWithSequence());
    }

    private EventProcessingSubTopology<K, E, A> eventProcessingTopology(final AggregateStreamResourceNames aggregateResourceNames,
                                                                        final AggregateSerdes<K, C, E, A> serdes) {
        return new EventProcessingSubTopology<>(aggregateResourceNames, aggregateSpec.generation().aggregator(),
                serdes.aggregateKey(), serdes.aggregateUpdate());
    }

    private AggregateStreamResourceNames aggregateStreamResourceNames(AggregateSpec<K, C, E, A> aggregateSpec) {
        return new AggregateStreamResourceNames(
                aggregateSpec.serialization().resourceNamingStrategy(), aggregateSpec.aggregateName());
    }

    private List<AggregateUpdateResultStreamConsumer<K, A>> aggregateUpdateResultStreamConsumers(final AggregateStreamResourceNames aggregateResourceNames,
                                                                                                 final AggregateSerdes<K, C, E, A> serdes) {
        final long retentionMillis = TimeUnit.SECONDS.toMillis(aggregateSpec.generation().stateStoreSpec().retentionInSeconds());
        return NonEmptyList.of(
                new AggregateStoreUpdateConsumer<>(aggregateResourceNames),
                new CommandResultStoreUpdateConsumer<>(aggregateResourceNames,
                        TimeWindows.of(retentionMillis).advanceBy(retentionMillis / HOPPING_WINDOW_FACTOR),
                        serdes.commandResponseKey(), serdes.updateResult()),
                new CommandResponsePublishConsumer<>(aggregateResourceNames, serdes.aggregateKey(),
                        serdes.commandResponse())
        );
    }
}
