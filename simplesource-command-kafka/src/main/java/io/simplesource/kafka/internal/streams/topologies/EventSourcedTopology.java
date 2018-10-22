package io.simplesource.kafka.internal.streams.topologies;

import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.AggregateUpdateResult;
import io.simplesource.kafka.model.CommandRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.aggregate_update;
import static io.simplesource.kafka.api.AggregateResources.TopicEntity.command_request;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;

/**
 * @param <A> the aggregate aggregate_update
 * @param <E> all events generated for this aggregate
 * @param <C> all commands for this aggregate
 * @param <K> the aggregate key
 */
public final class EventSourcedTopology<K, C, E, A> {
    private static final Logger logger = LoggerFactory.getLogger(EventSourcedTopology.class);


    private final AggregateSerdes<K, C, E, A> serdes;
    private final AggregateStreamResourceNames aggregateResourceNames;
    private final CommandProcessingSubTopology<K, C, E, A> commandProcessingSubTopology;
    private final EventProcessingSubTopology<K, E, A> eventProcessingSubTopology;
    private final List<AggregateUpdateResultStreamConsumer<K, A>> aggregateUpdateResultStreamConsumers;

    EventSourcedTopology(AggregateSerdes<K, C, E, A> serdes, AggregateStreamResourceNames aggregateResourceNames,
                                CommandProcessingSubTopology<K, C, E, A> commandProcessingSubTopology,
                                EventProcessingSubTopology<K, E, A> eventProcessingSubTopology,
                                List<AggregateUpdateResultStreamConsumer<K, A>> aggregateUpdateResultStreamConsumers) {
        this.serdes = serdes;
        this.aggregateResourceNames = aggregateResourceNames;
        this.commandProcessingSubTopology = commandProcessingSubTopology;
        this.eventProcessingSubTopology = eventProcessingSubTopology;
        this.aggregateUpdateResultStreamConsumers = new ArrayList<>(aggregateUpdateResultStreamConsumers);
    }

    public void addTopology(final StreamsBuilder builder) {
        addStateStores(builder);

        final KStream<K, CommandRequest<C>> requestStream = builder.stream(
                aggregateResourceNames.topicName(command_request), Consumed.with(serdes.aggregateKey(), serdes.commandRequest()));

        final KStream<K, CommandEvents<E, A>> eventResultStream = commandProcessingSubTopology.add(requestStream);
        final KStream<K, AggregateUpdateResult<A>> aggregateUpdateStream = eventProcessingSubTopology.add(eventResultStream);

        aggregateUpdateResultStreamConsumers.forEach(p -> p.accept(aggregateUpdateStream));
    }

    private void addStateStores(final StreamsBuilder builder) {
        final KeyValueStoreBuilder<K, AggregateUpdate<A>> aggregateStoreBuilder = new KeyValueStoreBuilder<>(
                persistentKeyValueStore(aggregateResourceNames.stateStoreName(aggregate_update)),
                serdes.aggregateKey(),
                serdes.aggregateUpdate(),
                Time.SYSTEM);
        builder.addStateStore(aggregateStoreBuilder);
    }
}
