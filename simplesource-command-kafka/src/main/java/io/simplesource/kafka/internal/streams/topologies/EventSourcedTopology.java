package io.simplesource.kafka.internal.streams.topologies;

import io.simplesource.kafka.api.AggregateResources.StateStoreEntity;
import io.simplesource.kafka.api.AggregateResources.TopicEntity;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.AggregateUpdateResult;
import io.simplesource.kafka.model.CommandRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;

/**
 * @param <A> the aggregate aggregate_update
 * @param <E> all events generated for this aggregate
 * @param <C> all commands for this aggregate
 * @param <K> the aggregate key
 */
public final class EventSourcedTopology<K, C, E, A> {
    private final AggregateTopologyContext<K, C, E, A> aggregateTopologyContext;
    private final CommandProcessingSubTopology<K, C, E, A> commandProcessingSubTopology;
    private final EventProcessingSubTopology<K, E, A> eventProcessingSubTopology;
    private final AggregateUpdatePublisher<K, C, E, A> aggregateUpdatePublisher;

    public EventSourcedTopology(final AggregateTopologyContext<K, C, E, A> aggregateTopologyContext) {
        this(aggregateTopologyContext, new CommandProcessingSubTopology<>(aggregateTopologyContext,
                new CommandRequestTransformer<>(aggregateTopologyContext)),
                new EventProcessingSubTopology<>(aggregateTopologyContext),
                new AggregateUpdatePublisher<>(aggregateTopologyContext)
        );
    }

    EventSourcedTopology(final AggregateTopologyContext<K, C, E, A> aggregateTopologyContext,
                         CommandProcessingSubTopology<K, C, E, A> commandProcessingSubTopology,
                         EventProcessingSubTopology<K, E, A> eventProcessingSubTopology,
                         AggregateUpdatePublisher<K, C, E, A> aggregateUpdatePublisher) {
        this.aggregateTopologyContext = aggregateTopologyContext;
        this.commandProcessingSubTopology = commandProcessingSubTopology;
        this.eventProcessingSubTopology = eventProcessingSubTopology;
        this.aggregateUpdatePublisher = aggregateUpdatePublisher;
    }

    public void addTopology(final StreamsBuilder builder) {
        addStateStores(builder);

        final KStream<K, CommandRequest<C>> requestStream = builder.stream(
                aggregateTopologyContext.topicName(TopicEntity.command_request), aggregateTopologyContext.commandEventsConsumed());

        final KStream<K, CommandEvents<E, A>> eventResultStream = commandProcessingSubTopology.add(requestStream);
        final KStream<K, AggregateUpdateResult<A>> aggregateUpdateStream = eventProcessingSubTopology.add(eventResultStream);

        aggregateUpdatePublisher.toAggregateStore(aggregateUpdateStream);
        aggregateUpdatePublisher.toCommandResultStore(aggregateUpdateStream);
        aggregateUpdatePublisher.toCommandResponseTopic(aggregateUpdateStream);
    }

    private void addStateStores(final StreamsBuilder builder) {
        final KeyValueStoreBuilder<K, AggregateUpdate<A>> aggregateStoreBuilder = new KeyValueStoreBuilder<>(
                persistentKeyValueStore(aggregateTopologyContext.stateStoreName(StateStoreEntity.aggregate_update)),
                aggregateTopologyContext.serdes().aggregateKey(),
                aggregateTopologyContext.serdes().aggregateUpdate(),
                Time.SYSTEM);
        builder.addStateStore(aggregateStoreBuilder);
    }
}
