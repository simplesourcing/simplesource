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
    private final TopologyContext<K, C, E, A> context;
    private final CommandProcessingSubTopology<K, C, E, A> commandProcessingSubTopology;
    private final EventProcessingSubTopology<K, E, A> eventProcessingSubTopology;
    private final AggregateUpdatePublisher<K, C, E, A> aggregateUpdatePublisher;

    public EventSourcedTopology(final TopologyContext<K, C, E, A> context) {
        this(context, new CommandProcessingSubTopology<>(context,
                new CommandRequestTransformer<>(context)),
                new EventProcessingSubTopology<>(context),
                new AggregateUpdatePublisher<>(context)
        );
    }

    EventSourcedTopology(final TopologyContext<K, C, E, A> context,
                         CommandProcessingSubTopology<K, C, E, A> commandProcessingSubTopology,
                         EventProcessingSubTopology<K, E, A> eventProcessingSubTopology,
                         AggregateUpdatePublisher<K, C, E, A> aggregateUpdatePublisher) {
        this.context = context;
        this.commandProcessingSubTopology = commandProcessingSubTopology;
        this.eventProcessingSubTopology = eventProcessingSubTopology;
        this.aggregateUpdatePublisher = aggregateUpdatePublisher;
    }

    public void addTopology(final StreamsBuilder builder) {
        addStateStores(builder);

        final KStream<K, CommandRequest<C>> requestStream = builder.stream(
                context.topicName(TopicEntity.command_request), context.commandEventsConsumed());

        final KStream<K, CommandEvents<E, A>> eventResultStream = commandProcessingSubTopology.add(requestStream);
        final KStream<K, AggregateUpdateResult<A>> aggregateUpdateStream = eventProcessingSubTopology.add(eventResultStream);

        aggregateUpdatePublisher.toAggregateStore(aggregateUpdateStream);
        aggregateUpdatePublisher.toCommandResultStore(aggregateUpdateStream);
        aggregateUpdatePublisher.toCommandResponseTopic(aggregateUpdateStream);
    }

    private void addStateStores(final StreamsBuilder builder) {
        final KeyValueStoreBuilder<K, AggregateUpdate<A>> aggregateStoreBuilder = new KeyValueStoreBuilder<>(
                persistentKeyValueStore(context.stateStoreName(StateStoreEntity.aggregate_update)),
                context.serdes().aggregateKey(),
                context.serdes().aggregateUpdate(),
                Time.SYSTEM);
        builder.addStateStore(aggregateStoreBuilder);
    }
}
