package io.simplesource.kafka.internal.streams.topologies;

import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.ValueWithSequence;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;

import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.aggregate_update;
import static io.simplesource.kafka.api.AggregateResources.TopicEntity.event;

final class CommandProcessingSubTopology<K, C, E, A> {
    private static final Logger logger = LoggerFactory.getLogger(CommandProcessingSubTopology.class);

    private final AggregateStreamResourceNames aggregateResourceNames;
    private final ValueTransformerWithKey<K, CommandRequest<C>, CommandEvents<E, A>> commandRequestTransformer;
    private final Produced<K, ValueWithSequence<E>> eventsConsumedProduced;

    CommandProcessingSubTopology(AggregateStreamResourceNames aggregateResourceNames,
                                 ValueTransformerWithKey<K, CommandRequest<C>, CommandEvents<E, A>> commandRequestTransformer,
                                 Serde<K> keySerde, Serde<ValueWithSequence<E>> valueWithSequenceSerde) {

        this.aggregateResourceNames = aggregateResourceNames;
        this.commandRequestTransformer = commandRequestTransformer;

        eventsConsumedProduced = Produced.with(keySerde, valueWithSequenceSerde);
    }

    KStream<K, CommandEvents<E, A>> add(final KStream<K, CommandRequest<C>> requestStream) {
        KStream<K, CommandEvents<E, A>> commandEventsStream = eventResultStream(requestStream);
        publishEvents(commandEventsStream);
        return commandEventsStream;
    }

    private KStream<K, CommandEvents<E, A>> eventResultStream(final KStream<K, CommandRequest<C>> requestStream) {
        return requestStream.transformValues(() -> commandRequestTransformer, aggregateResourceNames.stateStoreName(aggregate_update));
    }

    private void publishEvents(final KStream<K, CommandEvents<E, A>> eventResultStream) {
        KStream<K, ValueWithSequence<E>> eventStream = eventResultStream
                .flatMapValues(result -> result.eventValue()
                        .fold(reasons -> Collections.emptyList(), ArrayList::new));

        String topicName = aggregateResourceNames.topicName(event);
        if (logger.isDebugEnabled()) {
            eventStream = eventStream.peek((k, v) -> logger.debug("Writing event ({},{}) to {}", k, v, topicName));
        }
        eventStream.to(topicName, eventsConsumedProduced);
    }
}
