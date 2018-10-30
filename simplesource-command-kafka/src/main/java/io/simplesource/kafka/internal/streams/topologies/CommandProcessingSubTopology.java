package io.simplesource.kafka.internal.streams.topologies;

import io.simplesource.kafka.api.AggregateResources.StateStoreEntity;
import io.simplesource.kafka.api.AggregateResources.TopicEntity;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.ValueWithSequence;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;

final class CommandProcessingSubTopology<K, C, E, A> {
    private static final Logger logger = LoggerFactory.getLogger(CommandProcessingSubTopology.class);
    private final AggregateTopologyContext<K, C, E, A> aggregateTopologyContext;
    private final ValueTransformerWithKey<K, CommandRequest<C>, CommandEvents<E, A>> commandRequestTransformer;

    CommandProcessingSubTopology(AggregateTopologyContext<K, C, E, A> aggregateTopologyContext,
                                 ValueTransformerWithKey<K, CommandRequest<C>, CommandEvents<E, A>> commandRequestTransformer) {
        this.aggregateTopologyContext = aggregateTopologyContext;
        this.commandRequestTransformer = commandRequestTransformer;
    }

    KStream<K, CommandEvents<E, A>> add(final KStream<K, CommandRequest<C>> requestStream) {
        KStream<K, CommandEvents<E, A>> commandEventsStream = eventResultStream(requestStream);
        publishEvents(commandEventsStream);
        return commandEventsStream;
    }

    private KStream<K, CommandEvents<E, A>> eventResultStream(final KStream<K, CommandRequest<C>> requestStream) {
        return requestStream.transformValues(() -> commandRequestTransformer, aggregateTopologyContext.stateStoreName(StateStoreEntity.aggregate_update));
    }

    private void publishEvents(final KStream<K, CommandEvents<E, A>> eventResultStream) {
        KStream<K, ValueWithSequence<E>> eventStream = eventResultStream
                .flatMapValues(result -> result.eventValue()
                        .fold(reasons -> Collections.emptyList(), ArrayList::new));

        String topicName = aggregateTopologyContext.topicName(TopicEntity.event);
        if (logger.isDebugEnabled()) {
            eventStream = eventStream.peek((k, v) -> logger.debug("Writing event ({},{}) to {}", k, v, topicName));
        }
        eventStream.to(topicName, aggregateTopologyContext.eventsConsumedProduced());
    }
}
