package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.model.ValueWithSequence;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.simplesource.kafka.api.AggregateResources.TopicEntity.AGGREGATE;
import static io.simplesource.kafka.api.AggregateResources.TopicEntity.EVENT;

final class EventSourcedPublisher {
    private static final Logger logger = LoggerFactory.getLogger(EventSourcedPublisher.class);

    static <K, C, E, A> void publishEvents(TopologyContext<K, C, E, A> ctx, final KStream<K, ValueWithSequence<E>> eventStream) {
        eventStream
                .peek((k, v) -> logger.debug("Writing event ({},{}) to {}", k, v, ctx.topicName(EVENT)))
                .to(ctx.topicName(EVENT), ctx.eventsConsumedProduced());
    }

    static <K, A> void publishAggregateUpdates(TopologyContext<K, ?, ?, A> ctx, final KStream<K, AggregateUpdate<A>> aggregateUpdateStream) {
        aggregateUpdateStream.to(ctx.topicName(AGGREGATE), ctx.aggregatedUpdateProduced());
    }

    static <K> void publishCommandResponses(TopologyContext<K, ?, ?, ?> ctx, final KStream<K, CommandResponse<K>> responseStream) {
        responseStream.to(ctx.topicName(AggregateResources.TopicEntity.COMMAND_RESPONSE), ctx.commandResponseProduced());
    }
}
