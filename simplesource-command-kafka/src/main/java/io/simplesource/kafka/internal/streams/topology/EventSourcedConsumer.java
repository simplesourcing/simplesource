package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.kafka.model.CommandRequest;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import static io.simplesource.kafka.api.AggregateResources.TopicEntity.command_request;

public final class EventSourcedConsumer {
    static <K, C, E, A> KStream<K, CommandRequest<C>> commandRequestStream(TopologyContext<K, C, E, A> ctx, final StreamsBuilder builder) {
        return builder.stream(ctx.topicName(command_request), ctx.commandEventsConsumed());
    }
}
