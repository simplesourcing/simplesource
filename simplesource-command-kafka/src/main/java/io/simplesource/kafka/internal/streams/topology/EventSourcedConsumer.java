package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import static io.simplesource.kafka.api.AggregateResources.TopicEntity.aggregate;
import static io.simplesource.kafka.api.AggregateResources.TopicEntity.command_request;
import static io.simplesource.kafka.api.AggregateResources.TopicEntity.command_response;

final class EventSourcedConsumer {

    static <K, C> KStream<K, CommandRequest<K, C>> commandRequestStream(TopologyContext<K, C, ?, ?> ctx, final StreamsBuilder builder) {
        return builder.stream(ctx.topicName(command_request), ctx.commandRequestConsumed());
    }

    static <K, A> KTable<K, AggregateUpdate<A>> aggregateTable(TopologyContext<K, ?, ?, A> ctx, final StreamsBuilder builder) {
        return builder.table(ctx.topicName(aggregate), Consumed.with(ctx.serdes().aggregateKey(), ctx.serdes().aggregateUpdate()));
    }

    static <K, C> KStream<K, CommandResponse> commandResponseStream(TopologyContext<K, C, ?, ?> ctx, final StreamsBuilder builder) {
        return builder.stream(ctx.topicName(command_response), ctx.commandResponseConsumed());
    }
}

