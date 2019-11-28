package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import static io.simplesource.kafka.api.AggregateResources.TopicEntity.AGGREGATE;
import static io.simplesource.kafka.api.AggregateResources.TopicEntity.COMMAND_REQUEST;
import static io.simplesource.kafka.api.AggregateResources.TopicEntity.COMMAND_RESPONSE;

final class EventSourcedConsumer {

    static <K, C> KStream<K, CommandRequest<K, C>> commandRequestStream(TopologyContext<K, C, ?, ?> ctx, final StreamsBuilder builder) {
        return builder.stream(ctx.topicName(COMMAND_REQUEST), ctx.commandRequestConsumed());
    }

    static <K, A> KTable<K, AggregateUpdate<A>> aggregateTable(TopologyContext<K, ?, ?, A> ctx, final StreamsBuilder builder) {
        return builder.table(ctx.topicName(AGGREGATE), Consumed.with(ctx.serdes().aggregateKey(), ctx.serdes().aggregateUpdate()));
    }

    static <K, C> KStream<K, CommandResponse<K>> commandResponseStream(TopologyContext<K, C, ?, ?> ctx, final StreamsBuilder builder) {
        return builder.stream(ctx.topicName(COMMAND_RESPONSE), ctx.commandResponseConsumed());
    }
}

