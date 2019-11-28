package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.api.CommandId;
import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.kafka.model.*;
import lombok.Value;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

public final class EventSourcedTopology {

    @Value
    static final class InputStreams<K, C> {
        public final KStream<K, CommandRequest<K, C>> commandRequest;
        public final KStream<K, CommandResponse<K>> commandResponse;
    }

    public static <K, C, E, A> InputStreams<K, C> addTopology(TopologyContext<K, C, E, A> ctx, final StreamsBuilder builder) {
        // Consume from topics
        final KStream<K, CommandRequest<K, C>> commandRequestStream = EventSourcedConsumer.commandRequestStream(ctx, builder);
        final KStream<K, CommandResponse<K>> commandResponseStream = EventSourcedConsumer.commandResponseStream(ctx, builder);
        final KTable<K, AggregateUpdate<A>> aggregateTable = EventSourcedConsumer.aggregateTable(ctx, builder);
        final DistributorContext<CommandId, CommandResponse<K>> distCtx = getDistributorContext(ctx);
        final KStream<CommandId, String> resultsTopicMapStream = ResultDistributor.resultTopicMapStream(distCtx,  builder);

        // Handle idempotence by splitting stream into processed and unprocessed
        Tuple2<KStream<K, CommandRequest<K, C>>, KStream<K, CommandResponse<K>>> reqResp = EventSourcedStreams.getProcessedCommands(
                ctx, commandRequestStream, commandResponseStream);
        final KStream<K, CommandRequest<K, C>> unprocessedRequests = reqResp.v1();
        final KStream<K, CommandResponse<K>> processedResponses = reqResp.v2();
        
        // Transformations
        final KStream<K, CommandEvents<E, A>> commandEvents = EventSourcedStreams.getCommandEvents(ctx, unprocessedRequests, aggregateTable);
        final KStream<K, ValueWithSequence<E>> eventsWithSequence = EventSourcedStreams.getEventsWithSequence(commandEvents);

        final KStream<K, AggregateUpdateResult<A>> aggregateUpdateResults = EventSourcedStreams.getAggregateUpdateResults(ctx, commandEvents);
        final KStream<K, AggregateUpdate<A>> aggregateUpdates = EventSourcedStreams.getAggregateUpdates(aggregateUpdateResults);
        final KStream<K, CommandResponse<K>>commandResponses = EventSourcedStreams.getCommandResponses(aggregateUpdateResults);

        // Produce to topics
        EventSourcedPublisher.publishEvents(ctx, eventsWithSequence);
        EventSourcedPublisher.publishAggregateUpdates(ctx, aggregateUpdates);
        EventSourcedPublisher.publishCommandResponses(ctx, processedResponses);
        EventSourcedPublisher.publishCommandResponses(ctx, commandResponses);

        // Distribute command results
        ResultDistributor.distribute(distCtx, commandResponseStream, resultsTopicMapStream);

        // return input streams
        return new InputStreams<>(commandRequestStream, commandResponseStream);
    }

    private static <K> DistributorContext<CommandId, CommandResponse<K>> getDistributorContext(TopologyContext<K, ?, ?, ?> ctx) {
        return new DistributorContext<>(
                ctx.aggregateSpec().serialization().resourceNamingStrategy().topicName(ctx.aggregateSpec().aggregateName(), AggregateResources.TopicEntity.COMMAND_RESPONSE_TOPIC_MAP.toString()),
                new DistributorSerdes<>(ctx.serdes().commandId(), ctx.serdes().commandResponse()),
                ctx.aggregateSpec().generation().stateStoreSpec(),
                CommandResponse::commandId, CommandId::id);
    }
}


