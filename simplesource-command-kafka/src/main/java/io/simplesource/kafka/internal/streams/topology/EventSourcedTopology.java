package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.kafka.internal.util.Tuple;
import io.simplesource.kafka.model.*;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

public final class EventSourcedTopology {

    public static <K, C, E, A> void addTopology(TopologyContext<K, C, E, A> ctx, final StreamsBuilder builder) {
        // Create stores
        EventSourcedStores.addStateStores(ctx, builder);

        // Consume from topics
        final KStream<K, CommandRequest<K, C>> commandRequestStream = EventSourcedConsumer.commandRequestStream(ctx, builder);
        final KStream<K, CommandResponse> commandResponseStream = EventSourcedConsumer.commandResponseStream(ctx, builder);

        // Handle idempotence by splitting stream into processed and unprocessed
        Tuple<KStream<K, CommandRequest<K, C>>, KStream<K, CommandResponse>> reqResp = EventSourcedStreams.getProcessedCommands(
                ctx, commandRequestStream, commandResponseStream);
        KStream<K, CommandRequest<K, C>> unprocessedRequests = reqResp.v1();
        KStream<K, CommandResponse> processedResponses = reqResp.v2();
        
        // Transformations
        final KStream<K, CommandEvents<E, A>> eventResultStream = EventSourcedStreams.eventResultStream(ctx, unprocessedRequests);
        KStream<K, ValueWithSequence<E>> eventsWithSequence = EventSourcedStreams.getEventsWithSequence(eventResultStream);

        final KStream<K, AggregateUpdateResult<A>> aggregateUpdateResults = EventSourcedStreams.getAggregateUpdateResults(ctx, eventResultStream);
        final KStream<K, AggregateUpdate<A>> aggregateUpdates = EventSourcedStreams.getAggregateUpdates(aggregateUpdateResults);
        final KStream<K, CommandResponse>commandResponses = EventSourcedStreams.getCommandResponses(aggregateUpdateResults);

        // Produce to topics
        EventSourcedPublisher.publishEvents(ctx, eventsWithSequence);
        EventSourcedPublisher.publishAggregateUpdates(ctx, aggregateUpdates);
        EventSourcedPublisher.publishCommandResponses(ctx, processedResponses);
        EventSourcedPublisher.publishCommandResponses(ctx, commandResponses);

        // Update stores
        EventSourcedStores.updateAggregateStateStore(ctx, aggregateUpdateResults);
        EventSourcedStores.updateCommandResponseStore(ctx, commandResponses);
    }
}


