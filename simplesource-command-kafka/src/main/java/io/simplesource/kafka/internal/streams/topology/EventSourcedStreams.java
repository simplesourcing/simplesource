package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.api.CommandError;
import io.simplesource.data.Result;
import io.simplesource.kafka.internal.util.Tuple;
import io.simplesource.kafka.model.*;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;
import java.util.function.BiFunction;

import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.aggregate_update;

final class EventSourcedStreams {
    private static <K> long getResponseSequence(CommandResponse response) {
        return response.sequenceResult().getOrElse(response.readSequence()).getSeq();
    }

    static <K, C, E, A> KTable<UUID, CommandResponse> commandResponseTable(TopologyContext<K, C, E, A> ctx, final KStream<K, CommandResponse> commandResponseStream) {



        return null        ;
    }

    static <K, C, E, A> void getProcessedCommands(TopologyContext<K, C, E, A> ctx,
                                                  final KStream<K, CommandRequest<C>> commandRequestStream,
                                                  final KStream<K, CommandResponse> commandResponseStream) {

        KTable<UUID, CommandResponse> commandResponseById = commandResponseStream
                .selectKey((key, response) -> response.commandId())
                .groupByKey(Serialized.with(ctx.serdes().commandResponseKey(), ctx.serdes().commandResponse()))
                .reduce((r1, r2) -> getResponseSequence(r1) > getResponseSequence(r2) ? r1 : r2);

        KTable<K, CommandResponse> commandResponseByAggKey = commandResponseStream
                .groupByKey(Serialized.with(ctx.serdes().aggregateKey(), ctx.serdes().commandResponse()))
                .reduce((r1, r2) -> getResponseSequence(r1) > getResponseSequence(r2) ? r1 : r2);

        commandRequestStream
                .selectKey((k, v) -> v.commandId())
                .leftJoin(commandResponseById, Tuple::new, Joined.with(ctx.serdes().commandResponseKey(), ctx.serdes().commandRequest(), ctx.serdes().commandResponse()))
                .selectKey((k, v) -> v.v1());
    }



    static <K, C, E, A> KStream<K, CommandEvents<E, A>> eventResultStream(TopologyContext<K, C, E, A> ctx, final KStream<K, CommandRequest<C>> commandRequestStream) {
        return commandRequestStream
                .transformValues(() -> new CommandRequestTransformer<>(ctx), ctx.stateStoreName(aggregate_update));
    }

    static <K, E, A> KStream<K, ValueWithSequence<E>> getEventsWithSequence(final KStream<K, CommandEvents<E, A>> eventResultStream) {
        return eventResultStream.flatMapValues(result -> result.eventValue()
                .fold(reasons -> Collections.emptyList(), ArrayList::new));
    }

    static <K, E, A> KStream<K, AggregateUpdateResult<A>> getAggregateUpdateResults(TopologyContext<K, ?, E, A> ctx, final KStream<K, CommandEvents<E, A>> eventResultStream) {
        return eventResultStream
                .mapValues((serializedKey, result) -> {
                    final Result<CommandError, AggregateUpdate<A>> aggregateUpdateResult = result.eventValue().map(events -> {
                        final BiFunction<AggregateUpdate<A>, ValueWithSequence<E>, AggregateUpdate<A>> reducer =
                                (aggregateUpdate, eventWithSequence) -> new AggregateUpdate<>(
                                        ctx.aggregator().applyEvent(aggregateUpdate.aggregate(), eventWithSequence.value()),
                                        eventWithSequence.sequence()
                                );
                        return events.fold(
                                eventWithSequence -> new AggregateUpdate<>(
                                        ctx.aggregator().applyEvent(result.aggregate(), eventWithSequence.value()),
                                        eventWithSequence.sequence()
                                ),
                                reducer
                        );
                    });
                    return new AggregateUpdateResult<>(
                            result.commandId(),
                            result.readSequence(),
                            aggregateUpdateResult);
                });
    }

    static <K, A> KStream<K, AggregateUpdate<A>> getAggregateUpdates(final KStream<K, AggregateUpdateResult<A>> aggregateUpdateStream) {
        return aggregateUpdateStream
                .flatMapValues(update -> update.updatedAggregateResult().fold(
                        reasons -> Collections.emptyList(),
                        Collections::singletonList
                ));
    }

    static <K, A>  KStream<K, CommandResponse> getCommandResponses(final KStream<K, AggregateUpdateResult<A>> aggregateUpdateStream) {
        return aggregateUpdateStream
                .mapValues((key, update) ->
                        new CommandResponse(update.commandId(), update.readSequence(), update.updatedAggregateResult().map(AggregateUpdate::sequence))
                );
    }
}
