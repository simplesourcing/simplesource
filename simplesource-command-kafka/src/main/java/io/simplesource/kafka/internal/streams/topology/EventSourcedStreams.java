package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.api.CommandError;
import io.simplesource.data.Result;
import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.kafka.model.*;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Serialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;
import java.util.function.BiFunction;

final class EventSourcedStreams {
    private static final Logger logger = LoggerFactory.getLogger(EventSourcedStreams.class);

    private static <K> long getResponseSequence(CommandResponse response) {
        return response.sequenceResult().getOrElse(response.readSequence()).getSeq();
    }

    static <K, C, E, A> Tuple2<KStream<K, CommandRequest<K, C>>, KStream<K, CommandResponse>> getProcessedCommands(
            TopologyContext<K, C, E, A> ctx,
            final KStream<K, CommandRequest<K, C>> commandRequestStream,
            final KStream<K, CommandResponse> commandResponseStream) {

        final KTable<UUID, CommandResponse> commandResponseById = commandResponseStream
                .selectKey((key, response) -> response.commandId())
                .groupByKey(Serialized.with(ctx.serdes().commandResponseKey(), ctx.serdes().commandResponse()))
                .reduce((r1, r2) -> getResponseSequence(r1) > getResponseSequence(r2) ? r1 : r2);

        final KStream<K, Tuple2<CommandRequest<K, C>, CommandResponse>> reqResp = commandRequestStream
                .selectKey((k, v) -> v.commandId())
                .leftJoin(commandResponseById, Tuple2::new, Joined.with(ctx.serdes().commandResponseKey(), ctx.serdes().commandRequest(), ctx.serdes().commandResponse()))
                .selectKey((k, v) -> v.v1().aggregateKey());

        KStream<K, Tuple2<CommandRequest<K, C>, CommandResponse>>[] branches =
                reqResp.branch((k, tuple) -> tuple.v2() == null, (k, tuple) -> tuple.v2() != null);

        KStream<K, CommandRequest<K, C>> unProcessed = branches[0].mapValues((k, tuple) -> tuple.v1());

        KStream<K, CommandResponse> processed = branches[1].mapValues((k, tuple) -> tuple.v2())
                .peek((k, r) -> logger.info("Preprocessed: {}=CommandId:{}", k, r.commandId()));

        return new Tuple2<>(unProcessed, processed);
    }

    static <K, C, E, A> KStream<K, CommandEvents<E, A>> getCommandEvents(
            TopologyContext<K, C, E, A> ctx,
            final KStream<K, CommandRequest<K, C>> commandRequestStream,
            final KTable<K, AggregateUpdate<A>> aggregateTable) {
        return commandRequestStream.leftJoin(aggregateTable, (r, a) -> CommandRequestTransformer.getCommandEvents(ctx, a, r),
                Joined.with(ctx.serdes().aggregateKey(),
                        ctx.serdes().commandRequest(),
                        ctx.serdes().aggregateUpdate()));
    }

    static <K, E, A> KStream<K, ValueWithSequence<E>> getEventsWithSequence(final KStream<K, CommandEvents<E, A>> eventResultStream) {
        return eventResultStream.flatMapValues(result -> result.eventValue()
                .fold(reasons -> Collections.emptyList(), ArrayList::new));
    }

    static <K, E, A> KStream<K, AggregateUpdateResult<A>> getAggregateUpdateResults(
            TopologyContext<K, ?, E, A> ctx,
            final KStream<K, CommandEvents<E, A>> eventResultStream) {
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
