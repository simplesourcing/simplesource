package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.api.CommandError;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.kafka.model.*;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Serialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiFunction;

import static io.simplesource.data.Result.failure;
import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.aggregate_update;

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

        KStream<K, Tuple2<CommandRequest<K, C>, CommandResponse>>[] branches = reqResp.branch((k, tuple) -> tuple.v2() == null, (k, tuple) -> tuple.v2() != null);
        KStream<K, CommandRequest<K, C>> unProcessed = branches[0].mapValues((k, tuple) -> tuple.v1());

        KStream<K, CommandResponse> processed = branches[1].mapValues((k, tuple) -> tuple.v2())
                .peek((k, r) -> logger.info("Preprocessed: {}=CommandId:{}", k, r.commandId()));

        return new Tuple2<>(unProcessed, processed);
    }

    private static <K, C, E, A> CommandEvents<E, A> getCommandEvents(
            TopologyContext<K, C, E, A> ctx,
            final AggregateUpdate<A> currentUpdateInput, final CommandRequest<K, C> request) {

        final K readOnlyKey = request.aggregateKey();
        AggregateUpdate<A> currentUpdatePre;
        try {
            currentUpdatePre = Optional.ofNullable(currentUpdateInput)
                    .orElse(AggregateUpdate.of(ctx.initialValue().empty(readOnlyKey)));
        } catch (Exception e) {
            currentUpdatePre = AggregateUpdate.of(ctx.initialValue().empty(readOnlyKey));
        }
        final AggregateUpdate<A> currentUpdate = currentUpdatePre;

        Result<CommandError, NonEmptyList<E>> commandResult;
        try {
            Optional<CommandError> maybeReject =
                    Objects.equals(request.readSequence(), currentUpdate.sequence()) ? Optional.empty() :
                            ctx.aggregateSpec().generation().invalidSequenceHandler().shouldReject(
                                    readOnlyKey,
                                    currentUpdate.sequence(),
                                    request.readSequence(),
                                    currentUpdate.aggregate(),
                                    request.command());

            commandResult = maybeReject.<Result<CommandError, NonEmptyList<E>>>map(
                    commandErrorReason -> Result.failure(commandErrorReason)).orElseGet(
                    () -> ctx.aggregateSpec().generation().commandHandler().interpretCommand(
                            readOnlyKey,
                            currentUpdate.aggregate(),
                            request.command()));
        } catch (final Exception e) {
            logger.warn("[{} aggregate] Failed to apply command handler on key {} to request {}",
                    ctx.aggregateSpec().aggregateName(), readOnlyKey, request, e);
            commandResult = failure(CommandError.of(CommandError.Reason.CommandHandlerFailed, e));
        }
        final Result<CommandError, NonEmptyList<ValueWithSequence<E>>> eventsResult = commandResult.map(
                eventList -> {
                    // get round Java limitation of only using finals in lambdas by wrapping in an array
                    final Sequence[] eventSequence = {currentUpdate.sequence()};
                    return eventList.map(event -> {
                        eventSequence[0] = eventSequence[0].next();
                        return new ValueWithSequence<>(event, eventSequence[0]);
                    });
                });
        return new CommandEvents<>(
                request.commandId(),
                request.readSequence(),
                currentUpdate.aggregate(),
                eventsResult
        );
    }

    static <K, C, E, A> KStream<K, CommandEvents<E, A>> eventResultStream2(
            TopologyContext<K, C, E, A> ctx,
            final KStream<K, CommandRequest<K, C>> commandRequestStream,
            final KTable<K, AggregateUpdate<A>> aggregateTable) {
        return commandRequestStream.leftJoin(aggregateTable, (r, a) -> getCommandEvents(ctx, a, r),
                Joined.with(ctx.serdes().aggregateKey(),
                        ctx.serdes().commandRequest(),
                        ctx.serdes().aggregateUpdate()));
    }

    static <K, C, E, A> KStream<K, CommandEvents<E, A>> eventResultStream(TopologyContext<K, C, E, A> ctx, final KStream<K, CommandRequest<K, C>> commandRequestStream) {
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
