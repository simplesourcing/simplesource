package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.api.CommandError;
import io.simplesource.data.Result;
import io.simplesource.kafka.model.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.function.BiFunction;

import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.aggregate_update;

final class EventSourcedStreams {

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
