package io.simplesource.kafka.internal.streams.topologies;

import io.simplesource.api.CommandError;
import io.simplesource.api.CommandHandler;
import io.simplesource.api.InitialValue;
import io.simplesource.api.InvalidSequenceHandler;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.ValueWithSequence;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

import static io.simplesource.data.Result.failure;
import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.aggregate_update;

final class CommandRequestTransformer<K, C, E, A> implements ValueTransformerWithKey<K, CommandRequest<C>, CommandEvents<E, A>> {
    private static final Logger logger = LoggerFactory.getLogger(CommandRequestTransformer.class);

    private ReadOnlyKeyValueStore<K, AggregateUpdate<A>> stateStore;

    private final InitialValue<K, A> initialValue;
    private final InvalidSequenceHandler<K, C, A> invalidSequenceHandler;
    private final CommandHandler<K, C, E, A> commandHandler;
    private final AggregateStreamResourceNames aggregateResourceNames;

    CommandRequestTransformer(InitialValue<K, A> initialValue, InvalidSequenceHandler<K, C, A> invalidSequenceHandler,
                                     CommandHandler<K, C, E, A> commandHandler,
                                     AggregateStreamResourceNames aggregateResourceNames) {
        this.initialValue = initialValue;
        this.invalidSequenceHandler = invalidSequenceHandler;
        this.commandHandler = commandHandler;
        this.aggregateResourceNames = aggregateResourceNames;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(final ProcessorContext context) {
        stateStore = (ReadOnlyKeyValueStore<K, AggregateUpdate<A>>) context.getStateStore(aggregateResourceNames.stateStoreName(aggregate_update));
    }

    @Override
    public CommandEvents<E, A> transform(final K readOnlyKey, final CommandRequest<C> request) {

        final AggregateUpdate<A> currentAggregateUpdate = currentAggregateUpdateOrDefault(readOnlyKey);

        Result<CommandError, NonEmptyList<E>> commandResult = processCommandRequest(readOnlyKey, request, currentAggregateUpdate);

        final Result<CommandError, NonEmptyList<ValueWithSequence<E>>> eventsResult = commandResult.map(
                eventList -> {
                    // get round Java limitation of only using finals in lambdas by wrapping in an array
                    final Sequence[] eventSequence = {currentAggregateUpdate.sequence()};
                    return eventList.map(event -> {
                        eventSequence[0] = eventSequence[0].next();
                        return new ValueWithSequence<>(event, eventSequence[0]);
                    });
                });

        return new CommandEvents<>(request.commandId(), request.readSequence(), currentAggregateUpdate.aggregate(), eventsResult);
    }

    private Result<CommandError, NonEmptyList<E>> processCommandRequest(K readOnlyKey, CommandRequest<C> request,
                                                                        AggregateUpdate<A> currentAggregateUpdate) {
        Result<CommandError, NonEmptyList<E>> commandResult;
        try {
            Optional<CommandError> maybeReject =
                    Objects.equals(request.readSequence(), currentAggregateUpdate.sequence()) ? Optional.empty() :
                            invalidSequenceHandler.shouldReject(
                                    readOnlyKey,
                                    currentAggregateUpdate.sequence(),
                                    request.readSequence(),
                                    currentAggregateUpdate.aggregate(),
                                    request.command());

            commandResult = maybeReject.<Result<CommandError, NonEmptyList<E>>>map(
                    commandErrorReason -> Result.failure(commandErrorReason)).orElseGet(
                    () -> commandHandler.interpretCommand(
                            readOnlyKey,
                            currentAggregateUpdate.aggregate(),
                            request.command()));
        } catch (final Exception e) {
            logger.warn("[{} aggregate] Failed to apply command handler on key {} to request {}",
                    aggregateResourceNames.getAggregateName(), readOnlyKey, request, e);
            commandResult = failure(CommandError.of(CommandError.Reason.CommandHandlerFailed, e));
        }
        return commandResult;
    }

    private AggregateUpdate<A> currentAggregateUpdateOrDefault(K readOnlyKey) {
        AggregateUpdate<A> currentUpdatePre;
        try {
            currentUpdatePre = Optional.ofNullable(stateStore.get(readOnlyKey))
                    .orElse(AggregateUpdate.of(initialValue.empty(readOnlyKey)));
        } catch (Exception e) {
            currentUpdatePre = AggregateUpdate.of(initialValue.empty(readOnlyKey));
        }
        return currentUpdatePre;
    }

    @Override
    public void close() {
    }

}
