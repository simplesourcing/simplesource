package io.simplesource.kafka.internal.streams.topologies;

import io.simplesource.api.CommandError;
import io.simplesource.api.CommandHandler;
import io.simplesource.api.InitialValue;
import io.simplesource.api.InvalidSequenceHandler;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.AggregateResources.StateStoreEntity;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.ValueWithSequence;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

import static io.simplesource.data.Result.failure;

final class CommandRequestTransformer<K, C, E, A>
        implements ValueTransformerWithKey<K, CommandRequest<C>, CommandEvents<E, A>> {
    private static final Logger logger = LoggerFactory.getLogger(CommandRequestTransformer.class);

    private final TopologyContext<K, C, E, A> context;
    private final InvalidSequenceHandler<K, C, A> invalidSequenceHandler;
    private final CommandHandler<K, C, E, A> commandHandler;

    private ReadOnlyKeyValueStore<K, AggregateUpdate<A>> stateStore;

    CommandRequestTransformer(TopologyContext<K, C, E, A> context) {
        this.context = context;
        this.commandHandler = context.aggregateSpec().generation().commandHandler();
        this.invalidSequenceHandler = context.aggregateSpec().generation().invalidSequenceHandler();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(final ProcessorContext processorContext) {
        stateStore = (ReadOnlyKeyValueStore<K, AggregateUpdate<A>>) processorContext.getStateStore(
                context.stateStoreName(StateStoreEntity.aggregate_update));
    }

    @Override
    public CommandEvents<E, A> transform(final K readOnlyKey, final CommandRequest<C> request) {
        InitialValue<K, A> initialValue = context.aggregateSpec().generation().initialValue();

        AggregateUpdate<A> currentUpdatePre;
        try {
            currentUpdatePre = Optional.ofNullable(stateStore.get(readOnlyKey))
                    .orElse(AggregateUpdate.of(initialValue.empty(readOnlyKey)));
        } catch (InvalidStateStoreException e) {
            logger.warn("[{} aggregate] Failed to get aggregate with key {} from state store, it might be in middle of rebalance",
                    context.aggregateName(), readOnlyKey, e);

            return new CommandEvents<>(
                    request.commandId(),
                    request.readSequence(),
                    initialValue.empty(readOnlyKey),
                    Result.failure(CommandError.of(CommandError.Reason.InvalidStateStore, e)));
        }

        final AggregateUpdate<A> currentUpdate = currentUpdatePre;

        Result<CommandError, NonEmptyList<E>> commandResult;
        try {
            Optional<CommandError> maybeReject =
                    Objects.equals(request.readSequence(), currentUpdate.sequence()) ? Optional.empty() :
                            invalidSequenceHandler.shouldReject(
                                    readOnlyKey,
                                    currentUpdate.sequence(),
                                    request.readSequence(),
                                    currentUpdate.aggregate(),
                                    request.command());

            commandResult = maybeReject.<Result<CommandError, NonEmptyList<E>>>map(
                    commandErrorReason -> Result.failure(commandErrorReason)).orElseGet(
                    () -> commandHandler.interpretCommand(
                            readOnlyKey,
                            currentUpdate.aggregate(),
                            request.command()));
        } catch (final Exception e) {
            logger.warn("[{} aggregate] Failed to apply command handler on key {} to request {}",
                    context.aggregateName(), readOnlyKey, request, e);
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

    @Override
    public void close() {
    }

}
